package db

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

type DB struct {
	*gorm.DB
}

func New(path string) (*DB, error) {
	db, err := gorm.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("with gorm: %w", err)
	}

	db.SetLogger(log.New(os.Stdout, "gorm ", 0))
	db.DB().SetMaxOpenConns(4)

	// Custom pragmas for SQLite optimization
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA foreign_keys=ON")

	return &DB{DB: db}, nil
}

func (db *DB) GetUserByName(username string) *User {
	var user User
	err := db.Where("name=?", username).First(&user).Error
	if err != nil {
		return nil
	}
	return &user
}

func (db *DB) GetUserByID(id int) *User {
	var user User
	err := db.Where("id=?", id).First(&user).Error
	if err != nil {
		return nil
	}
	return &user
}

func (db *DB) UserCount() int64 {
	var count int64
	db.Model(&User{}).Count(&count)
	return count
}

func (db *DB) GetProxies() ([]*ProxyInstance, error) {
	var proxies []*ProxyInstance
	err := db.Order("created_at DESC").Find(&proxies).Error
	return proxies, err
}

func (db *DB) AddProxy(url string, name string, source string) error {
	return db.Create(&ProxyInstance{URL: url, Name: name, Source: source}).Error
}

func (db *DB) DeleteProxy(id int) error {
	return db.Where("id=?", id).Delete(&ProxyInstance{}).Error
}

func (db *DB) UpdateProxyHealth(id int, healthy bool) error {
	return db.Model(&ProxyInstance{}).Where("id=?", id).Update("is_healthy", healthy).Error
}

func (db *DB) GetSetting(key string, defaultValue string) string {
	var setting Setting
	err := db.Where("key=?", key).First(&setting).Error
	if err != nil {
		return defaultValue
	}
	return setting.Value
}

func (db *DB) SetSetting(key string, value string) error {
	var setting Setting
	err := db.Where("key=?", key).First(&setting).Error
	if err != nil {
		return db.Create(&Setting{Key: key, Value: value}).Error
	}
	setting.Value = value
	return db.Save(&setting).Error
}

// GetVirtualLibraryArtistIDs returns all artist URIs that the user has interacted with.
func (db *DB) GetVirtualLibraryArtistIDs(userID int) []string {
	var uris []string
	db.Raw(`
		SELECT uri FROM artist_stars WHERE user_id = ?
		UNION
		SELECT tm.artist_uri FROM track_stars ts JOIN track_metadata tm ON ts.uri = tm.uri WHERE ts.user_id = ?
		UNION
		SELECT tm.artist_uri FROM plays p JOIN track_metadata tm ON p.uri = tm.uri WHERE p.user_id = ?
		UNION
		SELECT tm.artist_uri FROM playlist_tracks pt JOIN playlists pl ON pt.playlist_id = pl.id JOIN track_metadata tm ON pt.uri = tm.uri WHERE pl.user_id = ?
		UNION
		SELECT key FROM metadata_cache WHERE key GLOB 'td:ar:[0-9]*'
	`, userID, userID, userID, userID).Pluck("uri", &uris)
	return uris
}

// GetVirtualLibraryAlbumIDs returns all album URIs that the user has interacted with.
func (db *DB) GetVirtualLibraryAlbumIDs(userID int) []string {
	var uris []string
	db.Raw(`
		SELECT uri FROM album_stars WHERE user_id = ?
		UNION
		SELECT tm.album_uri FROM track_stars ts JOIN track_metadata tm ON ts.uri = tm.uri WHERE ts.user_id = ?
		UNION
		SELECT tm.album_uri FROM plays p JOIN track_metadata tm ON p.uri = tm.uri WHERE p.user_id = ?
		UNION
		SELECT tm.album_uri FROM playlist_tracks pt JOIN playlists pl ON pt.playlist_id = pl.id JOIN track_metadata tm ON pt.uri = tm.uri WHERE pl.user_id = ?
		UNION
		SELECT key FROM metadata_cache WHERE key GLOB 'td:al:[0-9]*'
	`, userID, userID, userID, userID).Pluck("uri", &uris)
	return uris
}

// GetVirtualLibraryTrackIDs returns all track URIs that the user has interacted with.
func (db *DB) GetVirtualLibraryTrackIDs(userID int) []string {
	var uris []string
	db.Raw(`
		SELECT uri FROM track_stars WHERE user_id = ?
		UNION
		SELECT uri FROM plays WHERE user_id = ?
		UNION
		SELECT pt.uri FROM playlist_tracks pt JOIN playlists pl ON pt.playlist_id = pl.id WHERE pl.user_id = ?
		UNION
		SELECT key FROM metadata_cache WHERE key GLOB 'td:tr:[0-9]*'
	`, userID, userID, userID).Pluck("uri", &uris)
	return uris
}

// GetCachedMetadata retrieves cached metadata from SQLite
// Returns nil if not found or expired
var cacheHitCounter atomic.Int64

func (db *DB) GetCachedMetadata(key string) []byte {
	var cache MetadataCache
	err := db.Where("key = ?", key).First(&cache).Error
	if err != nil {
		if err != gorm.ErrRecordNotFound {
			log.Printf("[CACHE ERROR] Failed to get %s: %v", key, err)
		}
		return nil
	}

	// Check if expired
	if time.Since(cache.FetchedAt) > time.Duration(cache.TTLSeconds)*time.Second {
		log.Printf("[CACHE EXPIRED] %s (fetched %v ago)", key, time.Since(cache.FetchedAt))
		return nil
	}

	// Batch logging: log every 100 cache hits to reduce I/O spam
	count := cacheHitCounter.Add(1)
	if count%100 == 0 {
		log.Printf("[CACHE HIT] %s (%d bytes, age: %v) [batch: %d hits]", key, len(cache.Value), time.Since(cache.FetchedAt), count)
	}
	return cache.Value
}

// GetCachedMetadataBatch retrieves multiple cached entries in a single query
// Much more efficient than N individual GetCachedMetadata calls
func (db *DB) GetCachedMetadataBatch(keys []string) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}

	// Deduplicate keys
	keyMap := make(map[string]struct{})
	for _, k := range keys {
		keyMap[k] = struct{}{}
	}
	uniqueKeys := make([]string, 0, len(keyMap))
	for k := range keyMap {
		uniqueKeys = append(uniqueKeys, k)
	}

	// Query all matching keys in one shot using IN clause
	// Build placeholders for SQLite: (?, ?, ?, ...)
	placeholders := make([]string, len(uniqueKeys))
	args := make([]interface{}, len(uniqueKeys))
	for i, key := range uniqueKeys {
		placeholders[i] = "?"
		args[i] = key
	}

	var caches []MetadataCache
	sql := fmt.Sprintf("SELECT * FROM metadata_cache WHERE key IN (%s)", strings.Join(placeholders, ","))
	err := db.Raw(sql, args...).Scan(&caches).Error
	if err != nil {
		log.Printf("[CACHE ERROR] Batch fetch failed: %v", err)
		return nil
	}

	// Filter expired and build result map
	now := time.Now()
	result := make(map[string][]byte, len(caches))
	for _, cache := range caches {
		if now.Sub(cache.FetchedAt) > time.Duration(cache.TTLSeconds)*time.Second {
			continue // Expired, skip
		}
		result[cache.Key] = cache.Value
	}

	return result
}

// SetCachedMetadata stores metadata in SQLite cache
func (db *DB) SetCachedMetadata(key string, value []byte, ttlSeconds int) error {
	now := time.Now()
	return db.Exec(`INSERT INTO metadata_cache (key, value, fetched_at, ttl_seconds) 
                   VALUES (?, ?, ?, ?) 
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, fetched_at=excluded.fetched_at, ttl_seconds=excluded.ttl_seconds`,
		key, value, now, ttlSeconds).Error
}

// SetCachedMetadataBatch stores multiple metadata entries in a single transaction
func (db *DB) SetCachedMetadataBatch(batch map[string][]byte, ttlSeconds int) error {
	now := time.Now()
	tx := db.Begin()
	for key, value := range batch {
		if err := tx.Exec(`INSERT INTO metadata_cache (key, value, fetched_at, ttl_seconds) 
		                   VALUES (?, ?, ?, ?) 
		                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, fetched_at=excluded.fetched_at, ttl_seconds=excluded.ttl_seconds`,
			key, value, now, ttlSeconds).Error; err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit().Error
}

// CleanupExpiredCache removes all expired entries from metadata_cache
// This is optional but recommended for maintenance
func (db *DB) CleanupExpiredCache() error {
	return db.Where("fetched_at < datetime('now', '-' || ttl_seconds || ' seconds')").Delete(&MetadataCache{}).Error
}

// CleanupOldestEntries removes oldest entries to keep cache under maxEntries
// Call this periodically to prevent unbounded growth
func (db *DB) CleanupOldestEntries(maxEntries int) error {
	var count int64
	db.Table("metadata_cache").Count(&count)

	if count <= int64(maxEntries) {
		return nil
	}

	toDelete := count - int64(maxEntries)
	log.Printf("[CACHE] Cleaning up %d oldest metadata_cache entries (current: %d, max: %d)", toDelete, count, maxEntries)

	// Delete oldest entries beyond the limit
	return db.Exec(`
		DELETE FROM metadata_cache WHERE key IN (
			SELECT key FROM metadata_cache 
			ORDER BY fetched_at ASC 
			LIMIT ?
		)
	`, toDelete).Error
}

// StartCacheMaintenance starts background cleanup for metadata_cache
// Runs every interval to remove expired and excess entries
func (db *DB) StartCacheMaintenance(interval time.Duration, maxEntries int) {
	if interval <= 0 {
		interval = 1 * time.Hour // default
	}
	if maxEntries <= 0 {
		maxEntries = 100000 // default: keep last 100k entries
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			// Remove expired entries
			if err := db.CleanupExpiredCache(); err != nil {
				log.Printf("[CACHE] Error cleaning expired: %v", err)
			}

			// Remove oldest if over limit
			if err := db.CleanupOldestEntries(maxEntries); err != nil {
				log.Printf("[CACHE] Error cleaning oldest: %v", err)
			}
		}
	}()

	log.Printf("[CACHE] Started metadata_cache maintenance: interval=%v, maxEntries=%d", interval, maxEntries)
}

// buildINClause builds a SQL IN clause with proper placeholders for SQLite
func buildINClause(baseQuery string, userID int, uris []string) (string, []interface{}) {
	placeholders := make([]string, len(uris))
	args := make([]interface{}, 0, len(uris)+1)
	args = append(args, userID)

	for i, uri := range uris {
		placeholders[i] = "?"
		args = append(args, uri)
	}

	sql := fmt.Sprintf(baseQuery, strings.Join(placeholders, ","))
	return sql, args
}

// GetTrackStarsBatch retrieves star dates for multiple tracks in a single query
// Returns map[uri]starDate for tracks that are starred
func (db *DB) GetTrackStarsBatch(userID int, uris []string) map[string]time.Time {
	if len(uris) == 0 {
		return nil
	}

	var stars []TrackStar
	result := make(map[string]time.Time)

	// Build query with explicit placeholders for SQLite
	sql, args := buildINClause("SELECT * FROM track_stars WHERE user_id = ? AND uri IN (%s)", userID, uris)
	err := db.Raw(sql, args...).Scan(&stars).Error
	if err != nil {
		log.Printf("[DB ERROR] GetTrackStarsBatch failed: %v", err)
		return result
	}

	for _, s := range stars {
		result[s.URI] = s.StarDate
	}
	return result
}

// GetTrackRatingsBatch retrieves ratings for multiple tracks in a single query
// Returns map[uri]rating
func (db *DB) GetTrackRatingsBatch(userID int, uris []string) map[string]int {
	if len(uris) == 0 {
		return nil
	}

	var ratings []TrackRating
	result := make(map[string]int)

	sql, args := buildINClause("SELECT * FROM track_ratings WHERE user_id = ? AND uri IN (%s)", userID, uris)
	err := db.Raw(sql, args...).Scan(&ratings).Error
	if err != nil {
		log.Printf("[DB ERROR] GetTrackRatingsBatch failed: %v", err)
		return result
	}

	for _, r := range ratings {
		result[r.URI] = r.Rating
	}
	return result
}

// GetTrackPlayCountsBatch retrieves play counts for multiple tracks in a single query
// Returns map[uri]playCount
func (db *DB) GetTrackPlayCountsBatch(userID int, uris []string) map[string]int {
	if len(uris) == 0 {
		return nil
	}

	result := make(map[string]int)

	// Query from plays table (not scrobbles)
	sql, args := buildINClause("SELECT uri, count FROM plays WHERE user_id = ? AND uri IN (%s)", userID, uris)
	rows, err := db.Raw(sql, args...).Rows()

	if err != nil {
		log.Printf("[DB ERROR] GetTrackPlayCountsBatch failed: %v", err)
		return result
	}
	defer rows.Close()

	var uri string
	var count int
	for rows.Next() {
		if err := rows.Scan(&uri, &count); err == nil {
			result[uri] = count
		}
	}
	return result
}

// GetCacheStats returns current statistics about the metadata_cache
func (db *DB) GetCacheStats() (total int64, expired int64, oldest time.Time, newest time.Time) {
	db.Table("metadata_cache").Count(&total)

	db.Table("metadata_cache").
		Where("fetched_at < datetime('now', '-' || ttl_seconds || ' seconds')").
		Count(&expired)

	db.Raw("SELECT MIN(fetched_at), MAX(fetched_at) FROM metadata_cache").
		Row().Scan(&oldest, &newest)

	return
}

// ClearAllCache removes all entries from the metadata_cache
func (db *DB) ClearAllCache() error {
	// Forzar checkpoint de WAL antes de DELETE para consolidar escrituras previas
	db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	var count int64
	db.Table("metadata_cache").Count(&count)
	log.Printf("[CACHE] metadata_cache count before delete: %d", count)

	result := db.Exec("DELETE FROM metadata_cache")
	if result.Error != nil {
		return result.Error
	}
	log.Printf("[CACHE] Cleared all metadata_cache entries: %d deleted", result.RowsAffected)

	var countAfter int64
	db.Table("metadata_cache").Count(&countAfter)
	log.Printf("[CACHE] metadata_cache count after delete: %d", countAfter)
	return nil
}
