package db

import (
	"fmt"
	"log"
	"os"
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
		SELECT key FROM metadata_cache WHERE key LIKE 'td:ar:%'
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
		SELECT key FROM metadata_cache WHERE key LIKE 'td:al:%'
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
		SELECT key FROM metadata_cache WHERE key LIKE 'td:tr:%'
	`, userID, userID, userID).Pluck("uri", &uris)
	return uris
}

// GetCachedMetadata retrieves cached metadata from SQLite
// Returns nil if not found or expired
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

	log.Printf("[CACHE HIT] %s (%d bytes, age: %v)", key, len(cache.Value), time.Since(cache.FetchedAt))
	return cache.Value
}

// SetCachedMetadata stores metadata in SQLite cache
func (db *DB) SetCachedMetadata(key string, value []byte, ttlSeconds int) error {
	cache := MetadataCache{
		Key:        key,
		Value:      value,
		FetchedAt:  time.Now(),
		TTLSeconds: ttlSeconds,
	}
	err := db.Save(&cache).Error
	if err != nil {
		log.Printf("[CACHE ERROR] Failed to store %s: %v", key, err)
	} else {
		log.Printf("[CACHE STORED] %s (%d bytes)", key, len(value))
	}
	return err
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
		maxEntries = 10000 // default: keep last 10k entries
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
