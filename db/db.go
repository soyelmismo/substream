package db

import (
	"fmt"
	"io"
	"log"
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

	// Silent GORM logger - migration errors are handled gracefully
	db.SetLogger(log.New(io.Discard, "", 0))
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

func (db *DB) AdminCount() int64 {
	var count int64
	db.Model(&User{}).Where("is_admin = ?", true).Count(&count)
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

func (db *DB) GetTidalAccounts() ([]*TidalAccount, error) {
	var accounts []*TidalAccount
	err := db.Order("created_at DESC").Find(&accounts).Error
	return accounts, err
}

func (db *DB) GetActiveTidalAccounts() ([]*TidalAccount, error) {
	var accounts []*TidalAccount
	// Only return accounts with active paid streaming access (tier >= 50: Premium or HiFi)
	err := db.Where("is_active = ? AND quality_tier >= 50", true).Order("quality_tier DESC, created_at ASC").Find(&accounts).Error
	return accounts, err
}

func (db *DB) GetActiveTidalAccountsForQuality(minTier int) ([]*TidalAccount, error) {
	var accounts []*TidalAccount
	err := db.Where("is_active = ? AND quality_tier >= ?", true, minTier).Order("quality_tier DESC, created_at ASC").Find(&accounts).Error
	return accounts, err
}

func (db *DB) AddTidalAccount(userID, username, refreshToken, accessToken string, expiresAt time.Time) (*TidalAccount, error) {
	acc := &TidalAccount{
		UserID:           userID,
		Username:         username,
		RefreshToken:     refreshToken,
		AccessToken:      accessToken,
		SubscriptionType: "PREMIUM",
		HighestQuality:   "HIGH",
		QualityTier:      QualityTierHigh,
		ExpiresAt:        expiresAt,
		IsActive:         true,
	}
	err := db.Create(acc).Error
	return acc, err
}

func (db *DB) AddTidalAccountWithQuality(userID, username, refreshToken, accessToken, subType, highestQuality string, qualityTier int, expiresAt time.Time) (*TidalAccount, error) {
	if qualityTier <= 0 {
		qualityTier = QualityTierHigh
	}
	acc := &TidalAccount{
		UserID:           userID,
		Username:         username,
		RefreshToken:     refreshToken,
		AccessToken:      accessToken,
		SubscriptionType: subType,
		HighestQuality:   highestQuality,
		QualityTier:      qualityTier,
		ExpiresAt:        expiresAt,
		IsActive:         true,
	}
	err := db.Create(acc).Error
	return acc, err
}

func (db *DB) UpdateTidalAccountTokens(id int, accessToken string, expiresAt time.Time) error {
	return db.Model(&TidalAccount{}).Where("id=?", id).Updates(map[string]interface{}{
		"access_token": accessToken,
		"expires_at":   expiresAt,
		"updated_at":   time.Now(),
	}).Error
}

func (db *DB) UpdateTidalAccountSubscription(id int, subType, highestQuality string, qualityTier int) error {
	return db.Model(&TidalAccount{}).Where("id=?", id).Updates(map[string]interface{}{
		"subscription_type": subType,
		"highest_quality":   highestQuality,
		"quality_tier":      qualityTier,
		"updated_at":        time.Now(),
	}).Error
}

// MarkTidalAccountAsFree demotes a Tidal account to the Free tier in the DB,
// disabling it from the paid-quality rotation. Called when Tidal rejects a
// playback request with subStatus 5003 ("quality not allowed in user's
// subscription") or returns a PREVIEW-only asset — i.e. the account is
// actually FREE despite being flagged otherwise.
//
// The change is persistent: the row is no longer picked up by
// GetActiveTidalAccounts (which requires quality_tier >= 50), so subsequent
// requests naturally rotate to the next valid account.
func (db *DB) MarkTidalAccountAsFree(id int) error {
	return db.Model(&TidalAccount{}).Where("id=?", id).Updates(map[string]interface{}{
		"subscription_type": "FREE",
		"highest_quality":   "LOW",
		"quality_tier":      QualityTierFree,
		"is_active":         false,
		"updated_at":        time.Now(),
	}).Error
}

func (db *DB) DeleteTidalAccount(id int) error {
	return db.Where("id=?", id).Delete(&TidalAccount{}).Error
}

func (db *DB) ToggleTidalAccount(id int, active bool) error {
	return db.Model(&TidalAccount{}).Where("id=?", id).Update("is_active", active).Error
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

// CacheLogSuppress is an optional callback that returns true if cache hit logging should be suppressed
var CacheLogSuppress func() bool

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
	// Skip if sync suppression is active
	if CacheLogSuppress == nil || !CacheLogSuppress() {
		count := cacheHitCounter.Add(1)
		if count%100 == 0 {
			log.Printf("[CACHE HIT] %s (%d bytes, age: %v) [batch: %d hits]", key, len(cache.Value), time.Since(cache.FetchedAt), count)
		}
	} else {
		// Still increment counter but don't log
		cacheHitCounter.Add(1)
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

// GetGlobalStats returns total counts of starred albums, artists, and tracks across all users
func (db *DB) GetGlobalStats() (albums, artists, tracks int64) {
	db.Model(&AlbumStar{}).Count(&albums)
	db.Model(&ArtistStar{}).Count(&artists)
	db.Model(&TrackStar{}).Count(&tracks)
	return
}

// GetUserStats returns counts for a specific user
func (db *DB) GetUserStats(userID int) (albums, artists, tracks int64) {
	db.Model(&AlbumStar{}).Where("user_id = ?", userID).Count(&albums)
	db.Model(&ArtistStar{}).Where("user_id = ?", userID).Count(&artists)
	db.Model(&TrackStar{}).Where("user_id = ?", userID).Count(&tracks)
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

// GetYTMusicAccounts returns all configured YouTube Music accounts
func (db *DB) GetYTMusicAccounts() ([]*YTMusicAccount, error) {
	var accounts []*YTMusicAccount
	err := db.Order("id desc").Find(&accounts).Error
	return accounts, err
}

// GetActiveYTMusicAccounts returns all active YouTube Music accounts
func (db *DB) GetActiveYTMusicAccounts() ([]*YTMusicAccount, error) {
	var accounts []*YTMusicAccount
	err := db.Where("is_active = ?", true).Order("id asc").Find(&accounts).Error
	return accounts, err
}

// AddYTMusicAccount adds a new YouTube Music account
func (db *DB) AddYTMusicAccount(name, cookieData, cookieFile, cookieHeader string, isPremium bool) (*YTMusicAccount, error) {
	acc := &YTMusicAccount{
		Name:         name,
		CookieData:   cookieData,
		CookieFile:   cookieFile,
		CookieHeader: cookieHeader,
		IsActive:     true,
		IsPremium:    isPremium,
	}
	err := db.Create(acc).Error
	return acc, err
}

// DeleteYTMusicAccount deletes an account by ID
func (db *DB) DeleteYTMusicAccount(id int) error {
	return db.Where("id = ?", id).Delete(&YTMusicAccount{}).Error
}

// ToggleYTMusicAccount enables or disables an account
func (db *DB) ToggleYTMusicAccount(id int, active bool) error {
	return db.Model(&YTMusicAccount{}).Where("id = ?", id).Update("is_active", active).Error
}

// GetDeezerAccounts returns all configured Deezer accounts
func (db *DB) GetDeezerAccounts() ([]*DeezerAccount, error) {
	var accounts []*DeezerAccount
	err := db.Order("created_at DESC").Find(&accounts).Error
	return accounts, err
}

// GetActiveDeezerAccounts returns all active Deezer accounts ordered by quality tier descending
func (db *DB) GetActiveDeezerAccounts() ([]*DeezerAccount, error) {
	var accounts []*DeezerAccount
	err := db.Where("is_active = ?", true).Order("quality_tier DESC, created_at ASC").Find(&accounts).Error
	return accounts, err
}

// GetActiveDeezerAccountsForQuality returns active Deezer accounts meeting minTier
func (db *DB) GetActiveDeezerAccountsForQuality(minTier int) ([]*DeezerAccount, error) {
	var accounts []*DeezerAccount
	err := db.Where("is_active = ? AND quality_tier >= ?", true, minTier).Order("quality_tier DESC, created_at ASC").Find(&accounts).Error
	return accounts, err
}

// GetDeezerAccountByID retrieves a single Deezer account by ID
func (db *DB) GetDeezerAccountByID(id int) (*DeezerAccount, error) {
	var acc DeezerAccount
	err := db.Where("id = ?", id).First(&acc).Error
	if err != nil {
		return nil, err
	}
	return &acc, nil
}

// AddDeezerAccount adds a new Deezer account
func (db *DB) AddDeezerAccount(arl, userID, username, subType, highestQuality string, qualityTier int) (*DeezerAccount, error) {
	if qualityTier <= 0 {
		qualityTier = QualityTierLossless
	}
	acc := &DeezerAccount{
		ARL:              arl,
		UserID:           userID,
		Username:         username,
		SubscriptionType: subType,
		HighestQuality:   highestQuality,
		QualityTier:      qualityTier,
		IsActive:         true,
		LastValidatedAt:  time.Now(),
	}
	err := db.Create(acc).Error
	return acc, err
}

// UpdateDeezerAccount updates an existing Deezer account record
func (db *DB) UpdateDeezerAccount(acc *DeezerAccount) error {
	acc.UpdatedAt = time.Now()
	return db.Save(acc).Error
}

// UpdateDeezerAccountSubscription updates subscription tier and clears/sets last error
func (db *DB) UpdateDeezerAccountSubscription(id int, subType, highestQuality string, qualityTier int, lastError string) error {
	return db.Model(&DeezerAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"subscription_type": subType,
		"highest_quality":   highestQuality,
		"quality_tier":      qualityTier,
		"last_error":        lastError,
		"last_validated_at": time.Now(),
		"updated_at":        time.Now(),
	}).Error
}

// UpdateDeezerAccountError records the last validation or stream error for an account
func (db *DB) UpdateDeezerAccountError(id int, lastError string) error {
	return db.Model(&DeezerAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"last_error":        lastError,
		"last_validated_at": time.Now(),
		"updated_at":        time.Now(),
	}).Error
}

// ToggleDeezerAccount enables or disables an account
func (db *DB) ToggleDeezerAccount(id int, active bool) error {
	return db.Model(&DeezerAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"is_active":  active,
		"updated_at": time.Now(),
	}).Error
}

// MarkDeezerAccountAsFree demotes a Deezer account to the Free tier in the DB,
// disabling it from the paid/lossless rotation.
func (db *DB) MarkDeezerAccountAsFree(id int) error {
	return db.Model(&DeezerAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"subscription_type": "FREE",
		"highest_quality":   "MP3_128",
		"quality_tier":      QualityTierFree,
		"is_active":         false,
		"last_error":        "Subscription degraded to FREE or Lossless expired",
		"last_validated_at": time.Now(),
		"updated_at":        time.Now(),
	}).Error
}

// DeleteDeezerAccount deletes an account by ID
func (db *DB) DeleteDeezerAccount(id int) error {
	return db.Where("id = ?", id).Delete(&DeezerAccount{}).Error
}

// GetQobuzAccounts returns all configured Qobuz accounts
func (db *DB) GetQobuzAccounts() ([]*QobuzAccount, error) {
	var accounts []*QobuzAccount
	err := db.Order("created_at DESC").Find(&accounts).Error
	return accounts, err
}

// GetActiveQobuzAccounts returns all active Qobuz accounts ordered by quality tier descending
func (db *DB) GetActiveQobuzAccounts() ([]*QobuzAccount, error) {
	var accounts []*QobuzAccount
	err := db.Where("is_active = ?", true).Order("quality_tier DESC, created_at ASC").Find(&accounts).Error
	return accounts, err
}

// GetActiveQobuzAccountsForQuality returns active Qobuz accounts meeting minTier
func (db *DB) GetActiveQobuzAccountsForQuality(minTier int) ([]*QobuzAccount, error) {
	var accounts []*QobuzAccount
	err := db.Where("is_active = ? AND quality_tier >= ?", true, minTier).Order("quality_tier DESC, created_at ASC").Find(&accounts).Error
	return accounts, err
}

// GetQobuzAccountByID retrieves a single Qobuz account by ID
func (db *DB) GetQobuzAccountByID(id int) (*QobuzAccount, error) {
	var acc QobuzAccount
	err := db.Where("id = ?", id).First(&acc).Error
	if err != nil {
		return nil, err
	}
	return &acc, nil
}

// AddQobuzAccount adds a new Qobuz account
func (db *DB) AddQobuzAccount(token, userID, username, subType, highestQuality string, qualityTier int) (*QobuzAccount, error) {
	if qualityTier <= 0 {
		qualityTier = QualityTierLossless
	}
	acc := &QobuzAccount{
		UserAuthToken:    token,
		UserID:           userID,
		Username:         username,
		SubscriptionType: subType,
		HighestQuality:   highestQuality,
		QualityTier:      qualityTier,
		IsActive:         true,
		LastValidatedAt:  time.Now(),
	}
	err := db.Create(acc).Error
	return acc, err
}

// UpdateQobuzAccount updates an existing Qobuz account record
func (db *DB) UpdateQobuzAccount(acc *QobuzAccount) error {
	acc.UpdatedAt = time.Now()
	return db.Save(acc).Error
}

// UpdateQobuzAccountSubscription updates subscription tier and clears/sets last error
func (db *DB) UpdateQobuzAccountSubscription(id int, subType, highestQuality string, qualityTier int, lastError string) error {
	return db.Model(&QobuzAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"subscription_type": subType,
		"highest_quality":   highestQuality,
		"quality_tier":      qualityTier,
		"last_error":        lastError,
		"last_validated_at": time.Now(),
		"updated_at":        time.Now(),
	}).Error
}

// UpdateQobuzAccountError records the last validation or stream error for an account
func (db *DB) UpdateQobuzAccountError(id int, lastError string) error {
	return db.Model(&QobuzAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"last_error":        lastError,
		"last_validated_at": time.Now(),
		"updated_at":        time.Now(),
	}).Error
}

// ToggleQobuzAccount enables or disables an account
func (db *DB) ToggleQobuzAccount(id int, active bool) error {
	return db.Model(&QobuzAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"is_active":  active,
		"updated_at": time.Now(),
	}).Error
}

// MarkQobuzAccountAsFree demotes a Qobuz account to the Free tier in the DB,
// disabling it from the paid/lossless rotation.
func (db *DB) MarkQobuzAccountAsFree(id int) error {
	return db.Model(&QobuzAccount{}).Where("id = ?", id).Updates(map[string]interface{}{
		"subscription_type": "FREE",
		"highest_quality":   "MP3_320",
		"quality_tier":      QualityTierFree,
		"is_active":         false,
		"last_error":        "Subscription degraded to FREE or Hi-Res/Lossless expired",
		"last_validated_at": time.Now(),
		"updated_at":        time.Now(),
	}).Error
}

// DeleteQobuzAccount deletes an account by ID
func (db *DB) DeleteQobuzAccount(id int) error {
	return db.Where("id = ?", id).Delete(&QobuzAccount{}).Error
}


