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
// This includes: artist_stars, and artists inferred from track_stars/plays/playlist_tracks via track_metadata.
func (db *DB) GetVirtualLibraryArtistIDs(userID int) []string {
	var uris []string

	// Direct artist stars
	db.Table("artist_stars").Where("user_id = ?", userID).Pluck("uri", &uris)

	// Artists inferred from track stars (via track_metadata)
	var trackStarURIs []string
	db.Table("track_stars").Where("user_id = ?", userID).Pluck("uri", &trackStarURIs)
	for _, trackURI := range trackStarURIs {
		var tm TrackMetadata
		if db.Where("uri = ?", trackURI).First(&tm).Error == nil {
			uris = appendUniqueURI(uris, tm.ArtistURI)
		}
	}

	// Artists inferred from plays (via track_metadata)
	var playURIs []string
	db.Table("plays").Where("user_id = ?", userID).Pluck("uri", &playURIs)
	for _, trackURI := range playURIs {
		var tm TrackMetadata
		if db.Where("uri = ?", trackURI).First(&tm).Error == nil {
			uris = appendUniqueURI(uris, tm.ArtistURI)
		}
	}

	// Artists inferred from playlist tracks (via track_metadata)
	var playlistTrackURIs []string
	db.Raw(`
		SELECT DISTINCT pt.uri 
		FROM playlist_tracks pt
		JOIN playlists p ON pt.playlist_id = p.id
		WHERE p.user_id = ?
	`, userID).Pluck("uri", &playlistTrackURIs)
	for _, trackURI := range playlistTrackURIs {
		var tm TrackMetadata
		if db.Where("uri = ?", trackURI).First(&tm).Error == nil {
			uris = appendUniqueURI(uris, tm.ArtistURI)
		}
	}

	return uris
}

// GetVirtualLibraryAlbumIDs returns all album URIs that the user has interacted with.
// This includes: album_stars, and albums inferred from track_stars/plays/playlist_tracks via track_metadata.
func (db *DB) GetVirtualLibraryAlbumIDs(userID int) []string {
	var uris []string

	// Direct album stars
	db.Table("album_stars").Where("user_id = ?", userID).Pluck("uri", &uris)

	// Albums inferred from track stars (via track_metadata)
	var trackStarURIs []string
	db.Table("track_stars").Where("user_id = ?", userID).Pluck("uri", &trackStarURIs)
	for _, trackURI := range trackStarURIs {
		var tm TrackMetadata
		if db.Where("uri = ?", trackURI).First(&tm).Error == nil {
			uris = appendUniqueURI(uris, tm.AlbumURI)
		}
	}

	// Albums inferred from plays (via track_metadata)
	var playURIs []string
	db.Table("plays").Where("user_id = ?", userID).Pluck("uri", &playURIs)
	for _, trackURI := range playURIs {
		var tm TrackMetadata
		if db.Where("uri = ?", trackURI).First(&tm).Error == nil {
			uris = appendUniqueURI(uris, tm.AlbumURI)
		}
	}

	// Albums inferred from playlist tracks (via track_metadata)
	var playlistTrackURIs []string
	db.Raw(`
		SELECT DISTINCT pt.uri 
		FROM playlist_tracks pt
		JOIN playlists p ON pt.playlist_id = p.id
		WHERE p.user_id = ?
	`, userID).Pluck("uri", &playlistTrackURIs)
	for _, trackURI := range playlistTrackURIs {
		var tm TrackMetadata
		if db.Where("uri = ?", trackURI).First(&tm).Error == nil {
			uris = appendUniqueURI(uris, tm.AlbumURI)
		}
	}

	return uris
}

// GetVirtualLibraryTrackIDs returns all track URIs that the user has interacted with.
// This includes: track_stars, plays, and playlist_tracks.
func (db *DB) GetVirtualLibraryTrackIDs(userID int) []string {
	var uris []string

	// Track stars
	db.Table("track_stars").Where("user_id = ?", userID).Pluck("uri", &uris)

	// Plays
	var playURIs []string
	db.Table("plays").Where("user_id = ?", userID).Pluck("uri", &playURIs)
	for _, uri := range playURIs {
		uris = appendUniqueURI(uris, uri)
	}

	// Playlist tracks
	var playlistTrackURIs []string
	db.Raw(`
		SELECT DISTINCT pt.uri 
		FROM playlist_tracks pt
		JOIN playlists p ON pt.playlist_id = p.id
		WHERE p.user_id = ?
	`, userID).Pluck("uri", &playlistTrackURIs)
	for _, uri := range playlistTrackURIs {
		uris = appendUniqueURI(uris, uri)
	}

	return uris
}

// appendUniqueURI appends a URI to a slice if it doesn't already exist
func appendUniqueURI(slice []string, uri string) []string {
	for _, s := range slice {
		if s == uri {
			return slice
		}
	}
	return append(slice, uri)
}

// GetCachedMetadata retrieves cached metadata from SQLite
// Returns nil if not found or expired
func (db *DB) GetCachedMetadata(key string) []byte {
	var cache MetadataCache
	err := db.Where("key = ?", key).First(&cache).Error
	if err != nil {
		return nil
	}

	// Check if expired
	if cache.TTLSeconds > 0 {
		expiry := cache.FetchedAt.Add(time.Duration(cache.TTLSeconds) * time.Second)
		if time.Now().After(expiry) {
			// Delete expired entry
			db.Where("key = ?", key).Delete(&MetadataCache{})
			return nil
		}
	}

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
	return db.Save(&cache).Error
}

// CleanupExpiredCache removes all expired entries from metadata_cache
// This is optional but recommended for maintenance
func (db *DB) CleanupExpiredCache() error {
	return db.Where("fetched_at < datetime('now', '-' || ttl_seconds || ' seconds')").Delete(&MetadataCache{}).Error
}



