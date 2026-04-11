package tidalproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/internal/cache"
)

// metadataCacheTTL is the default TTL for persistent metadata cache (24 hours)
const metadataCacheTTL = 86400

// CachedProxy wraps TidalProxy with unified LRU+TTL caches + SQLite persistence
// Keys use the format: td:tr:ID, td:al:ID, td:ar:ID for consistency with metadata_cache
type CachedProxy struct {
	TidalProxy
	db         *db.DB               // Optional SQLite persistence (nil if not provided)
	tracks     *cache.Cache[[]byte] // JSON serialized TidalTrack
	albums     *cache.Cache[[]byte] // JSON serialized TidalAlbum
	artists    *cache.Cache[[]byte] // JSON serialized TidalArtistDetail
	albumArt   *cache.Cache[string] // td:al:ID -> cover UUID
	albumCount *cache.Cache[int]    // td:ar:ID -> album count
}

func NewCachedProxy(base TidalProxy, dbc *db.DB, ttl time.Duration) *CachedProxy {
	return &CachedProxy{
		TidalProxy: base,
		db:         dbc,
		tracks: cache.New[[]byte](cache.Config{
			Name:            "tidal-tracks",
			MaxSize:         500,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albums: cache.New[[]byte](cache.Config{
			Name:            "tidal-albums",
			MaxSize:         200,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		artists: cache.New[[]byte](cache.Config{
			Name:            "tidal-artists",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albumArt: cache.New[string](cache.Config{
			Name:            "tidal-album-art",
			MaxSize:         1000,
			DefaultTTL:      ttl * 2, // Album art changes less frequently
			CleanupInterval: 10 * time.Minute,
		}),
		albumCount: cache.New[int](cache.Config{
			Name:            "tidal-album-count",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
	}
}

func (c *CachedProxy) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	key := fmt.Sprintf("td:tr:%d", trackID)

	// 1. Check in-memory LRU cache
	if cached := c.tracks.Get(key); cached != nil {
		var t TidalTrack
		if err := json.Unmarshal(cached, &t); err == nil {
			return &t, nil
		}
	}

	// 2. Check SQLite persistent cache (if db available)
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var t TidalTrack
			if err := json.Unmarshal(cached, &t); err == nil {
				c.tracks.Set(key, cached, 0) // Warm LRU cache
				return &t, nil
			}
		}
	}

	// 3. Fetch from API
	t, err := c.TidalProxy.GetTrackInfo(ctx, trackID)
	if err == nil && t != nil {
		// Serialize to JSON
		if data, err := json.Marshal(t); err == nil {
			c.tracks.Set(key, data, 0)
			// Write-through to SQLite (if db available)
			if c.db != nil {
				c.db.SetCachedMetadata(key, data, metadataCacheTTL)
			}
		}
		if t.Album.Cover != "" {
			c.albumArt.Set(fmt.Sprintf("td:al:%d", t.Album.ID), t.Album.Cover, 0)
		}
	}
	return t, err
}

func (c *CachedProxy) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	key := fmt.Sprintf("td:al:%d", albumID)

	// 1. Check in-memory LRU cache
	if cached := c.albums.Get(key); cached != nil {
		var a TidalAlbum
		if err := json.Unmarshal(cached, &a); err == nil {
			return &a, nil
		}
	}

	// 2. Check SQLite persistent cache (if db available)
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var a TidalAlbum
			if err := json.Unmarshal(cached, &a); err == nil {
				c.albums.Set(key, cached, 0) // Warm LRU cache
				return &a, nil
			}
		}
	}

	// 3. Fetch from API
	a, err := c.TidalProxy.GetAlbumInfo(ctx, albumID)
	if err == nil && a != nil {
		// Serialize to JSON
		if data, err := json.Marshal(a); err == nil {
			c.albums.Set(key, data, 0)
			// Write-through to SQLite (if db available)
			if c.db != nil {
				c.db.SetCachedMetadata(key, data, metadataCacheTTL)
			}
		}
		// Cache album art with consistent key format
		if a.Cover != "" {
			c.albumArt.Set(key, a.Cover, 0)
		}
	}
	return a, err
}

func (c *CachedProxy) GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error) {
	key := fmt.Sprintf("td:ar:%d", artistID)

	// 1. Check in-memory LRU cache
	if cached := c.artists.Get(key); cached != nil {
		var a TidalArtistDetail
		if err := json.Unmarshal(cached, &a); err == nil {
			return &a, nil
		}
	}

	// 2. Check SQLite persistent cache (if db available)
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var a TidalArtistDetail
			if err := json.Unmarshal(cached, &a); err == nil {
				c.artists.Set(key, cached, 0) // Warm LRU cache
				return &a, nil
			}
		}
	}

	// 3. Fetch from API
	a, err := c.TidalProxy.GetArtistInfo(ctx, artistID)
	if err == nil && a != nil {
		// Serialize to JSON
		if data, err := json.Marshal(a); err == nil {
			c.artists.Set(key, data, 0)
			// Write-through to SQLite (if db available)
			if c.db != nil {
				c.db.SetCachedMetadata(key, data, metadataCacheTTL)
			}
		}
	}
	return a, err
}

// GetMirrorManager returns the underlying MirrorManager if the base is a Pool
func (c *CachedProxy) GetMirrorManager() *MirrorManager {
	if pool, ok := c.TidalProxy.(*Pool); ok {
		return pool.GetMirrorManager()
	}
	return nil
}

func (c *CachedProxy) GetCoverUUIDForAlbum(ctx context.Context, albumID int) string {
	key := fmt.Sprintf("td:al:%d", albumID)
	if cached := c.albumArt.Get(key); cached != "" {
		return cached
	}
	a, err := c.GetAlbumInfo(ctx, albumID)
	if err == nil && a != nil && a.Cover != "" {
		return a.Cover
	}
	return ""
}

func (c *CachedProxy) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error) {
	albums, err := c.TidalProxy.SearchAlbums(ctx, query, limit, offset)
	if err == nil {
		for _, a := range albums {
			if a.Cover != "" {
				c.albumArt.Set(fmt.Sprintf("td:al:%d", a.ID), a.Cover, 0)
			}
		}
	}
	return albums, err
}

func (c *CachedProxy) SearchTracks(ctx context.Context, query string, limit, offset int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.SearchTracks(ctx, query, limit, offset)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Set(fmt.Sprintf("td:al:%d", t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetArtistAlbums(ctx context.Context, artistID int, skipTracks bool) (*TidalArtistPage, error) {
	page, err := c.TidalProxy.GetArtistAlbums(ctx, artistID, skipTracks)
	if err == nil && page != nil {
		if len(page.Albums.Items) > 0 {
			for _, a := range page.Albums.Items {
				if a.Cover != "" {
					c.albumArt.Set(fmt.Sprintf("td:al:%d", a.ID), a.Cover, 0)
				}
			}
		}
		for _, t := range page.Tracks {
			if t.Album.Cover != "" {
				c.albumArt.Set(fmt.Sprintf("td:al:%d", t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return page, err
}

func (c *CachedProxy) GetTopTracks(ctx context.Context, limit int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.GetTopTracks(ctx, limit)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Set(fmt.Sprintf("td:al:%d", t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetRecommendations(ctx context.Context, trackID int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.GetRecommendations(ctx, trackID)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Set(fmt.Sprintf("td:al:%d", t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetArtistTopTracks(ctx context.Context, artistID int, limit int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.GetArtistTopTracks(ctx, artistID, limit)
	if err == nil {
		for _, t := range tracks {
			if t.Album.Cover != "" {
				c.albumArt.Set(fmt.Sprintf("td:al:%d", t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return tracks, err
}

// GetArtistAlbumCount returns cached album count for an artist, fetching if needed
func (c *CachedProxy) GetArtistAlbumCount(ctx context.Context, artistID int) int {
	key := fmt.Sprintf("td:ar:%d", artistID)
	if cached := c.albumCount.Get(key); cached != 0 {
		return cached
	}

	// Fetch from API
	page, err := c.GetArtistAlbums(ctx, artistID, true)
	count := 0
	if err == nil && page != nil {
		count = len(page.Albums.Items)
	}

	// Store in cache
	c.albumCount.Set(key, count, 0)
	return count
}

// Close stops all cache cleanup goroutines
func (c *CachedProxy) Close() {
	c.tracks.Stop()
	c.albums.Stop()
	c.artists.Stop()
	c.albumArt.Stop()
	c.albumCount.Stop()
}

// ClearAll clears all in-memory LRU caches
func (c *CachedProxy) ClearAll() {
	log.Printf("[CACHE] Clearing in-memory LRU caches...")
	c.tracks.Clear()
	c.albums.Clear()
	c.artists.Clear()
	c.albumArt.Clear()
	c.albumCount.Clear()
	log.Printf("[CACHE] In-memory LRU caches cleared")
}
