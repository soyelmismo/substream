package tidalproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
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

	// [New] Write-back buffer to prevent SQLite contention
	pendingMu sync.Mutex
	pending   map[string][]byte
	quit      chan struct{}

	// Discography caches
	artistAlbums    *cache.Cache[[]byte]
	artistTopTracks *cache.Cache[[]byte]
	similarArtists  *cache.Cache[[]byte]
}

func NewCachedProxy(base TidalProxy, dbc *db.DB, ttl time.Duration) *CachedProxy {
	p := &CachedProxy{
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
		artistAlbums: cache.New[[]byte](cache.Config{
			Name:            "tidal-artist-albums",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		artistTopTracks: cache.New[[]byte](cache.Config{
			Name:            "tidal-artist-top-tracks",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		similarArtists: cache.New[[]byte](cache.Config{
			Name:            "tidal-similar-artists",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		pending: make(map[string][]byte),
		quit:    make(chan struct{}),
	}

	// Start the write-back flusher
	if dbc != nil {
		go p.flushBufferLoop()
	}

	return p
}

func (c *CachedProxy) flushBufferLoop() {
	ticker := time.NewTicker(7 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.flushBufferToDisk()
		case <-c.quit:
			return
		}
	}
}

func (c *CachedProxy) flushBufferToDisk() {
	c.pendingMu.Lock()
	if len(c.pending) == 0 {
		c.pendingMu.Unlock()
		return
	}
	// Copy buffer to write in transaction
	batch := c.pending
	c.pending = make(map[string][]byte)
	c.pendingMu.Unlock()

	log.Printf("[CACHE] Flushing %d metadata entries to SQLite (Batch)...", len(batch))
	start := time.Now()
	if err := c.db.SetCachedMetadataBatch(batch, metadataCacheTTL); err != nil {
		log.Printf("[CACHE ERROR] Flush failed: %v", err)
	} else {
		log.Printf("[CACHE] Flush successful in %v", time.Since(start))
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

	// [New] Check pending write-back buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var t TidalTrack
		if err := json.Unmarshal(data, &t); err == nil {
			return &t, nil
		}
	}
	c.pendingMu.Unlock()

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
			// Buffer for SQLite write-back (Atomic RAM-first)
			if c.db != nil {
				c.pendingMu.Lock()
				c.pending[key] = data
				c.pendingMu.Unlock()
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

	// [New] Check pending write-back buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var a TidalAlbum
		if err := json.Unmarshal(data, &a); err == nil {
			return &a, nil
		}
	}
	c.pendingMu.Unlock()

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
			// Buffer for SQLite write-back (Atomic RAM-first)
			if c.db != nil {
				c.pendingMu.Lock()
				c.pending[key] = data
				c.pendingMu.Unlock()
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

	// [New] Check pending write-back buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var a TidalArtistDetail
		if err := json.Unmarshal(data, &a); err == nil {
			return &a, nil
		}
	}
	c.pendingMu.Unlock()

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
			// Buffer for SQLite write-back (Atomic RAM-first)
			if c.db != nil {
				c.pendingMu.Lock()
				c.pending[key] = data
				c.pendingMu.Unlock()
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
	key := fmt.Sprintf("td:ar:al:%d", artistID)
	if skipTracks {
		key += ":skip"
	}

	// 1. Check LRU
	if cached := c.artistAlbums.Get(key); cached != nil {
		var p TidalArtistPage
		if err := json.Unmarshal(cached, &p); err == nil {
			return &p, nil
		}
	}

	// 2. Check Pending Buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var p TidalArtistPage
		if err := json.Unmarshal(data, &p); err == nil {
			return &p, nil
		}
	}
	c.pendingMu.Unlock()

	// 3. Check SQLite
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var p TidalArtistPage
			if err := json.Unmarshal(cached, &p); err == nil {
				c.artistAlbums.Set(key, cached, 0)
				return &p, nil
			}
		}
	}

	// 4. Fetch from API
	page, err := c.TidalProxy.GetArtistAlbums(ctx, artistID, skipTracks)
	if err == nil && page != nil {
		if data, err := json.Marshal(page); err == nil {
			c.artistAlbums.Set(key, data, 0)
			if c.db != nil {
				c.pendingMu.Lock()
				c.pending[key] = data
				c.pendingMu.Unlock()
			}
		}

		// Cache album art as side effect
		if len(page.Albums.Items) > 0 {
			for _, a := range page.Albums.Items {
				if a.Cover != "" {
					c.albumArt.Set(fmt.Sprintf("td:al:%d", a.ID), a.Cover, 0)
				}
			}
		}
	}
	return page, err
}

func (c *CachedProxy) GetArtistTopTracks(ctx context.Context, artistID int, limit int) ([]TidalTrack, error) {
	key := fmt.Sprintf("td:ar:tt:%d", artistID)

	// 1. Check LRU
	if cached := c.artistTopTracks.Get(key); cached != nil {
		var t []TidalTrack
		if err := json.Unmarshal(cached, &t); err == nil {
			return t, nil
		}
	}

	// 2. Check Pending Buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var t []TidalTrack
		if err := json.Unmarshal(data, &t); err == nil {
			return t, nil
		}
	}
	c.pendingMu.Unlock()

	// 3. Check SQLite
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var t []TidalTrack
			if err := json.Unmarshal(cached, &t); err == nil {
				c.artistTopTracks.Set(key, cached, 0)
				return t, nil
			}
		}
	}

	// 4. Fetch from API
	tracks, err := c.TidalProxy.GetArtistTopTracks(ctx, artistID, limit)
	if err == nil && tracks != nil {
		if data, err := json.Marshal(tracks); err == nil {
			c.artistTopTracks.Set(key, data, 0)
			if c.db != nil {
				c.pendingMu.Lock()
				c.pending[key] = data
				c.pendingMu.Unlock()
			}
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetSimilarArtists(ctx context.Context, artistID int) ([]TidalArtist, error) {
	key := fmt.Sprintf("td:ar:sim:%d", artistID)

	// 1. Check LRU
	if cached := c.similarArtists.Get(key); cached != nil {
		var a []TidalArtist
		if err := json.Unmarshal(cached, &a); err == nil {
			return a, nil
		}
	}

	// 2. Check Pending Buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var a []TidalArtist
		if err := json.Unmarshal(data, &a); err == nil {
			return a, nil
		}
	}
	c.pendingMu.Unlock()

	// 3. Check SQLite
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var a []TidalArtist
			if err := json.Unmarshal(cached, &a); err == nil {
				c.similarArtists.Set(key, cached, 0)
				return a, nil
			}
		}
	}

	// 4. Fetch from API
	similar, err := c.TidalProxy.GetSimilarArtists(ctx, artistID)
	if err == nil && similar != nil {
		if data, err := json.Marshal(similar); err == nil {
			c.similarArtists.Set(key, data, 0)
			if c.db != nil {
				c.pendingMu.Lock()
				c.pending[key] = data
				c.pendingMu.Unlock()
			}
		}
	}
	return similar, err
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
	c.artistAlbums.Stop()
	c.artistTopTracks.Stop()
	c.similarArtists.Stop()
}

// ClearAll clears all in-memory LRU caches
func (c *CachedProxy) ClearAll() {
	log.Printf("[CACHE] Clearing in-memory LRU caches...")
	c.tracks.Clear()
	c.albums.Clear()
	c.artists.Clear()
	c.albumArt.Clear()
	c.albumCount.Clear()
	c.artistAlbums.Clear()
	c.artistTopTracks.Clear()
	c.similarArtists.Clear()
	log.Printf("[CACHE] In-memory LRU caches cleared")
}
