package tidalproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/internal/cache"
)

// metadataCacheTTL is the default TTL for persistent metadata cache (30 days)
const metadataCacheTTL = 2592000

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

	// [New] Stream URL cache (Short-lived, IP-locked)
	streamURLs *cache.Cache[string]

	// [New] Negative cache for tracks that are unavailable (403/404 errors)
	// This prevents repeated attempts on region-blocked or deleted tracks
	unavailableTracks *cache.Cache[time.Time] // trackID -> first failure time
}

func NewCachedProxy(base TidalProxy, dbc *db.DB, ttl time.Duration) *CachedProxy {
	p := &CachedProxy{
		TidalProxy: base,
		db:         dbc,
		tracks: cache.New[[]byte](cache.Config{
			Name:            "tidal-tracks",
			MaxSize:         5000,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albums: cache.New[[]byte](cache.Config{
			Name:            "tidal-albums",
			MaxSize:         2000,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		artists: cache.New[[]byte](cache.Config{
			Name:            "tidal-artists",
			MaxSize:         1000,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albumArt: cache.New[string](cache.Config{
			Name:            "tidal-album-art",
			MaxSize:         3000,
			DefaultTTL:      ttl * 2, // Album art changes less frequently
			CleanupInterval: 10 * time.Minute,
		}),
		albumCount: cache.New[int](cache.Config{
			Name:            "tidal-album-count",
			MaxSize:         1000,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		artistAlbums: cache.New[[]byte](cache.Config{
			Name:            "tidal-artist-albums",
			MaxSize:         1000,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		artistTopTracks: cache.New[[]byte](cache.Config{
			Name:            "tidal-artist-top-tracks",
			MaxSize:         1000,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		similarArtists: cache.New[[]byte](cache.Config{
			Name:            "tidal-similar-artists",
			MaxSize:         1000,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		streamURLs: cache.New[string](cache.Config{
			Name:            "tidal-stream-urls",
			MaxSize:         500,
			DefaultTTL:      30 * time.Minute, // Safely within 1h Tidal window
			CleanupInterval: 5 * time.Minute,
		}),
		unavailableTracks: cache.New[time.Time](cache.Config{
			Name:            "tidal-unavailable-tracks",
			MaxSize:         1000,             // Track up to 1000 unavailable tracks
			DefaultTTL:      10 * time.Minute, // Cache for 10 minutes
			CleanupInterval: 2 * time.Minute,
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

// cacheTrackJSON adds a track to both LRU and persistent write-back buffer
func (c *CachedProxy) cacheTrackJSON(t *TidalTrack) {
	if t == nil || t.ID == 0 {
		return
	}
	key := fmt.Sprintf("td:tr:%d", t.ID)
	data, err := json.Marshal(t)
	if err != nil {
		return
	}
	c.tracks.Set(key, data, 0)
	if c.db != nil {
		c.pendingMu.Lock()
		c.pending[key] = data
		c.pendingMu.Unlock()
	}
	// Also cache album art as side effect
	if t.Album.ID != 0 && t.Album.Cover != "" {
		c.albumArt.Set(fmt.Sprintf("td:al:%d", t.Album.ID), t.Album.Cover, 0)
	}
}

// backfillTrackYear fills in Album.ReleaseDate from album cache if track lacks it
// Checks both LRU and SQLite persistent cache
func (c *CachedProxy) backfillTrackYear(t *TidalTrack) {
	if t.Album.ReleaseDate != "" || t.Album.ID == 0 {
		return // Already has year or no album ID
	}
	albumKey := fmt.Sprintf("td:al:%d", t.Album.ID)

	// 1. Try LRU cache first (fast)
	if cached := c.albums.Get(albumKey); cached != nil {
		var album TidalAlbum
		if err := json.Unmarshal(cached, &album); err == nil && album.ReleaseDate != "" {
			t.Album.ReleaseDate = album.ReleaseDate
			return
		}
	}

	// 2. Fallback to SQLite persistent cache
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(albumKey); cached != nil {
			var album TidalAlbum
			if err := json.Unmarshal(cached, &album); err == nil && album.ReleaseDate != "" {
				t.Album.ReleaseDate = album.ReleaseDate
				// Warm the LRU cache for next time
				c.albums.Set(albumKey, cached, 0)
				return
			}
		}
	}
}

// backfillAlbumTracks propagates album ReleaseDate to all tracks in the album
// Used when reading cached albums that may have tracks without year
func (c *CachedProxy) backfillAlbumTracks(a *TidalAlbum) {
	if a.ReleaseDate == "" || len(a.Items) == 0 {
		return
	}
	for i := range a.Items {
		if a.Items[i].Album.ReleaseDate == "" {
			a.Items[i].Album.ReleaseDate = a.ReleaseDate
		}
	}
}

// cacheAlbumJSON adds an album to both LRU and persistent write-back buffer
func (c *CachedProxy) cacheAlbumJSON(a *TidalAlbum) {
	if a == nil || a.ID == 0 {
		return
	}

	albumRef := TidalAlbumRef{
		ID:          a.ID,
		Title:       a.Title,
		Cover:       a.Cover,
		ReleaseDate: a.ReleaseDate, // [YEAR] Propagate album year to tracks
	}

	for i := range a.Items {
		if a.Items[i].Album.ID == 0 {
			a.Items[i].Album = albumRef
		} else if a.Items[i].Album.ReleaseDate == "" {
			// Track has album ref but no year - fill it in
			a.Items[i].Album.ReleaseDate = a.ReleaseDate
		}
		if len(a.Items[i].Artists) == 0 && len(a.Artists) > 0 {
			a.Items[i].Artists = a.Artists
			a.Items[i].Artist = a.Artists[0]
		}
		c.cacheTrackJSON(&a.Items[i])
	}

	key := fmt.Sprintf("td:al:%d", a.ID)
	data, err := json.Marshal(a)
	if err != nil {
		return
	}

	c.albums.Set(key, data, 0)
	if c.db != nil {
		c.pendingMu.Lock()
		c.pending[key] = data
		c.pendingMu.Unlock()
	}
	if a.Cover != "" {
		c.albumArt.Set(key, a.Cover, 0)
	}
}

func (c *CachedProxy) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	key := fmt.Sprintf("td:tr:%d", trackID)

	// 1. Check in-memory LRU cache first
	if cached := c.tracks.Get(key); cached != nil {
		c.pendingMu.Lock()
		if pending := c.pending[key]; pending != nil {
			// Pending has newer data, use it and update LRU
			c.pendingMu.Unlock()
			c.tracks.Set(key, pending, 0)
			var t TidalTrack
			if err := json.Unmarshal(pending, &t); err == nil {
				c.backfillTrackYear(&t) // [YEAR] Backfill from album cache
				return &t, nil
			}
		} else {
			c.pendingMu.Unlock()
		}
		var t TidalTrack
		if err := json.Unmarshal(cached, &t); err == nil {
			c.backfillTrackYear(&t) // [YEAR] Backfill from album cache
			return &t, nil
		} else {
			// Data in pending buffer is corrupt (shouldn't happen)
			c.tracks.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt track in pending buffer key=%s: %v", key, err)
		}
	}

	// [New] Check pending write-back buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var t TidalTrack
		if err := json.Unmarshal(data, &t); err == nil {
			return &t, nil
		} else {
			// Data in pending buffer is corrupt (shouldn't happen)
			c.tracks.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt track in pending buffer key=%s: %v", key, err)
		}
	} else {
		c.pendingMu.Unlock()
	}

	// 2. Check SQLite persistent cache (if db available)
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var t TidalTrack
			if err := json.Unmarshal(cached, &t); err == nil {
				c.backfillTrackYear(&t)      // [YEAR] Backfill from album cache
				c.tracks.Set(key, cached, 0) // Warm LRU cache
				return &t, nil
			} else {
				// Data in SQLite is corrupt
				c.tracks.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt track in SQLite key=%s: %v", key, err)
			}
		}
	}

	// 3. Fetch from API using Normal priority (not critical like streaming)
	ctx = WithPriority(ctx, PriorityNormal)
	t, err := c.TidalProxy.GetTrackInfo(ctx, trackID)
	if err == nil && t != nil {
		c.cacheTrackJSON(t)
	}
	return t, err
}

func (c *CachedProxy) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	key := fmt.Sprintf("td:al:%d", albumID)

	checkCache := func(data []byte) (*TidalAlbum, bool) {
		var a TidalAlbum
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, true // Corrupt data
		}
		// Require items for GetAlbumInfo
		if len(a.Items) > 0 || a.NumberOfTracks == 0 {
			return &a, false
		}
		return nil, false // Valid but incomplete (not corrupt)
	}

	// 1. Check in-memory LRU cache
	if cached := c.albums.Get(key); cached != nil {
		if a, corrupt := checkCache(cached); corrupt {
			c.albums.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt album data in LRU key=%s", key)
		} else if a != nil {
			c.backfillAlbumTracks(a) // [YEAR] Fix tracks that lack year
			return a, nil
		}
	}

	// [New] Check pending write-back buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		if a, corrupt := checkCache(data); corrupt {
			c.albums.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt album in pending buffer key=%s", key)
		} else if a != nil {
			c.backfillAlbumTracks(a) // [YEAR] Fix tracks that lack year
			return a, nil
		}
	} else {
		c.pendingMu.Unlock()
	}

	// 2. Check SQLite persistent cache (if db available)
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			if a, corrupt := checkCache(cached); corrupt {
				c.albums.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt album in SQLite key=%s", key)
			} else if a != nil {
				c.backfillAlbumTracks(a)     // [YEAR] Fix tracks that lack year
				c.albums.Set(key, cached, 0) // Warm LRU cache
				return a, nil
			}
		}
	}

	// 4. Fetch from API using Normal priority
	ctx = WithPriority(ctx, PriorityNormal)
	a, err := c.TidalProxy.GetAlbumInfo(ctx, albumID)
	if err == nil && a != nil {
		c.cacheAlbumJSON(a)
	}
	return a, err
}

func (c *CachedProxy) GetAlbumMetadata(ctx context.Context, albumID int) (*TidalAlbum, error) {
	key := fmt.Sprintf("td:al:%d", albumID)

	// Accepts partial caches without Items
	if cached := c.albums.Get(key); cached != nil {
		var a TidalAlbum
		if err := json.Unmarshal(cached, &a); err == nil {
			return &a, nil
		} else {
			c.albums.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt album metadata in LRU key=%s: %v", key, err)
		}
	}

	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var a TidalAlbum
		if err := json.Unmarshal(data, &a); err == nil {
			return &a, nil
		} else {
			c.albums.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt album metadata in pending key=%s: %v", key, err)
		}
	} else {
		c.pendingMu.Unlock()
	}

	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var a TidalAlbum
			if err := json.Unmarshal(cached, &a); err == nil {
				c.albums.Set(key, cached, 0)
				return &a, nil
			} else {
				c.albums.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt album metadata in SQLite key=%s: %v", key, err)
			}
		}
	}

	// Fetch from API
	a, err := c.TidalProxy.GetAlbumInfo(ctx, albumID)
	if err == nil && a != nil {
		c.cacheAlbumJSON(a)
	}
	return a, err
}

// GetAlbumsInfoBatch fetches multiple albums efficiently using batch SQLite query
// Falls back to individual API calls for cache misses
// Uses MEDIUM tier to avoid saturating LOW tier mirrors
func (c *CachedProxy) GetAlbumsInfoBatch(ctx context.Context, albumIDs []int) map[int]*TidalAlbum {
	if len(albumIDs) == 0 {
		return nil
	}

	ctx = WithPriority(ctx, PriorityNormal)

	result := make(map[int]*TidalAlbum, len(albumIDs))
	missingIDs := make([]int, 0)

	// 1. Check in-memory LRU cache first
	for _, id := range albumIDs {
		key := fmt.Sprintf("td:al:%d", id)
		if cached := c.albums.Get(key); cached != nil {
			var a TidalAlbum
			if err := json.Unmarshal(cached, &a); err == nil {
				if len(a.Items) > 0 || a.NumberOfTracks == 0 {
					c.backfillAlbumTracks(&a) // [YEAR] Fix tracks that lack year
					result[id] = &a
					continue
				}
			} else {
				c.albums.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt album in batch LRU key=%s: %v", key, err)
			}
		}
		missingIDs = append(missingIDs, id)
	}

	if len(missingIDs) == 0 {
		return result
	}

	// 2. Batch query SQLite for remaining IDs (single query vs N queries)
	if c.db != nil {
		keys := make([]string, len(missingIDs))
		for i, id := range missingIDs {
			keys[i] = fmt.Sprintf("td:al:%d", id)
		}

		batch := c.db.GetCachedMetadataBatch(keys)
		if batch != nil {
			// Check which ones we found and warm the LRU cache
			foundIDs := make(map[int]struct{})
			for key, data := range batch {
				var a TidalAlbum
				if err := json.Unmarshal(data, &a); err == nil {
					if len(a.Items) > 0 || a.NumberOfTracks == 0 {
						// Extract ID from key
						var id int
						fmt.Sscanf(key, "td:al:%d", &id)
						if id > 0 {
							c.backfillAlbumTracks(&a) // [YEAR] Fix tracks that lack year
							result[id] = &a
							c.albums.Set(key, data, 0) // Warm LRU
							foundIDs[id] = struct{}{}
						}
					}
				} else {
					c.albums.MarkCorrupt()
					log.Printf("[CACHE ANOMALY] corrupt album in batch SQLite key=%s: %v", key, err)
				}
			}

			// Rebuild missingIDs excluding found ones
			stillMissing := make([]int, 0, len(missingIDs))
			for _, id := range missingIDs {
				if _, found := foundIDs[id]; !found {
					stillMissing = append(stillMissing, id)
				}
			}
			missingIDs = stillMissing
		}
	}

	if len(missingIDs) == 0 {
		return result
	}

	// 3. Fetch missing from API concurrently (with semaphore)
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Limit concurrent API calls

	for _, id := range missingIDs {
		wg.Add(1)
		go func(albumID int) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			a, err := c.TidalProxy.GetAlbumInfo(ctx, albumID)
			if err == nil && a != nil {
				c.cacheAlbumJSON(a)
				mu.Lock()
				result[albumID] = a
				mu.Unlock()
			}
		}(id)
	}

	wg.Wait()
	return result
}

func (c *CachedProxy) GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error) {
	key := fmt.Sprintf("td:ar:%d", artistID)

	// 1. Check in-memory LRU cache
	if cached := c.artists.Get(key); cached != nil {
		var a TidalArtistDetail
		if err := json.Unmarshal(cached, &a); err == nil {
			return &a, nil
		} else {
			c.artists.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt artist data in LRU key=%s: %v", key, err)
		}
	}

	// [New] Check pending write-back buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var a TidalArtistDetail
		if err := json.Unmarshal(data, &a); err == nil {
			return &a, nil
		} else {
			c.artists.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt artist in pending buffer key=%s: %v", key, err)
		}
	} else {
		c.pendingMu.Unlock()
	}

	// 2. Check SQLite persistent cache (if db available)
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var a TidalArtistDetail
			if err := json.Unmarshal(cached, &a); err == nil {
				c.artists.Set(key, cached, 0) // Warm LRU cache
				return &a, nil
			} else {
				c.artists.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt artist in SQLite key=%s: %v", key, err)
			}
		}
	}

	// 3. Fetch from API using Normal priority
	ctx = WithPriority(ctx, PriorityNormal)
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

	// Trigger album fetch to get cover (Normal priority via GetAlbumInfo)
	ctx = WithPriority(ctx, PriorityNormal)
	a, err := c.GetAlbumInfo(ctx, albumID)
	if err == nil && a != nil {
		return a.Cover
	}
	return ""
}

func (c *CachedProxy) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error) {
	ctx = WithPriority(ctx, PriorityNormal) // Search uses Normal priority
	albums, err := c.TidalProxy.SearchAlbums(ctx, query, limit, offset)
	if err == nil {
		for _, a := range albums {
			c.cacheAlbumJSON(&a)
		}
	}
	return albums, err
}

func (c *CachedProxy) SearchTracks(ctx context.Context, query string, limit, offset int) ([]TidalTrack, error) {
	ctx = WithPriority(ctx, PriorityNormal) // Search uses Normal priority
	tracks, err := c.TidalProxy.SearchTracks(ctx, query, limit, offset)
	if err == nil {
		for _, t := range tracks {
			c.cacheTrackJSON(&t)
		}
	}
	return tracks, err
}

func (c *CachedProxy) SearchArtists(ctx context.Context, query string, limit, offset int) ([]TidalArtist, error) {
	ctx = WithPriority(ctx, PriorityNormal) // Search uses Normal priority
	return c.TidalProxy.SearchArtists(ctx, query, limit, offset)
}

func (c *CachedProxy) GetArtistAlbums(ctx context.Context, artistID int, skipTracks bool) (*TidalArtistPage, error) {
	ctx = WithPriority(ctx, PriorityNormal) // Artist albums uses Normal priority
	key := fmt.Sprintf("td:ar:al:%d", artistID)
	if skipTracks {
		key += ":skip"
	}

	// 1. Check LRU
	if cached := c.artistAlbums.Get(key); cached != nil {
		var p TidalArtistPage
		if err := json.Unmarshal(cached, &p); err == nil {
			return &p, nil
		} else {
			c.artistAlbums.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt artist albums in LRU key=%s: %v", key, err)
		}
	}

	// 2. Check Pending Buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var p TidalArtistPage
		if err := json.Unmarshal(data, &p); err == nil {
			return &p, nil
		} else {
			c.artistAlbums.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt artist albums in pending key=%s: %v", key, err)
		}
	} else {
		c.pendingMu.Unlock()
	}

	// 3. Check SQLite
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var p TidalArtistPage
			if err := json.Unmarshal(cached, &p); err == nil {
				c.artistAlbums.Set(key, cached, 0)
				return &p, nil
			} else {
				c.artistAlbums.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt artist albums in SQLite key=%s: %v", key, err)
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

		// Cache results
		if len(page.Albums.Items) > 0 {
			for _, a := range page.Albums.Items {
				c.cacheAlbumJSON(&a)
			}
		}
		if len(page.Tracks) > 0 {
			for _, t := range page.Tracks {
				c.cacheTrackJSON(&t)
			}
		}
	}
	return page, err
}

func (c *CachedProxy) GetArtistTopTracks(ctx context.Context, artistID int, limit int) ([]TidalTrack, error) {
	ctx = WithPriority(ctx, PriorityNormal) // Top tracks uses Normal priority
	key := fmt.Sprintf("td:ar:tt:%d", artistID)

	// 1. Check LRU
	if cached := c.artistTopTracks.Get(key); cached != nil {
		var t []TidalTrack
		if err := json.Unmarshal(cached, &t); err == nil {
			return t, nil
		} else {
			c.artistTopTracks.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt top tracks in LRU key=%s: %v", key, err)
		}
	}

	// 2. Check Pending Buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var t []TidalTrack
		if err := json.Unmarshal(data, &t); err == nil {
			return t, nil
		} else {
			c.artistTopTracks.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt top tracks in pending key=%s: %v", key, err)
		}
	} else {
		c.pendingMu.Unlock()
	}

	// 3. Check SQLite
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var t []TidalTrack
			if err := json.Unmarshal(cached, &t); err == nil {
				c.artistTopTracks.Set(key, cached, 0)
				return t, nil
			} else {
				c.artistTopTracks.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt top tracks in SQLite key=%s: %v", key, err)
			}
		}
	}

	// 4. Fetch from API
	tracks, err := c.TidalProxy.GetArtistTopTracks(ctx, artistID, limit)
	if err == nil && tracks != nil {
		for _, t := range tracks {
			c.cacheTrackJSON(&t)
		}
		// Persist the list itself
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
	ctx = WithPriority(ctx, PriorityNormal) // Similar artists uses Normal priority
	key := fmt.Sprintf("td:ar:sim:%d", artistID)

	// 1. Check LRU
	if cached := c.similarArtists.Get(key); cached != nil {
		var a []TidalArtist
		if err := json.Unmarshal(cached, &a); err == nil {
			return a, nil
		} else {
			c.similarArtists.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt similar artists in LRU key=%s: %v", key, err)
		}
	}

	// 2. Check Pending Buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var a []TidalArtist
		if err := json.Unmarshal(data, &a); err == nil {
			return a, nil
		} else {
			c.similarArtists.MarkCorrupt()
			log.Printf("[CACHE ANOMALY] corrupt similar artists in pending key=%s: %v", key, err)
		}
	} else {
		c.pendingMu.Unlock()
	}

	// 3. Check SQLite
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			var a []TidalArtist
			if err := json.Unmarshal(cached, &a); err == nil {
				c.similarArtists.Set(key, cached, 0)
				return a, nil
			} else {
				c.similarArtists.MarkCorrupt()
				log.Printf("[CACHE ANOMALY] corrupt similar artists in SQLite key=%s: %v", key, err)
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
	ctx = WithPriority(ctx, PriorityNormal) // Top tracks uses Normal priority
	tracks, err := c.TidalProxy.GetTopTracks(ctx, limit)
	if err == nil {
		for _, t := range tracks {
			c.cacheTrackJSON(&t)
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetRecommendations(ctx context.Context, trackID int) ([]TidalTrack, error) {
	ctx = WithPriority(ctx, PriorityNormal) // Recommendations uses Normal priority
	tracks, err := c.TidalProxy.GetRecommendations(ctx, trackID)
	if err == nil {
		for _, t := range tracks {
			c.cacheTrackJSON(&t)
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetStreamURL(ctx context.Context, trackID int, quality string, clientIP string) (string, error) {
	// Check negative cache first - if track is known to be unavailable, fail fast
	unavailableKey := fmt.Sprintf("td:unavailable:%d", trackID)
	if failTime := c.unavailableTracks.Get(unavailableKey); !failTime.IsZero() {
		return "", fmt.Errorf("track %d is unavailable (cached since %v)", trackID, failTime)
	}

	key := fmt.Sprintf("td:st:%d:%s:%s", trackID, quality, clientIP)
	if url := c.streamURLs.Get(key); url != "" {
		return url, nil
	}

	url, err := c.TidalProxy.GetStreamURL(ctx, trackID, quality, clientIP)
	if err == nil && url != "" {
		// [CRITICAL] Only cache progressive/BTS URLs, NOT HLS manifests
		// HLS URLs expire quickly and prevent BTS fallback from working
		isHLS := strings.Contains(url, ".m3u8") ||
			strings.Contains(url, "manifest") ||
			strings.Contains(url, "/manifests/")
		if !isHLS {
			c.streamURLs.Set(key, url, 0)
			log.Printf("[CACHE] Cached BTS URL for track=%d quality=%s (progressive)", trackID, quality)
		} else {
			log.Printf("[CACHE] Skipping cache for HLS URL track=%d (manifest expires quickly)", trackID)
		}
	} else if err != nil {
		// Check if error indicates track is permanently unavailable
		// This prevents wasting resources on region-blocked or deleted tracks
		// BE CONSERVATIVE: Only cache as unavailable when definitively clear
		errStr := strings.ToLower(err.Error())
		isDefinitivelyUnavailable := false

		// Only these patterns indicate genuine track unavailability:
		// 1. Explicit "preview" - track only available as preview clip
		// 2. Explicit "not available" or "unavailable" message (but NOT "upstream api error")
		// 3. Region-blocked content (explicit "region" or "restricted")
		if strings.Contains(errStr, "preview") {
			isDefinitivelyUnavailable = true
		} else if strings.Contains(errStr, "not available") ||
			(strings.Contains(errStr, "unavailable") && !strings.Contains(errStr, "upstream")) {
			isDefinitivelyUnavailable = true
		} else if strings.Contains(errStr, "region") || strings.Contains(errStr, "restricted") {
			isDefinitivelyUnavailable = true
		}

		// 403 errors are AMBIGUOUS - they can be:
		// - Auth/session issues (transient)
		// - Track unavailable (permanent)
		// - Rate limiting (transient)
		// Only mark as unavailable if it explicitly mentions auth keywords
		if strings.Contains(errStr, "403") {
			if strings.Contains(errStr, "auth") ||
				strings.Contains(errStr, "session") ||
				strings.Contains(errStr, "token") ||
				strings.Contains(errStr, "unauthorized") {
				// This is an auth error, NOT track unavailable - don't cache
				isDefinitivelyUnavailable = false
			} else if strings.Contains(errStr, "preview") ||
				strings.Contains(errStr, "region") ||
				strings.Contains(errStr, "restricted") {
				// These are genuine unavailability reasons
				isDefinitivelyUnavailable = true
			}
			// Other 403 errors (like "upstream api error") are NOT cached
			// because they might be transient proxy issues
		}

		if isDefinitivelyUnavailable {
			c.unavailableTracks.Set(unavailableKey, time.Now(), 0)
			log.Printf("[CACHE] Track %d marked as unavailable: %v (cached for 10 min)", trackID, err)
		} else {
			log.Printf("[CACHE] Track %d error not cached as unavailable (transient): %v", trackID, err)
		}
	}
	return url, err
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
	c.streamURLs.Stop()
	c.unavailableTracks.Stop()
}

// CacheLevelStats holds stats for a single cache level
type CacheLevelStats struct {
	Name      string
	Size      int
	Hits      int64
	Misses    int64
	Evictions int64
	Corrupt   int64 // Invalid/corrupted entries detected
	HitRate   float64
}

// CacheStats holds statistics for all cache levels
type CacheStats struct {
	// In-memory LRU caches
	Tracks          CacheLevelStats
	Albums          CacheLevelStats
	Artists         CacheLevelStats
	AlbumArt        CacheLevelStats
	AlbumCount      CacheLevelStats
	ArtistAlbums    CacheLevelStats
	ArtistTopTracks CacheLevelStats
	SimilarArtists  CacheLevelStats
	StreamURLs      CacheLevelStats
	Unavailable     CacheLevelStats
	// Write-back buffer
	PendingBuffer int
	// SQLite persistent cache
	SQLiteTotal   int64
	SQLiteExpired int64
}

// Stats returns current statistics for all cache levels
func (c *CachedProxy) Stats() CacheStats {
	tracksStats := c.tracks.Stats()
	albumsStats := c.albums.Stats()
	artistsStats := c.artists.Stats()
	albumArtStats := c.albumArt.Stats()
	albumCountStats := c.albumCount.Stats()
	artistAlbumsStats := c.artistAlbums.Stats()
	artistTopTracksStats := c.artistTopTracks.Stats()
	similarArtistsStats := c.similarArtists.Stats()
	streamURLsStats := c.streamURLs.Stats()
	unavailableStats := c.unavailableTracks.Stats()

	c.pendingMu.Lock()
	pendingCount := len(c.pending)
	c.pendingMu.Unlock()

	var sqliteTotal, sqliteExpired int64
	if c.db != nil {
		sqliteTotal, sqliteExpired, _, _ = c.db.GetCacheStats()
	}

	return CacheStats{
		Tracks: CacheLevelStats{
			Name: "tracks", Size: tracksStats.Size, Hits: tracksStats.Hits,
			Misses: tracksStats.Misses, Evictions: tracksStats.Evictions, Corrupt: tracksStats.Corrupt, HitRate: tracksStats.HitRate,
		},
		Albums: CacheLevelStats{
			Name: "albums", Size: albumsStats.Size, Hits: albumsStats.Hits,
			Misses: albumsStats.Misses, Evictions: albumsStats.Evictions, Corrupt: albumsStats.Corrupt, HitRate: albumsStats.HitRate,
		},
		Artists: CacheLevelStats{
			Name: "artists", Size: artistsStats.Size, Hits: artistsStats.Hits,
			Misses: artistsStats.Misses, Evictions: artistsStats.Evictions, Corrupt: artistsStats.Corrupt, HitRate: artistsStats.HitRate,
		},
		AlbumArt: CacheLevelStats{
			Name: "album-art", Size: albumArtStats.Size, Hits: albumArtStats.Hits,
			Misses: albumArtStats.Misses, Evictions: albumArtStats.Evictions, Corrupt: albumArtStats.Corrupt, HitRate: albumArtStats.HitRate,
		},
		AlbumCount: CacheLevelStats{
			Name: "album-count", Size: albumCountStats.Size, Hits: albumCountStats.Hits,
			Misses: albumCountStats.Misses, Evictions: albumCountStats.Evictions, Corrupt: albumCountStats.Corrupt, HitRate: albumCountStats.HitRate,
		},
		ArtistAlbums: CacheLevelStats{
			Name: "artist-albums", Size: artistAlbumsStats.Size, Hits: artistAlbumsStats.Hits,
			Misses: artistAlbumsStats.Misses, Evictions: artistAlbumsStats.Evictions, Corrupt: artistAlbumsStats.Corrupt, HitRate: artistAlbumsStats.HitRate,
		},
		ArtistTopTracks: CacheLevelStats{
			Name: "artist-top-tracks", Size: artistTopTracksStats.Size, Hits: artistTopTracksStats.Hits,
			Misses: artistTopTracksStats.Misses, Evictions: artistTopTracksStats.Evictions, Corrupt: artistTopTracksStats.Corrupt, HitRate: artistTopTracksStats.HitRate,
		},
		SimilarArtists: CacheLevelStats{
			Name: "similar-artists", Size: similarArtistsStats.Size, Hits: similarArtistsStats.Hits,
			Misses: similarArtistsStats.Misses, Evictions: similarArtistsStats.Evictions, Corrupt: similarArtistsStats.Corrupt, HitRate: similarArtistsStats.HitRate,
		},
		StreamURLs: CacheLevelStats{
			Name: "stream-urls", Size: streamURLsStats.Size, Hits: streamURLsStats.Hits,
			Misses: streamURLsStats.Misses, Evictions: streamURLsStats.Evictions, Corrupt: streamURLsStats.Corrupt, HitRate: streamURLsStats.HitRate,
		},
		Unavailable: CacheLevelStats{
			Name: "unavailable-tracks", Size: unavailableStats.Size, Hits: unavailableStats.Hits,
			Misses: unavailableStats.Misses, Evictions: unavailableStats.Evictions, Corrupt: unavailableStats.Corrupt, HitRate: unavailableStats.HitRate,
		},
		PendingBuffer: pendingCount,
		SQLiteTotal:   sqliteTotal,
		SQLiteExpired: sqliteExpired,
	}
}

// GetPlaylist fetches playlist metadata and tracks (no caching for playlists)
func (c *CachedProxy) GetPlaylist(ctx context.Context, playlistUUID string) (*TidalPlaylist, error) {
	// Playlists are dynamic, don't cache them
	return c.TidalProxy.GetPlaylist(ctx, playlistUUID)
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
	c.streamURLs.Clear()
	c.unavailableTracks.Clear()
	log.Printf("[CACHE] In-memory LRU caches cleared")
}
