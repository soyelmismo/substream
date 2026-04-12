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

// cacheAlbumJSON adds an album to both LRU and persistent write-back buffer
func (c *CachedProxy) cacheAlbumJSON(a *TidalAlbum) {
	if a == nil || a.ID == 0 {
		return
	}

	albumRef := TidalAlbumRef{
		ID:    a.ID,
		Title: a.Title,
		Cover: a.Cover,
	}

	for i := range a.Items {
		if a.Items[i].Album.ID == 0 {
			a.Items[i].Album = albumRef
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
		c.cacheTrackJSON(t)
	}
	return t, err
}

func (c *CachedProxy) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	key := fmt.Sprintf("td:al:%d", albumID)

	checkCache := func(data []byte) *TidalAlbum {
		var a TidalAlbum
		if err := json.Unmarshal(data, &a); err == nil {
			// Require items for GetAlbumInfo
			if len(a.Items) > 0 || a.NumberOfTracks == 0 {
				return &a
			}
		}
		return nil
	}

	// 1. Check in-memory LRU cache
	if cached := c.albums.Get(key); cached != nil {
		if a := checkCache(cached); a != nil {
			return a, nil
		}
	}

	// [New] Check pending write-back buffer
	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		if a := checkCache(data); a != nil {
			return a, nil
		}
	} else {
		c.pendingMu.Unlock()
	}

	// 2. Check SQLite persistent cache (if db available)
	if c.db != nil {
		if cached := c.db.GetCachedMetadata(key); cached != nil {
			if a := checkCache(cached); a != nil {
				c.albums.Set(key, cached, 0) // Warm LRU cache
				return a, nil
			}
		}
	}

	// 3. Fetch from API
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
		}
	}

	c.pendingMu.Lock()
	if data, ok := c.pending[key]; ok {
		c.pendingMu.Unlock()
		var a TidalAlbum
		if err := json.Unmarshal(data, &a); err == nil {
			return &a, nil
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
			c.cacheAlbumJSON(&a)
		}
	}
	return albums, err
}

func (c *CachedProxy) SearchTracks(ctx context.Context, query string, limit, offset int) ([]TidalTrack, error) {
	tracks, err := c.TidalProxy.SearchTracks(ctx, query, limit, offset)
	if err == nil {
		for _, t := range tracks {
			c.cacheTrackJSON(&t)
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
			c.cacheTrackJSON(&t)
		}
	}
	return tracks, err
}

func (c *CachedProxy) GetRecommendations(ctx context.Context, trackID int) ([]TidalTrack, error) {
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
		c.streamURLs.Set(key, url, 0)
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
