package tidalproxy

import (
	"container/list"
	"context"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// cacheEntry holds a cached value with metadata.
type cacheEntry[T any] struct {
	value   T
	expiry  time.Time
	element *list.Element // Pointer to LRU list element
}

// Cache is a thread-safe LRU cache with TTL support.
// Type-safe using generics to avoid panics from type assertions.
type Cache[T any] struct {
	name    string
	maxSize int
	ttl     time.Duration

	mu      sync.RWMutex
	entries map[string]*cacheEntry[T]
	lru     *list.List

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64

	stopCleanup chan struct{}
	stopped     atomic.Bool
}

// CacheConfig holds configuration for a cache.
type CacheConfig struct {
	Name            string        // Cache identifier for logging
	MaxSize         int           // Maximum entries (0 = unlimited)
	DefaultTTL      time.Duration // Default TTL (0 = no expiration)
	CleanupInterval time.Duration // Cleanup interval (0 = disabled)
}

// validateKey checks if a key is valid (non-empty string).
func validateKey(key string) bool {
	return key != ""
}

// NewCache creates a new cache with the given configuration.
func NewCache[T any](cfg CacheConfig) *Cache[T] {
	// Validate configuration
	if cfg.MaxSize < 0 {
		cfg.MaxSize = 0
	}
	if cfg.DefaultTTL < 0 {
		cfg.DefaultTTL = 0
	}
	if cfg.CleanupInterval < 0 {
		cfg.CleanupInterval = 0
	}

	c := &Cache[T]{
		name:        cfg.Name,
		maxSize:     cfg.MaxSize,
		ttl:         cfg.DefaultTTL,
		entries:     make(map[string]*cacheEntry[T]),
		lru:         list.New(),
		stopCleanup: make(chan struct{}),
	}

	if cfg.CleanupInterval > 0 {
		go c.cleanupLoop(cfg.CleanupInterval)
	}

	return c
}

// Get retrieves a value by key. Returns zero value if not found or expired.
func (c *Cache[T]) Get(key string) T {
	var zero T

	if !validateKey(key) {
		return zero
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[key]
	if !exists {
		c.misses.Add(1)
		return zero
	}

	// Check expiration
	if !entry.expiry.IsZero() && time.Now().After(entry.expiry) {
		c.misses.Add(1)
		c.removeEntry(key, entry)
		return zero
	}

	// Move to front of LRU and update stats
	c.lru.MoveToFront(entry.element)
	c.hits.Add(1)
	return entry.value
}

// Set stores a value with optional TTL. Uses config default if TTL is zero.
func (c *Cache[T]) Set(key string, value T, ttl time.Duration) {
	if !validateKey(key) {
		return
	}

	if ttl == 0 {
		ttl = c.ttl
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove existing entry if present
	if entry, exists := c.entries[key]; exists {
		c.removeEntry(key, entry)
	}

	// Calculate expiry
	var expiry time.Time
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}

	// Add new entry at front
	element := c.lru.PushFront(key)
	c.entries[key] = &cacheEntry[T]{
		value:   value,
		expiry:  expiry,
		element: element,
	}

	// Evict oldest if at capacity
	if c.maxSize > 0 && c.lru.Len() > c.maxSize {
		c.evictOldest()
	}
}

// Delete removes a specific key.
func (c *Cache[T]) Delete(key string) {
	if !validateKey(key) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.entries[key]; exists {
		c.removeEntry(key, entry)
	}
}

// Size returns the current number of entries.
func (c *Cache[T]) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lru.Len()
}

// Stats returns cache statistics.
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	Size      int
	HitRate   float64 // Hit rate as percentage (0-100)
}

func (c *Cache[T]) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return CacheStats{
		Hits:      hits,
		Misses:    misses,
		Evictions: c.evictions.Load(),
		Size:      c.lru.Len(),
		HitRate:   hitRate,
	}
}

// removeEntry removes an entry from both map and list.
// Must be called with write lock held.
func (c *Cache[T]) removeEntry(key string, entry *cacheEntry[T]) {
	c.lru.Remove(entry.element)
	delete(c.entries, key)
}

// evictOldest removes the least recently used entry.
// Must be called with write lock held.
func (c *Cache[T]) evictOldest() {
	oldest := c.lru.Back()
	if oldest == nil {
		return
	}

	// Type-safe extraction of key from LRU list
	key, ok := oldest.Value.(string)
	if !ok {
		log.Printf("[cache:%s] WARNING: LRU list contains non-string value: %T", c.name, oldest.Value)
		return
	}

	if entry, exists := c.entries[key]; exists {
		c.removeEntry(key, entry)
		c.evictions.Add(1)
	}
}

// cleanupLoop periodically removes expired entries.
func (c *Cache[T]) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

// cleanupExpired removes all expired entries.
func (c *Cache[T]) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.entries {
		if !entry.expiry.IsZero() && now.After(entry.expiry) {
			c.removeEntry(key, entry)
		}
	}
}

// Stop stops the cleanup goroutine. Safe to call multiple times.
func (c *Cache[T]) Stop() {
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stopCleanup)
	}
}

// CachedProxy wraps TidalProxy with type-safe LRU+TTL caches
type CachedProxy struct {
	TidalProxy
	tracks     *Cache[*TidalTrack]
	albums     *Cache[*TidalAlbum]
	artists    *Cache[*TidalArtistDetail]
	albumArt   *Cache[string] // int -> string UUID
	albumCount *Cache[int]    // artistID -> int (album count)
}

func NewCachedProxy(base TidalProxy, ttl time.Duration) *CachedProxy {
	return &CachedProxy{
		TidalProxy: base,
		tracks: NewCache[*TidalTrack](CacheConfig{
			Name:            "tidal-tracks",
			MaxSize:         500,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albums: NewCache[*TidalAlbum](CacheConfig{
			Name:            "tidal-albums",
			MaxSize:         200,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		artists: NewCache[*TidalArtistDetail](CacheConfig{
			Name:            "tidal-artists",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
		albumArt: NewCache[string](CacheConfig{
			Name:            "tidal-album-art",
			MaxSize:         1000,
			DefaultTTL:      ttl * 2, // Album art changes less frequently
			CleanupInterval: 10 * time.Minute,
		}),
		albumCount: NewCache[int](CacheConfig{
			Name:            "tidal-album-count",
			MaxSize:         100,
			DefaultTTL:      ttl,
			CleanupInterval: 10 * time.Minute,
		}),
	}
}

func (c *CachedProxy) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	key := strconv.Itoa(trackID)
	if cached := c.tracks.Get(key); cached != nil {
		return cached, nil
	}
	t, err := c.TidalProxy.GetTrackInfo(ctx, trackID)
	if err == nil && t != nil {
		c.tracks.Set(key, t, 0)
		if t.Album.Cover != "" {
			c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
		}
	}
	return t, err
}

func (c *CachedProxy) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	key := strconv.Itoa(albumID)
	if cached := c.albums.Get(key); cached != nil {
		return cached, nil
	}
	t, err := c.TidalProxy.GetAlbumInfo(ctx, albumID)
	if err == nil && t != nil {
		c.albums.Set(key, t, 0)
		if t.Cover != "" {
			c.albumArt.Set(key, t.Cover, 0)
		}
	}
	return t, err
}

func (c *CachedProxy) GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error) {
	key := strconv.Itoa(artistID)
	if cached := c.artists.Get(key); cached != nil {
		return cached, nil
	}
	t, err := c.TidalProxy.GetArtistInfo(ctx, artistID)
	if err == nil && t != nil {
		c.artists.Set(key, t, 0)
	}
	return t, err
}

func (c *CachedProxy) GetCoverUUIDForAlbum(ctx context.Context, albumID int) string {
	key := strconv.Itoa(albumID)
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
				c.albumArt.Set(strconv.Itoa(a.ID), a.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
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
					c.albumArt.Set(strconv.Itoa(a.ID), a.Cover, 0)
				}
			}
		}
		for _, t := range page.Tracks {
			if t.Album.Cover != "" {
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
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
				c.albumArt.Set(strconv.Itoa(t.Album.ID), t.Album.Cover, 0)
			}
		}
	}
	return tracks, err
}

// GetArtistAlbumCount returns cached album count for an artist, fetching if needed
func (c *CachedProxy) GetArtistAlbumCount(ctx context.Context, artistID int) int {
	key := strconv.Itoa(artistID)
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
