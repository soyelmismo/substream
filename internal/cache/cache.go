// Package cache provides a type-safe LRU + TTL cache system using generics.
// Designed for single instances with few users.
//
// BASIC USAGE:
//
//	// Create cache
//	cache := New[int](Config{
//	    Name:            "genre-tracks",
//	    MaxSize:         100,
//	    DefaultTTL:      30 * time.Minute,
//	    CleanupInterval: 5 * time.Minute,
//	})
//	defer cache.Stop()
//
//	// Use cache
//	cache.Set("rock", []int{1, 2, 3}, 0) // Uses default TTL
//	tracks := cache.Get("rock")
//
//	// With custom TTL
//	cache.Set("temp", data, 5*time.Minute)
//
//	// Peek without affecting LRU or stats
//	if val := cache.Peek("rock"); val != nil {
//	    // Value exists but LRU order unchanged
//	}
//
//	// Cache-aside pattern
//	tracks := cache.GetOrSet("rock", fetchTracks(), 0)
//
//	// Lazy loading with context support
//	tracks, err := cache.GetWithLoader(ctx, "rock", func(ctx context.Context) (int, error) {
//	    return fetchFromDB(ctx, "rock")
//	}, 0)
//
//	// Metrics
//	stats := cache.Stats()
//	// stats.Hits, stats.Misses, stats.Evictions, stats.Size, stats.HitRate
//
//	// Reset stats
//	cache.ResetStats()
//
// FEATURES:
//
//   - Type-safe with generics (no type assertions)
//   - Automatic LRU when MaxSize is reached
//   - Configurable TTL per entry
//   - Periodic cleanup of expired entries
//   - Built-in metrics (hits, misses, evictions, hit rate)
//   - Thread-safe with mutex + lock upgrade
//   - Peek() for read-only access without LRU update
//   - GetOrSet() for cache-aside pattern
//   - GetWithLoader() for lazy loading with context support
//   - ResetStats() to clear metrics
//   - Name() for debugging and logging
package cache

import (
	"container/list"
	"context"
	"fmt"
	"log"
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

// Config holds configuration for a cache.
type Config struct {
	Name            string        // Cache identifier for logging
	MaxSize         int           // Maximum entries (0 = unlimited)
	DefaultTTL      time.Duration // Default TTL (0 = no expiration)
	CleanupInterval time.Duration // Cleanup interval (0 = disabled)
}

// validateKey checks if a key is valid (non-empty string).
// Empty keys are rejected to prevent ambiguity with zero values
// and to maintain consistent behavior across all operations.
// Returns false for empty strings, preventing cache operations.
func validateKey(key string) bool {
	return key != ""
}

// New creates a new cache with the given configuration.
func New[T any](cfg Config) *Cache[T] {
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

// Peek retrieves a value by key without updating LRU order.
// Returns zero value if not found or expired. Does not affect hit/miss stats.
func (c *Cache[T]) Peek(key string) T {
	var zero T

	if !validateKey(key) {
		return zero
	}

	c.mu.RLock()
	entry, exists := c.entries[key]
	c.mu.RUnlock()

	if !exists {
		return zero
	}

	// Check expiration
	if !entry.expiry.IsZero() && time.Now().After(entry.expiry) {
		return zero
	}

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

// GetOrSet returns the cached value if valid, otherwise stores and returns the provided value.
// Useful for cache-aside pattern where you want to fetch and cache in one operation.
// The value is only stored if the key is missing or expired.
func (c *Cache[T]) GetOrSet(key string, value T, ttl time.Duration) T {
	if !validateKey(key) {
		return value
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[key]

	// Check if entry exists and is not expired
	if exists {
		if entry.expiry.IsZero() || time.Now().Before(entry.expiry) {
			// Valid entry exists, update LRU and stats
			c.lru.MoveToFront(entry.element)
			c.hits.Add(1)
			return entry.value
		}
		// Entry expired, remove it
		c.removeEntry(key, entry)
		c.misses.Add(1)
	} else {
		c.misses.Add(1)
	}

	// Value not in cache or expired, store it
	if ttl == 0 {
		ttl = c.ttl
	}

	// Remove existing entry if present (handles race where entry was added after we checked)
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

	return value
}

// LoaderFunc is a function that loads a value for the cache.
// It receives a context for cancellation and timeout support.
type LoaderFunc[T any] func(ctx context.Context) (T, error)

// GetWithLoader returns the cached value if valid, otherwise calls the loader function
// to fetch and cache the value. This is useful for lazy loading patterns where you
// want to fetch data only when needed and cache it for subsequent accesses.
//
// The loader function is called with the provided context, allowing for cancellation
// and timeout control. If the loader returns an error, the error is propagated and
// the value is not cached.
//
// Example:
//
//	tracks, err := cache.GetWithLoader(ctx, "rock", func(ctx context.Context) ([]int, error) {
//	    return fetchTracksFromDB(ctx, "rock")
//	}, time.Hour)
func (c *Cache[T]) GetWithLoader(ctx context.Context, key string, loader LoaderFunc[T], ttl time.Duration) (T, error) {
	var zero T

	if !validateKey(key) {
		return zero, fmt.Errorf("invalid cache key: key cannot be empty")
	}

	// Try to get from cache first
	c.mu.Lock()
	entry, exists := c.entries[key]
	if exists {
		if entry.expiry.IsZero() || time.Now().Before(entry.expiry) {
			// Valid entry exists, update LRU and stats
			c.lru.MoveToFront(entry.element)
			c.hits.Add(1)
			c.mu.Unlock()
			return entry.value, nil
		}
		// Entry expired, remove it
		c.removeEntry(key, entry)
		c.misses.Add(1)
	} else {
		c.misses.Add(1)
	}
	c.mu.Unlock()

	// Value not in cache or expired, call loader
	value, err := loader(ctx)
	if err != nil {
		return zero, err
	}

	// Store the loaded value
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use default TTL if not specified
	if ttl == 0 {
		ttl = c.ttl
	}

	// Remove existing entry if present (handles race where entry was added after we checked)
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

	return value, nil
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

// Clear removes all entries.
func (c *Cache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*cacheEntry[T])
	c.lru.Init()
}

// Size returns the current number of entries.
func (c *Cache[T]) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lru.Len()
}

// Name returns the cache name for debugging and logging purposes.
func (c *Cache[T]) Name() string {
	return c.name
}

// Has returns true if the key exists and is not expired.
// Does not update LRU order or stats. Useful for existence checks without retrieval.
func (c *Cache[T]) Has(key string) bool {
	if !validateKey(key) {
		return false
	}

	c.mu.RLock()
	entry, exists := c.entries[key]
	c.mu.RUnlock()

	if !exists {
		return false
	}

	// Check expiration
	if !entry.expiry.IsZero() && time.Now().After(entry.expiry) {
		return false
	}

	return true
}

// Keys returns all non-expired keys in the cache.
// Does not update LRU order or stats. Useful for debugging and inspection.
func (c *Cache[T]) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	keys := make([]string, 0, len(c.entries))
	for key, entry := range c.entries {
		if entry.expiry.IsZero() || !now.After(entry.expiry) {
			keys = append(keys, key)
		}
	}
	return keys
}

// GetExpiry returns the expiration time for a key.
// Returns zero time if key doesn't exist or is expired.
// Does not update LRU order or stats.
func (c *Cache[T]) GetExpiry(key string) time.Time {
	if !validateKey(key) {
		return time.Time{}
	}

	c.mu.RLock()
	entry, exists := c.entries[key]
	c.mu.RUnlock()

	if !exists {
		return time.Time{}
	}

	// Check expiration
	if !entry.expiry.IsZero() && time.Now().After(entry.expiry) {
		return time.Time{}
	}

	return entry.expiry
}

// Stats returns cache statistics.
type Stats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	Size      int
	HitRate   float64 // Hit rate as percentage (0-100)
}

func (c *Cache[T]) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return Stats{
		Hits:      hits,
		Misses:    misses,
		Evictions: c.evictions.Load(),
		Size:      c.lru.Len(),
		HitRate:   hitRate,
	}
}

// ResetStats clears all statistics counters (hits, misses, evictions).
func (c *Cache[T]) ResetStats() {
	c.hits.Store(0)
	c.misses.Store(0)
	c.evictions.Store(0)
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
		// This should never happen if cache is used correctly.
		// Log the error for debugging rather than panicking.
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
// Note: This iterates over all entries (O(n)), which is acceptable for the
// intended use case (single instance with few users) since it runs in a
// background goroutine and doesn't block cache operations. For very large
// caches, consider a priority queue-based approach for O(log n) expiration.
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
