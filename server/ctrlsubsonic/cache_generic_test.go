package ctrlsubsonic

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// awaitExpiration waits for a cache entry to expire by polling.
// This is more reliable than fixed sleep times in tests.
func awaitExpiration[T comparable](cache *Cache[T], key string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	initialStats := cache.Stats()
	pollInterval := 2 * time.Millisecond // Reduced for faster detection

	for time.Now().Before(deadline) {
		var zero T
		val := cache.Get(key)
		if val == zero {
			// Verify it was actually a miss (entry expired), not just a zero value
			stats := cache.Stats()
			if stats.Misses > initialStats.Misses {
				return true
			}
		}
		time.Sleep(pollInterval)
	}
	return false
}

func TestCache_BasicOperations(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Test set and get
	cache.Set("key1", 100, 0)
	val := cache.Get("key1")
	if val != 100 {
		t.Errorf("Expected 100, got %d", val)
	}

	// Test non-existent key
	val = cache.Get("nonexistent")
	if val != 0 {
		t.Errorf("Expected 0 for non-existent key, got %d", val)
	}

	// Test delete
	cache.Delete("key1")
	val = cache.Get("key1")
	if val != 0 {
		t.Errorf("Expected 0 after delete, got %d", val)
	}
}

func TestCache_TTL(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		DefaultTTL:      50 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	cache.Set("key1", 100, 0)

	// Should be available immediately
	val := cache.Get("key1")
	if val != 100 {
		t.Errorf("Expected 100 before expiry, got %d", val)
	}

	// Wait for expiry using helper
	if !awaitExpiration(cache, "key1", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}

	val = cache.Get("key1")
	if val != 0 {
		t.Errorf("Expected 0 after expiry, got %d", val)
	}
}

func TestCache_LRUEviction(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         3,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Fill cache to capacity
	cache.Set("key1", 1, 0)
	cache.Set("key2", 2, 0)
	cache.Set("key3", 3, 0)

	// Access key1 to make it recently used
	cache.Get("key1")

	// Add key4, should evict key2 (least recently used)
	cache.Set("key4", 4, 0)

	// key1 should still exist
	if val := cache.Get("key1"); val != 1 {
		t.Errorf("Expected key1 to exist, got %d", val)
	}

	// key2 should be evicted
	if val := cache.Get("key2"); val != 0 {
		t.Errorf("Expected key2 to be evicted, got %d", val)
	}

	// key3 and key4 should exist
	if val := cache.Get("key3"); val != 3 {
		t.Errorf("Expected key3 to exist, got %d", val)
	}
	if val := cache.Get("key4"); val != 4 {
		t.Errorf("Expected key4 to exist, got %d", val)
	}
}

func TestCache_ConcurrentAccess(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         100,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	done := make(chan bool)

	// Concurrent writers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("writer-%d-%d", id, j)
				cache.Set(key, id*1000+j, 0)
			}
			done <- true
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("writer-%d-%d", id, j)
				cache.Get(key)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Verify no race conditions and correct size
	stats := cache.Stats()
	if stats.Size > cfg.MaxSize {
		t.Errorf("Cache size %d exceeds max %d", stats.Size, cfg.MaxSize)
	}
}

func TestCache_Stats(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Initial stats
	stats := cache.Stats()
	if stats.Hits != 0 || stats.Misses != 0 {
		t.Errorf("Expected zero initial stats, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}
	if stats.HitRate != 0 {
		t.Errorf("Expected zero initial hit rate, got %f", stats.HitRate)
	}

	// Generate a hit
	cache.Set("key1", 100, 0)
	cache.Get("key1")

	stats = cache.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}
	if stats.HitRate != 100.0 {
		t.Errorf("Expected 100%% hit rate, got %f", stats.HitRate)
	}

	// Generate a miss
	cache.Get("nonexistent")

	stats = cache.Stats()
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
	if stats.HitRate != 50.0 {
		t.Errorf("Expected 50%% hit rate, got %f", stats.HitRate)
	}
}

func TestCache_Clear(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Add some entries
	for i := 0; i < 5; i++ {
		cache.Set(fmt.Sprintf("key%d", i), i, 0)
	}

	if cache.Size() != 5 {
		t.Errorf("Expected size 5 before clear, got %d", cache.Size())
	}

	// Clear all
	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", cache.Size())
	}
}

func TestCache_CustomTTL(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		DefaultTTL:      time.Hour,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Set with custom short TTL
	cache.Set("key1", 100, 50*time.Millisecond)

	// Should be available immediately
	if val := cache.Get("key1"); val != 100 {
		t.Errorf("Expected 100 before expiry, got %d", val)
	}

	// Wait for custom TTL using helper
	if !awaitExpiration(cache, "key1", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}

	if val := cache.Get("key1"); val != 0 {
		t.Errorf("Expected 0 after custom TTL expiry, got %d", val)
	}
}

func TestCache_CustomTTLWithZeroGlobal(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		DefaultTTL:      0, // No global TTL
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Set with custom TTL even though global TTL is 0
	cache.Set("key1", 100, 50*time.Millisecond)

	// Should be available immediately
	if val := cache.Get("key1"); val != 100 {
		t.Errorf("Expected 100 before expiry, got %d", val)
	}

	// Wait for custom TTL to expire using helper
	if !awaitExpiration(cache, "key1", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}

	// Should be expired even though global TTL is 0
	if val := cache.Get("key1"); val != 0 {
		t.Errorf("Expected 0 after custom TTL expiry (with global TTL=0), got %d", val)
	}

	// Verify a second entry with no TTL doesn't expire
	cache.Set("key2", 200, 0)
	time.Sleep(10 * time.Millisecond)
	if val := cache.Get("key2"); val != 200 {
		t.Errorf("Expected 200 for entry with no TTL, got %d", val)
	}
}

func TestCache_Cleanup(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		DefaultTTL:      50 * time.Millisecond,
		CleanupInterval: 100 * time.Millisecond,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Add entries
	cache.Set("key1", 1, 0)
	cache.Set("key2", 2, 0)

	if cache.Size() != 2 {
		t.Errorf("Expected size 2, got %d", cache.Size())
	}

	// Wait for cleanup
	time.Sleep(200 * time.Millisecond)

	// Entries should be cleaned up
	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after cleanup, got %d", cache.Size())
	}
}

func TestCache_MultipleStop(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 100 * time.Millisecond,
	}

	cache := NewCache[int](cfg)

	// Should not panic
	cache.Stop()
	cache.Stop()
	cache.Stop()
}

func TestCache_ZeroValues(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Set zero value
	cache.Set("zero", 0, 0)

	// Verify it's actually cached (hit, not miss)
	stats := cache.Stats()
	val := cache.Get("zero")
	stats = cache.Stats()
	if val != 0 {
		t.Errorf("Expected 0, got %d", val)
	}
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit for zero value, got %d", stats.Hits)
	}
}

func TestCache_NegativeConfig(t *testing.T) {
	tests := []struct {
		name             string
		cfg              CacheConfig
		expectMaxSize    int
		expectDefaultTTL time.Duration
	}{
		{
			name: "all negative values",
			cfg: CacheConfig{
				Name:            "test",
				MaxSize:         -10,
				DefaultTTL:      -time.Minute,
				CleanupInterval: -time.Second,
			},
			expectMaxSize:    0, // Should be clamped to 0
			expectDefaultTTL: 0, // Should be clamped to 0
		},
		{
			name: "partial negative values",
			cfg: CacheConfig{
				Name:            "test",
				MaxSize:         100,
				DefaultTTL:      -time.Minute,
				CleanupInterval: -time.Second,
			},
			expectMaxSize:    100,
			expectDefaultTTL: 0,
		},
		{
			name: "all zero values",
			cfg: CacheConfig{
				Name:            "test",
				MaxSize:         0,
				DefaultTTL:      0,
				CleanupInterval: 0,
			},
			expectMaxSize:    0,
			expectDefaultTTL: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewCache[int](tt.cfg)
			defer cache.Stop()

			// Should handle negative values gracefully by clamping to zero
			cache.Set("key1", 100, 0)
			val := cache.Get("key1")
			if val != 100 {
				t.Errorf("Expected 100, got %d", val)
			}

			// Size should work and not be negative
			size := cache.Size()
			if size < 0 {
				t.Errorf("Size should not be negative, got %d", size)
			}
		})
	}
}

func TestCache_EmptyKey(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Empty string key should be rejected
	cache.Set("", 100, 0)
	val := cache.Get("")
	if val != 0 {
		t.Errorf("Expected 0 for empty key (rejected), got %d", val)
	}

	// Verify it wasn't actually stored
	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after empty key rejection, got %d", cache.Size())
	}

	// Peek should also reject empty keys
	val = cache.Peek("")
	if val != 0 {
		t.Errorf("Expected 0 for empty key from Peek, got %d", val)
	}

	// Delete should handle empty keys gracefully
	cache.Delete("") // Should not panic

	// GetOrSet should reject empty keys and return the provided value
	val = cache.GetOrSet("", 999, 0)
	if val != 999 {
		t.Errorf("Expected 999 from GetOrSet with empty key, got %d", val)
	}
}

func TestCache_UnlimitedSize(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         0, // Unlimited
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Should be able to add many entries without eviction
	for i := 0; i < 1000; i++ {
		cache.Set(fmt.Sprintf("key%d", i), i, 0)
	}

	if cache.Size() != 1000 {
		t.Errorf("Expected size 1000 with unlimited cache, got %d", cache.Size())
	}

	stats := cache.Stats()
	if stats.Evictions != 0 {
		t.Errorf("Expected no evictions with unlimited cache, got %d", stats.Evictions)
	}
}

func TestCache_NoExpiration(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		DefaultTTL:      0, // No expiration
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	cache.Set("key1", 100, 0)

	// Should still be available after delay
	time.Sleep(10 * time.Millisecond)
	val := cache.Get("key1")
	if val != 100 {
		t.Errorf("Expected 100 with no expiration, got %d", val)
	}
}

func TestCache_ConcurrentStop(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 100 * time.Millisecond,
	}

	cache := NewCache[int](cfg)

	// Multiple goroutines calling Stop
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			cache.Stop()
			done <- true
		}()
	}

	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic
}

func TestCache_Peek(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         3, // Small size for eviction test
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Set a value
	cache.Set("key1", 100, 0)

	// Peek should return value without updating LRU or stats
	val := cache.Peek("key1")
	if val != 100 {
		t.Errorf("Expected 100 from Peek, got %d", val)
	}

	// Stats should not be affected by Peek
	stats := cache.Stats()
	if stats.Hits != 0 {
		t.Errorf("Peek should not affect hit count, got %d", stats.Hits)
	}
	if stats.Misses != 0 {
		t.Errorf("Peek should not affect miss count, got %d", stats.Misses)
	}

	// Peek non-existent key
	val = cache.Peek("nonexistent")
	if val != 0 {
		t.Errorf("Expected 0 for non-existent key from Peek, got %d", val)
	}

	// Verify LRU order unchanged by Peek
	cache.Set("key2", 200, 0)
	cache.Set("key3", 300, 0)

	// At this point: key1 (oldest), key2, key3 (newest)
	// Peek key1 (oldest) - should NOT update LRU
	cache.Peek("key1")

	// Add new entry - should evict key1 (oldest) since Peek didn't update it
	cache.Set("key4", 400, 0)

	// key1 should be evicted (was oldest and Peek didn't update LRU)
	if val := cache.Get("key1"); val != 0 {
		t.Errorf("key1 should be evicted after Peek (oldest), got %d", val)
	}
	// key2, key3, key4 should exist
	if val := cache.Get("key2"); val != 200 {
		t.Errorf("key2 should exist, got %d", val)
	}
	if val := cache.Get("key3"); val != 300 {
		t.Errorf("key3 should exist, got %d", val)
	}
	if val := cache.Get("key4"); val != 400 {
		t.Errorf("key4 should exist, got %d", val)
	}
}

func TestCache_GetOrSet(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// First call - key doesn't exist, should set and return
	val := cache.GetOrSet("key1", 100, 0)
	if val != 100 {
		t.Errorf("Expected 100 from first GetOrSet, got %d", val)
	}

	// Verify it was cached
	if cached := cache.Get("key1"); cached != 100 {
		t.Errorf("Expected cached value 100, got %d", cached)
	}

	// Second call - key exists, should return cached value
	val = cache.GetOrSet("key1", 999, 0)
	if val != 100 {
		t.Errorf("Expected cached value 100 from second GetOrSet, got %d", val)
	}

	// Verify it wasn't overwritten
	if cached := cache.Get("key1"); cached != 100 {
		t.Errorf("Expected cached value still 100, got %d", cached)
	}

	// Test with expired entry
	cache.Set("temp", 50, 10*time.Millisecond)
	if !awaitExpiration(cache, "temp", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}

	val = cache.GetOrSet("temp", 999, 0)
	if val != 999 {
		t.Errorf("Expected new value 999 after expiry, got %d", val)
	}
}

func TestCache_ResetStats(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Generate some stats
	cache.Set("key1", 100, 0)
	cache.Get("key1")
	cache.Get("nonexistent")

	stats := cache.Stats()
	if stats.Hits != 1 || stats.Misses != 1 {
		t.Errorf("Expected hits=1 misses=1, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// Reset stats
	cache.ResetStats()

	stats = cache.Stats()
	if stats.Hits != 0 || stats.Misses != 0 || stats.Evictions != 0 {
		t.Errorf("Expected all stats zero after reset, got hits=%d misses=%d evictions=%d",
			stats.Hits, stats.Misses, stats.Evictions)
	}
	if stats.HitRate != 0 {
		t.Errorf("Expected hit rate zero after reset, got %f", stats.HitRate)
	}

	// Verify stats still work after reset
	cache.Get("key1")
	stats = cache.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected hits=1 after reset, got %d", stats.Hits)
	}
	if stats.HitRate != 100.0 {
		t.Errorf("Expected 100%% hit rate after reset, got %f", stats.HitRate)
	}
}

func TestCache_ConcurrentSameKey(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	done := make(chan bool)

	// Multiple goroutines updating the same key
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				cache.Set("key1", id*1000+j, 0)
				cache.Get("key1")
			}
			done <- true
		}(i)
	}

	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic and size should be correct
	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	// Final value should be one of the set values
	val := cache.Get("key1")
	if val < 0 || val > 9999 {
		t.Errorf("Unexpected final value: %d", val)
	}
}

func TestCache_LargeValues(t *testing.T) {
	type LargeStruct struct {
		Data []byte
		Meta map[string]string
	}

	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[LargeStruct](cfg)
	defer cache.Stop()

	// Create a large value
	largeValue := LargeStruct{
		Data: make([]byte, 1024*1024), // 1MB
		Meta: make(map[string]string),
	}
	for i := 0; i < 100; i++ {
		largeValue.Meta[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
	}

	// Set and retrieve large value
	cache.Set("large", largeValue, 0)
	retrieved := cache.Get("large")

	if len(retrieved.Data) != len(largeValue.Data) {
		t.Errorf("Large value data size mismatch: got %d, want %d",
			len(retrieved.Data), len(largeValue.Data))
	}

	if len(retrieved.Meta) != len(largeValue.Meta) {
		t.Errorf("Large value meta size mismatch: got %d, want %d",
			len(retrieved.Meta), len(largeValue.Meta))
	}
}

func TestCache_SpecialCharactersInKey(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Test various special characters
	specialKeys := []string{
		"key-with-dash",
		"key_with_underscore",
		"key.with.dot",
		"key:with:colon",
		"key/with/slash",
		"key with space",
		"key\nwith\nnewline",
		"key\twith\ttab",
		"日本語",
		"🎉emoji",
	}

	for _, key := range specialKeys {
		cache.Set(key, 42, 0)
		val := cache.Get(key)
		if val != 42 {
			t.Errorf("Expected 42 for key %q, got %d", key, val)
		}
	}

	// Verify all were stored
	if cache.Size() != len(specialKeys) {
		t.Errorf("Expected size %d, got %d", len(specialKeys), cache.Size())
	}
}

func TestCache_VeryLongKey(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Create a very long key (10KB) efficiently using strings.Builder
	longKey := make([]byte, 10240)
	for i := range longKey {
		longKey[i] = 'a'
	}

	cache.Set(string(longKey), 123, 0)
	val := cache.Get(string(longKey))
	if val != 123 {
		t.Errorf("Expected 123 for very long key, got %d", val)
	}

	// Verify it counts as one entry
	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}
}

func TestCache_OperationsAfterStop(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 100 * time.Millisecond,
	}

	cache := NewCache[int](cfg)

	// Add some entries
	cache.Set("key1", 100, 0)
	cache.Set("key2", 200, 0)

	// Stop the cache
	cache.Stop()

	// Operations should still work (cleanup goroutine just stops)
	val := cache.Get("key1")
	if val != 100 {
		t.Errorf("Expected Get to work after Stop, got %d", val)
	}

	cache.Set("key3", 300, 0)
	val = cache.Get("key3")
	if val != 300 {
		t.Errorf("Expected Set to work after Stop, got %d", val)
	}

	cache.Delete("key1")
	val = cache.Get("key1")
	if val != 0 {
		t.Errorf("Expected Delete to work after Stop, got %d", val)
	}

	stats := cache.Stats()
	// After Stop: key2 and key3 remain (key1 was deleted)
	if stats.Size != 2 {
		t.Errorf("Expected Stats to work after Stop, got size %d (expected 2)", stats.Size)
	}
}

func TestCache_Name(t *testing.T) {
	cfg := CacheConfig{
		Name:            "my-cache",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	if cache.Name() != "my-cache" {
		t.Errorf("Expected name 'my-cache', got '%s'", cache.Name())
	}
}

func TestCache_Has(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      50 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Non-existent key
	if cache.Has("key1") {
		t.Error("Expected Has to return false for non-existent key")
	}

	// Existing key
	cache.Set("key1", 100, 0)
	if !cache.Has("key1") {
		t.Error("Expected Has to return true for existing key")
	}

	// Expired key
	if !awaitExpiration(cache, "key1", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}
	if cache.Has("key1") {
		t.Error("Expected Has to return false for expired key")
	}

	// Empty key
	if cache.Has("") {
		t.Error("Expected Has to return false for empty key")
	}

	// Verify Has doesn't affect stats
	cache.Set("key2", 200, 0)
	initialStats := cache.Stats()
	cache.Has("key2")
	stats := cache.Stats()
	if stats.Hits != initialStats.Hits || stats.Misses != initialStats.Misses {
		t.Errorf("Has should not affect stats, got hits=%d misses=%d (initial: hits=%d misses=%d)",
			stats.Hits, stats.Misses, initialStats.Hits, initialStats.Misses)
	}
}

func TestCache_Keys(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      50 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Empty cache
	keys := cache.Keys()
	if len(keys) != 0 {
		t.Errorf("Expected empty keys slice, got %d", len(keys))
	}

	// Add entries
	cache.Set("key1", 1, 0)
	cache.Set("key2", 2, 0)
	cache.Set("key3", 3, 0)

	keys = cache.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	// Verify all keys present
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, expected := range []string{"key1", "key2", "key3"} {
		if !keySet[expected] {
			t.Errorf("Expected key %s not found", expected)
		}
	}

	// Wait for expiration
	if !awaitExpiration(cache, "key1", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}

	// Keys should exclude expired
	keys = cache.Keys()
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys after expiration, got %d", len(keys))
	}
}

func TestCache_GetExpiry(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      100 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Non-existent key
	expiry := cache.GetExpiry("nonexistent")
	if !expiry.IsZero() {
		t.Errorf("Expected zero time for non-existent key, got %v", expiry)
	}

	// Empty key
	expiry = cache.GetExpiry("")
	if !expiry.IsZero() {
		t.Errorf("Expected zero time for empty key, got %v", expiry)
	}

	// Key with default TTL
	cache.Set("key1", 100, 0)
	expiry = cache.GetExpiry("key1")
	if expiry.IsZero() {
		t.Error("Expected non-zero expiry for key with default TTL")
	}

	// Verify expiry is approximately correct (within 50ms tolerance)
	expectedExpiry := time.Now().Add(100 * time.Millisecond)
	diff := time.Duration(expiry.Sub(expectedExpiry).Abs())
	if diff > 50*time.Millisecond {
		t.Errorf("Expiry diff too large: %v", diff)
	}

	// Key with custom TTL
	customTTL := 200 * time.Millisecond
	cache.Set("key2", 200, customTTL)
	expiry = cache.GetExpiry("key2")
	if expiry.IsZero() {
		t.Error("Expected non-zero expiry for key with custom TTL")
	}

	// Key with no expiration
	cache.Set("key3", 300, -1) // Negative TTL becomes 0 (no expiration)
	expiry = cache.GetExpiry("key3")
	if !expiry.IsZero() {
		t.Errorf("Expected zero time for key with no expiration, got %v", expiry)
	}

	// Expired key
	cache.Set("key4", 400, 10*time.Millisecond)
	if !awaitExpiration(cache, "key4", 100*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}
	expiry = cache.GetExpiry("key4")
	if !expiry.IsZero() {
		t.Errorf("Expected zero time for expired key, got %v", expiry)
	}
}

func TestCache_GetWithLoader_Basic(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	callCount := 0

	loader := func(ctx context.Context) (int, error) {
		callCount++
		return 42, nil
	}

	// First call - loader should be called
	val, err := cache.GetWithLoader(ctx, "key1", loader, 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
	if callCount != 1 {
		t.Errorf("Expected loader to be called once, got %d", callCount)
	}

	// Second call - loader should NOT be called (cached)
	val, err = cache.GetWithLoader(ctx, "key1", loader, 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
	if callCount != 1 {
		t.Errorf("Expected loader to still be called once, got %d", callCount)
	}
}

func TestCache_GetWithLoader_Error(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	expectedErr := errors.New("load failed")

	loader := func(ctx context.Context) (int, error) {
		return 0, expectedErr
	}

	// Loader returns error - value should not be cached
	val, err := cache.GetWithLoader(ctx, "key1", loader, 0)
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
	if val != 0 {
		t.Errorf("Expected 0 on error, got %d", val)
	}

	// Verify it wasn't cached
	stats := cache.Stats()
	if stats.Size != 0 {
		t.Errorf("Expected cache to be empty after loader error, got size %d", stats.Size)
	}
}

func TestCache_GetWithLoader_ContextCancellation(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	loader := func(ctx context.Context) (int, error) {
		// Check context before doing work
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 42, nil
	}

	// Context is already cancelled
	val, err := cache.GetWithLoader(ctx, "key1", loader, 0)
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}
	if val != 0 {
		t.Errorf("Expected 0 on context cancellation, got %d", val)
	}
}

func TestCache_GetWithLoader_Expiration(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		DefaultTTL:      50 * time.Millisecond,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	callCount := 0

	loader := func(ctx context.Context) (int, error) {
		callCount++
		return 42, nil
	}

	// First call - loader should be called
	val, err := cache.GetWithLoader(ctx, "key1", loader, 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
	if callCount != 1 {
		t.Errorf("Expected loader to be called once, got %d", callCount)
	}

	// Wait for expiry
	if !awaitExpiration(cache, "key1", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}

	// Second call - loader should be called again (entry expired)
	val, err = cache.GetWithLoader(ctx, "key1", loader, 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
	if callCount != 2 {
		t.Errorf("Expected loader to be called twice after expiry, got %d", callCount)
	}
}

func TestCache_GetWithLoader_CustomTTL(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		DefaultTTL:      time.Hour, // Long default TTL
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	callCount := 0

	loader := func(ctx context.Context) (int, error) {
		callCount++
		return 42, nil
	}

	// First call with short custom TTL
	val, err := cache.GetWithLoader(ctx, "key1", loader, 50*time.Millisecond)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
	if callCount != 1 {
		t.Errorf("Expected loader to be called once, got %d", callCount)
	}

	// Wait for custom TTL to expire
	if !awaitExpiration(cache, "key1", 200*time.Millisecond) {
		t.Error("Entry did not expire within timeout")
	}

	// Second call - loader should be called again
	val, err = cache.GetWithLoader(ctx, "key1", loader, 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
	if callCount != 2 {
		t.Errorf("Expected loader to be called twice after custom TTL expiry, got %d", callCount)
	}
}

func TestCache_GetWithLoader_EmptyKey(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         10,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	loader := func(ctx context.Context) (int, error) {
		return 42, nil
	}

	// Empty key should return error
	val, err := cache.GetWithLoader(ctx, "", loader, 0)
	if err == nil {
		t.Error("Expected error for empty key")
	}
	if val != 0 {
		t.Errorf("Expected 0 for empty key error, got %d", val)
	}

	// Verify loader was not called
	stats := cache.Stats()
	if stats.Size != 0 {
		t.Errorf("Expected cache to be empty after empty key rejection, got size %d", stats.Size)
	}
}

func TestCache_GetWithLoader_Concurrent(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         100,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	callCount := 0

	loader := func(ctx context.Context) (int, error) {
		callCount++
		return 42, nil
	}

	done := make(chan bool)

	// Concurrent calls to same key
	for i := 0; i < 10; i++ {
		go func() {
			val, err := cache.GetWithLoader(ctx, "key1", loader, 0)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if val != 42 {
				t.Errorf("Expected 42, got %d", val)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Loader should have been called at least once (may be called multiple times due to race)
	if callCount == 0 {
		t.Error("Expected loader to be called at least once")
	}

	// Verify only one entry in cache
	stats := cache.Stats()
	if stats.Size != 1 {
		t.Errorf("Expected cache size 1, got %d", stats.Size)
	}
}

func TestCache_GetWithLoader_LRUEviction(t *testing.T) {
	cfg := CacheConfig{
		Name:            "test",
		MaxSize:         3,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()

	loader := func(ctx context.Context) (int, error) {
		return 42, nil
	}

	// Fill cache to capacity
	cache.GetWithLoader(ctx, "key1", loader, 0)
	cache.GetWithLoader(ctx, "key2", loader, 0)
	cache.GetWithLoader(ctx, "key3", loader, 0)

	// Access key1 to make it recently used
	cache.Get("key1")

	// Add key4, should evict key2 (least recently used)
	cache.GetWithLoader(ctx, "key4", loader, 0)

	// key1 should still exist
	if val := cache.Get("key1"); val != 42 {
		t.Errorf("Expected key1 to exist, got %d", val)
	}

	// key2 should be evicted
	if val := cache.Get("key2"); val != 0 {
		t.Errorf("Expected key2 to be evicted, got %d", val)
	}

	// key3 and key4 should exist
	if val := cache.Get("key3"); val != 42 {
		t.Errorf("Expected key3 to exist, got %d", val)
	}
	if val := cache.Get("key4"); val != 42 {
		t.Errorf("Expected key4 to exist, got %d", val)
	}
}

func BenchmarkCache_Get(b *testing.B) {
	cfg := CacheConfig{
		Name:            "bench",
		MaxSize:         10000,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		cache.Set(fmt.Sprintf("key%d", i), i, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(fmt.Sprintf("key%d", i%1000))
	}
}

func BenchmarkCache_Set(b *testing.B) {
	cfg := CacheConfig{
		Name:            "bench",
		MaxSize:         10000,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(fmt.Sprintf("key%d", i), i, 0)
	}
}

func BenchmarkCache_Concurrent(b *testing.B) {
	cfg := CacheConfig{
		Name:            "bench",
		MaxSize:         10000,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		cache.Set(fmt.Sprintf("key%d", i), i, 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Get(fmt.Sprintf("key%d", i%1000))
			i++
		}
	})
}

func BenchmarkCache_GetWithLoader_Cached(b *testing.B) {
	cfg := CacheConfig{
		Name:            "bench",
		MaxSize:         10000,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	loader := func(ctx context.Context) (int, error) {
		return 42, nil
	}

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		cache.GetWithLoader(ctx, fmt.Sprintf("key%d", i), loader, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.GetWithLoader(ctx, fmt.Sprintf("key%d", i%1000), loader, 0)
	}
}

func BenchmarkCache_GetWithLoader_Uncached(b *testing.B) {
	cfg := CacheConfig{
		Name:            "bench",
		MaxSize:         10000,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	ctx := context.Background()
	callCount := 0
	loader := func(ctx context.Context) (int, error) {
		callCount++
		return 42, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.GetWithLoader(ctx, fmt.Sprintf("key%d", i), loader, 0)
	}
}

func BenchmarkCache_GetWithLoader_Concurrent(b *testing.B) {
	cfg := CacheConfig{
		Name:            "bench",
		MaxSize:         10000,
		DefaultTTL:      time.Minute,
		CleanupInterval: 0,
	}

	cache := NewCache[int](cfg)
	defer cache.Stop()

	// Pre-populate cache
	ctx := context.Background()
	loader := func(ctx context.Context) (int, error) {
		return 42, nil
	}
	for i := 0; i < 1000; i++ {
		cache.GetWithLoader(ctx, fmt.Sprintf("key%d", i), loader, 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.GetWithLoader(ctx, fmt.Sprintf("key%d", i%1000), loader, 0)
			i++
		}
	})
}
