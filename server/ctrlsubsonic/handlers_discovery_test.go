package ctrlsubsonic

import (
	"sync"
	"testing"
	"time"
)

func TestGenreCache_BasicOperations(t *testing.T) {
	cache := newGenreCache(3)

	// Test set and get
	cache.set("rock", []int{1, 2, 3}, time.Minute)
	result := cache.get("rock")
	if result == nil || len(result) != 3 {
		t.Errorf("Expected 3 tracks, got %v", result)
	}

	// Test non-existent key
	result = cache.get("nonexistent")
	if result != nil {
		t.Error("Expected nil for non-existent key")
	}
}

func TestGenreCache_Expiration(t *testing.T) {
	cache := newGenreCache(10)

	// Set with very short TTL
	cache.set("jazz", []int{10, 20}, 10*time.Millisecond)

	// Should be available immediately
	result := cache.get("jazz")
	if result == nil || len(result) != 2 {
		t.Errorf("Expected 2 tracks before expiration, got %v", result)
	}

	// Wait for expiration
	time.Sleep(15 * time.Millisecond)

	// Should be expired
	result = cache.get("jazz")
	if result != nil {
		t.Error("Expected nil after expiration")
	}
}

func TestGenreCache_LRUEviction(t *testing.T) {
	cache := newGenreCache(2)

	// Fill cache to capacity
	cache.set("pop", []int{1}, time.Minute)
	cache.set("rock", []int{2}, time.Minute)

	// Access "pop" to make it most recently used
	cache.get("pop")

	// Add third entry - should evict "rock" (least recently used)
	cache.set("jazz", []int{3}, time.Minute)

	// "pop" should still exist
	if cache.get("pop") == nil {
		t.Error("Expected 'pop' to still exist after LRU eviction")
	}

	// "rock" should be evicted
	if cache.get("rock") != nil {
		t.Error("Expected 'rock' to be evicted")
	}

	// "jazz" should exist
	if cache.get("jazz") == nil {
		t.Error("Expected 'jazz' to exist")
	}
}

func TestGenreCache_ConcurrentAccess(t *testing.T) {
	cache := newGenreCache(100)
	var wg sync.WaitGroup
	numGoroutines := 50
	operationsPerGoroutine := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := "genre-" + string(rune(id%10))
				tracks := []int{id*100 + j}
				cache.set(key, tracks, time.Minute)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := "genre-" + string(rune(id%10))
				cache.get(key)
			}
		}(i)
	}

	wg.Wait()
	// If we reach here without deadlock or panic, test passes
}

func TestGenreCache_UpdateExisting(t *testing.T) {
	cache := newGenreCache(10)

	// Set initial value
	cache.set("metal", []int{1, 2}, time.Minute)

	// Update with new value
	cache.set("metal", []int{3, 4, 5}, time.Minute)

	result := cache.get("metal")
	if result == nil || len(result) != 3 {
		t.Errorf("Expected 3 tracks after update, got %v", result)
	}

	if result[0] != 3 || result[1] != 4 || result[2] != 5 {
		t.Errorf("Expected [3,4,5], got %v", result)
	}
}
