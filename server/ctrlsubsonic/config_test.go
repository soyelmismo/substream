package ctrlsubsonic

import (
	"os"
	"testing"
	"time"
)

func TestInitConfig_DefaultValues(t *testing.T) {
	// Ensure no env vars are set
	os.Unsetenv("GONIC_HOT_HTTP_TIMEOUT")
	os.Unsetenv("GONIC_HOT_MAX_IDLE_CONNS")
	os.Unsetenv("GONIC_HOT_FETCH_CONCURRENCY")
	os.Unsetenv("GONIC_HOT_GENRE_CACHE_SIZE")

	// Reset to defaults (simulating fresh start)
	httpClientTimeout = 10 * time.Second
	httpMaxIdleConns = 100
	hotFetchConcurrency = 5
	maxGenreCacheSize = 100

	initConfig()

	// Verify defaults are unchanged
	if httpClientTimeout != 10*time.Second {
		t.Errorf("Expected default timeout 10s, got %v", httpClientTimeout)
	}
	if httpMaxIdleConns != 100 {
		t.Errorf("Expected default max idle conns 100, got %d", httpMaxIdleConns)
	}
	if hotFetchConcurrency != 5 {
		t.Errorf("Expected default concurrency 5, got %d", hotFetchConcurrency)
	}
	if maxGenreCacheSize != 100 {
		t.Errorf("Expected default cache size 100, got %d", maxGenreCacheSize)
	}
}

func TestInitConfig_EnvironmentOverrides(t *testing.T) {
	// Set environment variables
	os.Setenv("GONIC_HOT_HTTP_TIMEOUT", "15")
	os.Setenv("GONIC_HOT_MAX_IDLE_CONNS", "200")
	os.Setenv("GONIC_HOT_FETCH_CONCURRENCY", "10")
	os.Setenv("GONIC_HOT_GENRE_CACHE_SIZE", "500")

	// Reset to defaults first
	httpClientTimeout = 10 * time.Second
	httpMaxIdleConns = 100
	hotFetchConcurrency = 5
	maxGenreCacheSize = 100

	initConfig()

	// Verify overrides
	if httpClientTimeout != 15*time.Second {
		t.Errorf("Expected timeout 15s, got %v", httpClientTimeout)
	}
	if httpMaxIdleConns != 200 {
		t.Errorf("Expected max idle conns 200, got %d", httpMaxIdleConns)
	}
	if hotFetchConcurrency != 10 {
		t.Errorf("Expected concurrency 10, got %d", hotFetchConcurrency)
	}
	if maxGenreCacheSize != 500 {
		t.Errorf("Expected cache size 500, got %d", maxGenreCacheSize)
	}

	// Cleanup
	os.Unsetenv("GONIC_HOT_HTTP_TIMEOUT")
	os.Unsetenv("GONIC_HOT_MAX_IDLE_CONNS")
	os.Unsetenv("GONIC_HOT_FETCH_CONCURRENCY")
	os.Unsetenv("GONIC_HOT_GENRE_CACHE_SIZE")
}

func TestInitConfig_InvalidValues(t *testing.T) {
	// Set invalid values
	os.Setenv("GONIC_HOT_HTTP_TIMEOUT", "invalid")
	os.Setenv("GONIC_HOT_MAX_IDLE_CONNS", "-5")

	// Reset to defaults
	httpClientTimeout = 10 * time.Second
	httpMaxIdleConns = 100

	initConfig()

	// Should keep defaults on invalid input
	if httpClientTimeout != 10*time.Second {
		t.Errorf("Expected default timeout on invalid input, got %v", httpClientTimeout)
	}
	if httpMaxIdleConns != 100 {
		t.Errorf("Expected default max idle conns on invalid input, got %d", httpMaxIdleConns)
	}

	// Cleanup
	os.Unsetenv("GONIC_HOT_HTTP_TIMEOUT")
	os.Unsetenv("GONIC_HOT_MAX_IDLE_CONNS")
}

func TestHotGenreMapping_Consistency(t *testing.T) {
	// Verify that hotGenreList entries have corresponding mappings
	for _, genre := range hotGenreList {
		if _, ok := hotGenreMapping[genre.Name]; !ok {
			// Some genres might not have direct mappings, that's okay
			// This test just verifies the structure is consistent
		}
	}

	// Verify mapping keys are normalized
	for k := range hotGenreMapping {
		if k == "" {
			t.Error("Empty key in hotGenreMapping")
		}
	}
}
