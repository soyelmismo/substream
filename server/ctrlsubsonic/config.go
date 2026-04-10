package ctrlsubsonic

import (
	"os"
	"strconv"
	"time"
)

const (
	// hotMonochromeURL is the base URL for the hot.monochrome.tf API.
	// Provides album and track discovery data for genre browsing and recommendations.
	hotMonochromeURL = "https://hot.monochrome.tf"
)

var (
	// httpClientTimeout is the timeout for HTTP requests to external APIs.
	// Includes connection time, redirects, and reading response body.
	httpClientTimeout = 10 * time.Second

	// httpMaxIdleConns is the maximum number of idle connections the HTTP client
	// maintains across all hosts. Higher values improve performance but increase memory usage.
	httpMaxIdleConns = 100

	// httpMaxIdleConnsPerHost is the maximum number of idle connections the HTTP client
	// maintains per host. Prevents connection hoarding for single hosts.
	httpMaxIdleConnsPerHost = 10

	// httpIdleConnTimeout is the maximum duration an idle connection remains open
	// before closing. Frees resources for active connections.
	httpIdleConnTimeout = 90 * time.Second

	// hotFetchConcurrency is the maximum number of concurrent goroutines for
	// fetching albums from hot.monochrome.tf. Limits rate to avoid overwhelming the API.
	hotFetchConcurrency = 5

	// hotFetchTimeout is the HTTP timeout for requests to hot.monochrome.tf API.
	// Shorter than general timeout to fail fast on unresponsive requests.
	hotFetchTimeout = 5 * time.Second

	// hotFetchMaxAlbums is the maximum number of albums to fetch from hot.monochrome.tf
	// when extracting tracks for a genre. Limits API calls when top_tracks is empty.
	hotFetchMaxAlbums = 10

	// hotFetchMaxTracks is the maximum number of tracks to extract per album from
	// hot.monochrome.tf when building genre track lists. Prevents oversized responses.
	hotFetchMaxTracks = 5

	// hotFallbackThresholdRecent is the minimum number of local albums before falling
	// back to hot.monochrome.tf for recent/frequent album lists. Low threshold ensures
	// discovery even with small libraries.
	hotFallbackThresholdRecent = 5

	// hotFallbackThresholdRandom is the minimum number of local albums before falling
	// back to hot.monochrome.tf for random album lists. Higher threshold for random
	// since users expect more variety from their own library.
	hotFallbackThresholdRandom = 10

	// maxGenreCacheSize is the maximum number of genre track caches to keep in memory.
	// Uses LRU eviction to remove least recently used entries when full.
	maxGenreCacheSize = 100

	// genreCacheTTL is the time-to-live for genre track caches before they expire.
	// Balances freshness with API rate limiting. 30 minutes reduces redundant calls.
	genreCacheTTL = 30 * time.Minute

	// genreCountsCacheTTL is the time-to-live for genre counts cache before it expires.
	// Genre counts change slowly, so 5 minutes is reasonable for freshness vs load.
	genreCountsCacheTTL = 5 * time.Minute

	// genreAlbumCacheTTL is the time-to-live for genre album caches before they expire.
	// Album metadata changes infrequently, so 1 hour balances freshness with API rate limiting.
	genreAlbumCacheTTL = time.Hour

	// searchCacheTTL is the time-to-live for search result caches before they expire.
	// Search results can become stale as library changes, so 5 minutes is reasonable.
	searchCacheTTL = 5 * time.Minute

	// genreFetchTimeout is the timeout for genre track fetching from hot.monochrome.tf.
	// Short timeout ensures quick fallback to local content.
	genreFetchTimeout = 3 * time.Second

	// genreCountFetchConcurrency is the maximum concurrent requests when fetching genre counts.
	// Limits rate to avoid overwhelming the API.
	genreCountFetchConcurrency = 5

	// genreFetchMaxCount is the maximum number of tracks to return per genre request.
	// Prevents oversized responses.
	genreFetchMaxCount = 50

	// similarSongsMaxCount is the maximum number of similar songs to return.
	// Limited to reduce cover loading time and improve response latency.
	similarSongsMaxCount = 10

	// similarSongsTimeout is the timeout for fetching similar song recommendations.
	// Fast timeout ensures quick fallback to random songs.
	similarSongsTimeout = 2 * time.Second

	// topSongsSearchCandidates is the number of artist candidates to search when finding exact match.
	// Higher value increases chance of finding correct artist with fuzzy search.
	topSongsSearchCandidates = 10

	// randomSongsDiscoveryExtra is the number of extra tracks to fetch from top tracks for shuffling.
	// Fetching extra allows better randomization in the final selection.
	randomSongsDiscoveryExtra = 10
)

// initConfig loads configuration from environment variables, allowing runtime
// overrides without recompilation. Variables are prefixed with GONIC_HOT_ for clarity.
func initConfig() {
	if v := os.Getenv("GONIC_HOT_HTTP_TIMEOUT"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			httpClientTimeout = time.Duration(secs) * time.Second
		}
	}
	if v := os.Getenv("GONIC_HOT_MAX_IDLE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			httpMaxIdleConns = n
		}
	}
	if v := os.Getenv("GONIC_HOT_MAX_IDLE_CONNS_PER_HOST"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			httpMaxIdleConnsPerHost = n
		}
	}
	if v := os.Getenv("GONIC_HOT_FETCH_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			hotFetchConcurrency = n
		}
	}
	if v := os.Getenv("GONIC_HOT_FETCH_TIMEOUT"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			hotFetchTimeout = time.Duration(secs) * time.Second
		}
	}
	if v := os.Getenv("GONIC_HOT_GENRE_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxGenreCacheSize = n
		}
	}
	if v := os.Getenv("GONIC_HOT_GENRE_CACHE_TTL"); v != "" {
		if mins, err := strconv.Atoi(v); err == nil && mins > 0 {
			genreCacheTTL = time.Duration(mins) * time.Minute
		}
	}
}

// hotGenreMapping maps Subsonic genre names to hot.monochrome.tf genre IDs
// IDs match monochrome client's genre IDs
var hotGenreMapping = map[string]string{
	"Pop":               "pop",
	"Rock":              "rock",
	"Hip-Hop":           "hip_hop",
	"R&B":               "rnb",
	"R&B / Soul":        "rnb",
	"Electronic":        "electronic",
	"Classical":         "classical",
	"Jazz":              "jazz",
	"Blues":             "blues",
	"Country":           "country",
	"Metal":             "metal",
	"Reggae":            "reggae",
	"Reggae / Dancehall": "reggae",
	"Latin":             "latin",
	"World":             "world",
	"Global":            "world",
	"Kids":              "kids",
	"Gospel":            "gospel",
	"Gospel / Christian": "gospel",
	"Dance & Electronic": "dance_electronic",
	"Indie Rock":        "indierock",
	"Rock / Indie":      "indierock",
	"Folk / Americana":  "americana",
	"Americana":         "americana",
	"K-Pop":             "kpop",
	"Legacy":            "retro",
	"Retro":             "retro",
	"Alternative":       "alternative",
	"Dance":            "dance",
	"Soul":             "soul",
	"All":              "pop",
}

// hotGenreList is the canonical list of genres exposed to clients.
// Each entry maps a hot.monochrome.tf ID to a display name.
var hotGenreList = []struct {
	ID   string
	Name string
}{
	{ID: "hip_hop", Name: "Hip-Hop"},
	{ID: "rnb", Name: "R&B / Soul"},
	{ID: "blues", Name: "Blues"},
	{ID: "classical", Name: "Classical"},
	{ID: "country", Name: "Country"},
	{ID: "dance_electronic", Name: "Dance & Electronic"},
	{ID: "americana", Name: "Folk / Americana"},
	{ID: "world", Name: "Global"},
	{ID: "gospel", Name: "Gospel / Christian"},
	{ID: "jazz", Name: "Jazz"},
	{ID: "kpop", Name: "K-Pop"},
	{ID: "kids", Name: "Kids"},
	{ID: "latin", Name: "Latin"},
	{ID: "metal", Name: "Metal"},
	{ID: "pop", Name: "Pop"},
	{ID: "reggae", Name: "Reggae / Dancehall"},
	{ID: "retro", Name: "Legacy"},
	{ID: "indierock", Name: "Rock / Indie"},
}
