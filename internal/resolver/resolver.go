// Package resolver provides concurrent fuzzy matching from text queries to Tidal track IDs.
// It uses an in-memory LRU cache with TTL to minimize API calls to Tidal.
package resolver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/internal/cache"
	"go.senan.xyz/gonic/internal/matchutil"
	"go.senan.xyz/gonic/tidalproxy"
)

// Query represents a text-based track lookup request
type Query struct {
	Artist string
	Title  string
	Album  string // Optional, for better matching precision
}

// Key returns a normalized cache key for the query
func (q Query) Key() string {
	// Normalize for consistent cache keys
	normalize := func(s string) string {
		return strings.ToLower(strings.TrimSpace(s))
	}
	if q.Album != "" {
		return fmt.Sprintf("%s|%s|%s", normalize(q.Artist), normalize(q.Title), normalize(q.Album))
	}
	return fmt.Sprintf("%s|%s", normalize(q.Artist), normalize(q.Title))
}

// Resolver provides concurrent text-to-track resolution with caching
type Resolver struct {
	proxy tidalproxy.TidalProxy
	cache *cache.Cache[int] // Query key -> Tidal track ID
}

// New creates a new Resolver with the given Tidal proxy
func New(proxy tidalproxy.TidalProxy) *Resolver {
	return &Resolver{
		proxy: proxy,
		cache: cache.New[int](cache.Config{
			Name:            "fuzzy-resolver",
			MaxSize:         5000,               // 5k entries ~ 40MB
			DefaultTTL:      72 * time.Hour,     // 3 days - tracks don't change often
			CleanupInterval: 1 * time.Hour,
		}),
	}
}

type resolveResult struct {
	QueryKey string
	TidalID  int
}

// ResolveBatch resolves multiple queries concurrently.
// It returns a map of QueryKey -> TidalID for successfully resolved tracks.
// Resolution is bounded by maxConcurrency to protect Tidal's rate limits.
// The context can be used to set overall timeout (e.g., 2 seconds for similar songs).
func (r *Resolver) ResolveBatch(ctx context.Context, queries []Query) map[string]int {
	if len(queries) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	resultsCh := make(chan resolveResult, len(queries))

	// Semaphore limits concurrent searches to protect rate limits
	const maxConcurrency = 4
	sem := make(chan struct{}, maxConcurrency)

	for _, q := range queries {
		wg.Add(1)
		go func(query Query) {
			defer wg.Done()

			key := query.Key()

			// 1. Fast path: In-memory cache
			if id := r.cache.Get(key); id != 0 {
				resultsCh <- resolveResult{QueryKey: key, TidalID: id}
				return
			}

			// 2. Slow path: Search in Tidal
			// Acquire semaphore slot
			select {
			case sem <- struct{}{}:
				// Acquired slot
			case <-ctx.Done():
				// Parent context cancelled while waiting for slot
				return
			}

			// Release slot when done
			defer func() { <-sem }()

			// Check context cancellation again before making request
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Search Tidal - use MEDIUM tier since this is discovery, not critical streaming
			searchCtx := tidalproxy.WithTier(ctx, tidalproxy.TierMedium)

			// Use artist + title for broader search, or just title if artist empty
			searchQuery := query.Title
			if query.Artist != "" {
				searchQuery = query.Artist + " " + query.Title
			}

			tracks, err := r.proxy.SearchTracks(searchCtx, searchQuery, 5, 0)
			if err != nil || len(tracks) == 0 {
				return
			}

			// 3. Fuzzy match
			bestID := matchutil.FindBest(query.Artist, query.Title, query.Album, tracks)
			if bestID != 0 {
				r.cache.Set(key, bestID, 0) // Use default TTL (72h)
				resultsCh <- resolveResult{QueryKey: key, TidalID: bestID}
			}

		}(q)
	}

	// Close channel when all workers done
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results (will exit when channel closed or context cancelled)
	resolved := make(map[string]int, len(queries))
	for {
		select {
		case res, ok := <-resultsCh:
			if !ok {
				// Channel closed, all workers done
				return resolved
			}
			resolved[res.QueryKey] = res.TidalID
		case <-ctx.Done():
			// Context cancelled - return what we have so far
			return resolved
		}
	}
}

// ResolveSingle resolves a single query. Convenience wrapper around ResolveBatch.
func (r *Resolver) ResolveSingle(ctx context.Context, query Query) (int, error) {
	results := r.ResolveBatch(ctx, []Query{query})
	id, ok := results[query.Key()]
	if !ok {
		return 0, fmt.Errorf("could not resolve: %s - %s", query.Artist, query.Title)
	}
	return id, nil
}

// Stats returns cache statistics for monitoring
func (r *Resolver) Stats() cache.Stats {
	return r.cache.Stats()
}

// Stop gracefully stops the resolver's background cleanup goroutine
func (r *Resolver) Stop() {
	r.cache.Stop()
}
