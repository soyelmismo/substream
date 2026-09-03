package tidalproxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// streamCacheTTL es el tiempo de vida para el micro-caché de URLs de stream
const streamCacheTTL = 10 * time.Second

// streamCacheEntry almacena una URL con su timestamp
type streamCacheEntry struct {
	URL       string
	Timestamp time.Time
	Quality   string
}

// PoolConfig holds configuration for the proxy pool
type PoolConfig struct {
	HealthInterval time.Duration
	Timeout        time.Duration
	Quality        string // default streaming quality
}

type instance struct {
	url     string
	healthy atomic.Bool
}

// Pool implements TidalProxy with multi-instance failover
type Pool struct {
	instances []*instance
	client    *http.Client
	quality   string
	mu        sync.RWMutex

	// New mirror manager for intelligent selection
	mirrorMgr *MirrorManager

	// Micro-cache for stream URLs (10s TTL) to reduce redundant requests
	streamCache   map[int]streamCacheEntry
	streamCacheMu sync.RWMutex
}

// NewPool creates a proxy pool from a list of hifi-api base URLs
func NewPool(urls []string, cfg PoolConfig) *Pool {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Quality == "" {
		cfg.Quality = "HI_RES_LOSSLESS"
	}
	if cfg.HealthInterval == 0 {
		cfg.HealthInterval = 30 * time.Second
	}

	p := &Pool{
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
		quality:     cfg.Quality,
		streamCache: make(map[int]streamCacheEntry),
	}

	// Initialize mirror manager for intelligent routing
	var mirrorConfigs []MirrorConfig
	for _, u := range urls {
		mirrorConfigs = append(mirrorConfigs, MirrorConfig{
			URL:            strings.TrimSuffix(u, "/"),
			Weight:         100,           // Default weight
			HealthEndpoint: "/info/?id=1", // Lightweight endpoint for health checks
		})
		p.instances = append(p.instances, &instance{url: strings.TrimSuffix(u, "/")})
	}

	p.mirrorMgr = NewMirrorManager(mirrorConfigs, cfg.HealthInterval)
	p.mirrorMgr.Start()
	log.Printf("[POOL] Initialized with %d mirrors using intelligent routing", len(mirrorConfigs))

	return p
}

func (p *Pool) SetInstances(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.instances = nil
	for _, u := range urls {
		p.instances = append(p.instances, &instance{url: strings.TrimSuffix(u, "/")})
	}

	// Update MirrorManager with new mirrors
	if p.mirrorMgr != nil {
		p.mirrorMgr.UpdateMirrors(urls)
	}
}

// GetMirrorManager returns the mirror manager for status inspection
func (p *Pool) GetMirrorManager() *MirrorManager {
	return p.mirrorMgr
}

// ErrorCategory classifies errors to determine appropriate retry/fail behavior
type ErrorCategory int

const (
	ErrorTransient        ErrorCategory = iota // Temporary errors: 502, 503, 504, timeout - definitely retry
	ErrorRateLimited                           // 429 - retry after longer backoff
	ErrorAuthExpired                           // 403 auth/session expired - retry once, then fail
	ErrorTrackUnavailable                      // 403/404 specific to track (region blocked, deleted) - fail fast, don't retry
	ErrorPermanent                             // Other non-retryable errors
)

// classifyError categorizes API errors for smart retry logic
func classifyError(err error) ErrorCategory {
	if err == nil {
		return ErrorTransient // Shouldn't happen, but be safe
	}
	errStr := err.Error()
	lowerErr := strings.ToLower(errStr)

	// Check for specific track unavailability patterns (fail fast, don't retry)
	// These indicate the track is genuinely unavailable, not a proxy issue
	trackUnavailablePatterns := []string{
		"preview",       // Track only available as preview
		"not available", // Explicit "not available" message
		" unavailable",  // Explicit unavailable message
		"region",        // Region-locked content
		"restricted",    // Restricted content
	}

	// "upstream api error" can mean either track unavailable OR proxy issue
	// Check context: if it's a 403 with auth/session keywords, it's auth-related
	// If it's 404, it might be track unavailable
	// Otherwise it's likely a transient proxy error
	for _, pattern := range trackUnavailablePatterns {
		if strings.Contains(lowerErr, pattern) {
			return ErrorTrackUnavailable
		}
	}

	// Rate limited - specific backoff needed
	if strings.Contains(errStr, "429") {
		return ErrorRateLimited
	}

	// Server errors - definitely retry
	if strings.Contains(errStr, "502") ||
		strings.Contains(errStr, "503") ||
		strings.Contains(errStr, "504") {
		return ErrorTransient
	}

	// 404 on trackManifests, track, or album endpoints - resource unavailable
	// These indicate the resource doesn't exist (deleted, region-blocked, etc.)
	if strings.Contains(errStr, "404") {
		// Track endpoint: unavailable
		if strings.Contains(lowerErr, "track") && strings.Contains(lowerErr, "manifest") {
			return ErrorTrackUnavailable
		}
		// Album endpoint: unavailable (album deleted or doesn't exist)
		if strings.Contains(lowerErr, "/album/") {
			return ErrorTrackUnavailable
		}
		return ErrorTransient
	}

	// 403 can be auth expired OR track unavailable - check context
	if strings.Contains(errStr, "403") {
		// If error mentions "auth", "session", "token" - it's auth expired
		authPatterns := []string{"auth", "session", "token", "unauthorized"}
		for _, pattern := range authPatterns {
			if strings.Contains(lowerErr, pattern) {
				return ErrorAuthExpired
			}
		}
		// Otherwise assume track unavailable (region lock, account restriction)
		return ErrorTrackUnavailable
	}

	// 400 with "upstream api error" usually means proxy's Tidal credentials expired
	// Retry with another proxy instead of failing permanently
	if strings.Contains(errStr, "400") && strings.Contains(lowerErr, "upstream api error") {
		return ErrorAuthExpired
	}

	// Network/timeout errors - retry
	if strings.Contains(lowerErr, "timeout") ||
		strings.Contains(lowerErr, "deadline exceeded") ||
		strings.Contains(lowerErr, "connection refused") ||
		strings.Contains(lowerErr, "no such host") ||
		strings.Contains(lowerErr, "connection reset") {
		return ErrorTransient
	}

	// HTML instead of JSON - mirror misconfiguration, retry with another
	if strings.Contains(lowerErr, "html instead of json") {
		return ErrorTransient
	}

	return ErrorPermanent
}

// calculateBackoff returns exponential backoff duration with jitter based on error category
func calculateBackoff(try int, baseDelay time.Duration, errCategory ErrorCategory) time.Duration {
	var delay time.Duration

	switch errCategory {
	case ErrorRateLimited:
		// Rate limit needs longer backoff - start at 2 seconds
		delay = 2 * time.Second * time.Duration(1<<try)
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}
	case ErrorAuthExpired:
		// Auth errors - medium backoff
		delay = 500 * time.Millisecond * time.Duration(1<<try)
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
	case ErrorTransient:
		// Server errors/timeout - standard backoff
		delay = baseDelay * time.Duration(1<<try)
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	default:
		delay = baseDelay
	}

	// Add jitter (0-25%) to prevent thundering herd
	jitter := time.Duration(rand.Int63n(int64(delay) / 4))
	return delay + jitter
}

// doFetchRawWithInstance performs a GET request to a specific proxy instance (by index).
// Used for retry logic when 404 errors are encountered.
// Also tracks results with MirrorManager if available.
// Respects CtxPriority from context for dynamic mirror selection.
func (p *Pool) doFetchRawWithInstance(ctx context.Context, path string, query url.Values, clientIP string, instanceIdx int) ([]byte, error) {
	p.mu.RLock()
	// Select mirror based on priority from context
	var m *Mirror
	var base string

	if instanceIdx == 0 && p.mirrorMgr != nil {
		priority := GetPriorityFromContext(ctx)
		mirrors := p.mirrorMgr.GetMirrorsByPriority(priority)

		if len(mirrors) > 0 {
			m = mirrors[0]
			// Try to find a less loaded one among the top 3
			limit := len(mirrors)
			if limit > 3 {
				limit = 3
			}
			for _, candidate := range mirrors[1:limit] {
				if candidate.activeRequests.Load() < m.activeRequests.Load() {
					m = candidate
				}
			}
			if m != nil {
				base = m.URL
			}
		}

		// Fallback to default selection if priority selection failed
		if base == "" {
			m = p.mirrorMgr.SelectMirror()
			if m != nil {
				base = m.URL
			}
		}
	}

	if base == "" {
		idx := instanceIdx % len(p.instances)
		base = p.instances[idx].url
		if p.mirrorMgr != nil && idx < len(p.mirrorMgr.mirrors) {
			m = p.mirrorMgr.mirrors[idx]
		}
	}
	p.mu.RUnlock()

	// Track active requests for load balancing
	if m != nil {
		m.activeRequests.Add(1)
		defer m.activeRequests.Add(-1)
	}

	u := base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
		req.Header.Set("X-Real-IP", clientIP)
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		if m != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return nil, fmt.Errorf("request %s: %w", path, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		err := fmt.Errorf("upstream %s returned %d: %s", path, resp.StatusCode, string(body))
		if m != nil {
			// [FIX] Only penalize mirror for server errors (5xx) or rate limits (429)
			// 4xx errors mean content unavailable, not mirror failure
			if resp.StatusCode >= 500 || resp.StatusCode == 429 {
				p.mirrorMgr.ReportResult(m, latency, err)
			} else {
				p.mirrorMgr.ReportResult(m, latency, nil)
			}
		}
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		if m != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return nil, err
	}

	// Validate response is JSON, not HTML error page
	if len(body) > 0 && body[0] == '<' {
		htmlPreview := string(body)
		if len(htmlPreview) > 100 {
			htmlPreview = htmlPreview[:100] + "..."
		}
		err := fmt.Errorf("upstream %s returned HTML instead of JSON: %s", path, htmlPreview)
		if m != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return nil, err
	}

	if m != nil {
		p.mirrorMgr.ReportResult(m, latency, nil)
	}

	return body, nil
}

// doFetchRawWithMirror performs a GET request to a specific mirror and decodes JSON response.
// This allows GetStreamURL to use smart mirror selection with exclusion.
func (p *Pool) doFetchRawWithMirror(ctx context.Context, path string, query url.Values, clientIP string, m *Mirror, result interface{}) error {
	if m == nil {
		return fmt.Errorf("no mirror provided")
	}

	// Increment active requests for load balancing
	m.activeRequests.Add(1)
	defer m.activeRequests.Add(-1)

	u := m.URL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
		req.Header.Set("X-Real-IP", clientIP)
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		if p.mirrorMgr != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return fmt.Errorf("request %s: %w", path, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		err := fmt.Errorf("upstream %s returned %d: %s", path, resp.StatusCode, string(body))
		if p.mirrorMgr != nil {
			// [FIX] Only penalize mirror for server errors (5xx) or rate limits (429)
			// 4xx errors mean content unavailable, not mirror failure
			if resp.StatusCode >= 500 || resp.StatusCode == 429 {
				p.mirrorMgr.ReportResult(m, latency, err)
			} else {
				p.mirrorMgr.ReportResult(m, latency, nil)
			}
		}
		return err
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		if p.mirrorMgr != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return fmt.Errorf("read body: %w", err)
	}

	// Try to unmarshal envelope first, then direct
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, result); err == nil {
			if p.mirrorMgr != nil {
				p.mirrorMgr.ReportResult(m, latency, nil)
			}
			return nil
		}
	}

	// Fallback: direct unmarshal
	if err := json.Unmarshal(body, result); err != nil {
		return fmt.Errorf("decode result: %w (body: %s)", err, string(body))
	}

	if p.mirrorMgr != nil {
		p.mirrorMgr.ReportResult(m, latency, nil)
	}
	return nil
}

// apiGet performs a GET to a hifi-api endpoint and decodes JSON from .data field with retries.
// It now accepts an optional clientIP to set X-Forwarded-For for Tidal's IP-locked streaming.
func (p *Pool) apiGet(ctx context.Context, path string, query url.Values, result interface{}, clientIP string) error {
	return p.apiGetWithRetry(ctx, path, query, result, clientIP, 3) // Default 3 retries
}

// apiGetWithRetry attempts the request with multiple proxies on retryable errors.
// This handles Tidal API inconsistency and transient proxy errors.
func (p *Pool) apiGetWithRetry(ctx context.Context, path string, query url.Values, result interface{}, clientIP string, maxRetries int) error {
	var lastErr error
	baseDelay := 100 * time.Millisecond

	for try := 0; try < maxRetries; try++ {
		body, err := p.doFetchRawWithInstance(ctx, path, query, clientIP, try)
		if err != nil {
			lastErr = err
			errCat := classifyError(err)
			if errCat == ErrorTransient || errCat == ErrorRateLimited || errCat == ErrorAuthExpired {
				log.Printf("[TIDAL] apiGet retryable error on try %d/%d for %s: %v", try+1, maxRetries, path, err)
				if try < maxRetries-1 {
					backoff := calculateBackoff(try, baseDelay, errCat)
					select {
					case <-time.After(backoff):
						continue
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				continue
			}
			return err // Non-retryable error, fail fast
		}

		// Envelope handling: many hifi-api endpoints wrap data in a top-level JSON
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}

		if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Data) > 0 {
			if err := json.Unmarshal(envelope.Data, result); err == nil {
				return nil
			}
		}

		// Fallback: try unmarshaling directly into result
		if err := json.Unmarshal(body, result); err != nil {
			return fmt.Errorf("decode result: %w (body: %s)", err, string(body))
		}
		return nil
	}
	return fmt.Errorf("all %d proxies returned error for %s: %v", maxRetries, path, lastErr)
}

// apiGetRaw returns the raw body as bytes with retries.
// It now accepts an optional clientIP to set X-Forwarded-For.
func (p *Pool) apiGetRaw(ctx context.Context, path string, query url.Values, clientIP string) ([]byte, error) {
	return p.apiGetRawWithRetry(ctx, path, query, clientIP, 3)
}

// apiGetRawWithRetry attempts the request with multiple proxies on retryable errors.
func (p *Pool) apiGetRawWithRetry(ctx context.Context, path string, query url.Values, clientIP string, maxRetries int) ([]byte, error) {
	var lastErr error
	baseDelay := 100 * time.Millisecond

	for try := 0; try < maxRetries; try++ {
		body, err := p.doFetchRawWithInstance(ctx, path, query, clientIP, try)
		if err != nil {
			lastErr = err
			errCat := classifyError(err)
			if errCat == ErrorTransient || errCat == ErrorRateLimited || errCat == ErrorAuthExpired {
				log.Printf("[TIDAL] apiGetRaw retryable error on try %d/%d for %s: %v", try+1, maxRetries, path, err)
				if try < maxRetries-1 {
					backoff := calculateBackoff(try, baseDelay, errCat)
					select {
					case <-time.After(backoff):
						continue
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				continue
			}
			return nil, err
		}
		return body, nil
	}
	return nil, fmt.Errorf("all %d proxies returned error for %s: %v", maxRetries, path, lastErr)
}

// =====================================================================
// TidalProxy Interface Implementation
// =====================================================================

func (p *Pool) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	var track TidalTrack
	q := url.Values{"id": {fmt.Sprint(trackID)}}
	if err := p.apiGet(ctx, "/info/", q, &track, ""); err != nil {
		return nil, err
	}
	return &track, nil
}

func (p *Pool) GetCoverUUIDForAlbum(ctx context.Context, albumID int) string {
	if a, err := p.GetAlbumInfo(ctx, albumID); err == nil && a.Cover != "" {
		return a.Cover
	}
	return ""
}

func (p *Pool) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	body, err := p.apiGetRaw(ctx, "/album/", url.Values{"id": {fmt.Sprint(albumID)}}, "")
	if err != nil {
		return nil, err
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode album envelope: %w", err)
	}

	var album TidalAlbum
	if err := json.Unmarshal(envelope.Data, &album); err != nil {
		// fallback to direct decode
		if err2 := json.Unmarshal(body, &album); err2 != nil {
			return nil, fmt.Errorf("decode album data: %w", err)
		}
	}

	// items wrapper
	var raw struct {
		Items []struct {
			Item TidalTrack `json:"item"`
		} `json:"items"`
	}
	if json.Unmarshal(envelope.Data, &raw) == nil && len(raw.Items) > 0 {
		album.Items = make([]TidalTrack, len(raw.Items))
		for i, it := range raw.Items {
			album.Items[i] = it.Item
		}
	}

	return &album, nil
}

// GetAlbumsInfoBatch fetches multiple albums concurrently
// For Pool (base proxy), this just calls GetAlbumInfo for each ID
func (p *Pool) GetAlbumsInfoBatch(ctx context.Context, albumIDs []int) map[int]*TidalAlbum {
	if len(albumIDs) == 0 {
		return nil
	}

	result := make(map[int]*TidalAlbum, len(albumIDs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Limit concurrent API calls

	for _, id := range albumIDs {
		wg.Add(1)
		go func(albumID int) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			a, err := p.GetAlbumInfo(ctx, albumID)
			if err == nil && a != nil {
				mu.Lock()
				result[albumID] = a
				mu.Unlock()
			}
		}(id)
	}

	wg.Wait()
	return result
}

func (p *Pool) GetAlbumMetadata(ctx context.Context, albumID int) (*TidalAlbum, error) {
	// Fallback to GetAlbumInfo if not cached
	return p.GetAlbumInfo(ctx, albumID)
}

func (p *Pool) GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error) {
	body, err := p.apiGetRaw(ctx, "/artist/", url.Values{"id": {fmt.Sprint(artistID)}}, "")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Artist TidalArtist `json:"artist"`
		Cover  *struct {
			Size750 string `json:"750"`
		} `json:"cover"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode artist: %w", err)
	}

	return &TidalArtistDetail{
		Artist: resp.Artist,
		Cover:  resp.Cover,
	}, nil
}

func (p *Pool) GetArtistAlbums(ctx context.Context, artistID int, skipTracks bool) (*TidalArtistPage, error) {
	q := url.Values{"f": {fmt.Sprint(artistID)}}
	if skipTracks {
		q.Set("skip_tracks", "true")
	}

	body, err := p.apiGetRaw(ctx, "/artist/", q, "")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Albums struct {
			Items []TidalAlbum `json:"items"`
		} `json:"albums"`
		Tracks []TidalTrack `json:"tracks"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode artist albums: %w", err)
	}

	// Deduplicate albums natively at the proxy level
	seenAlbums := make(map[string]bool)
	var uniqueItems []TidalAlbum
	for _, item := range resp.Albums.Items {
		// Use lowercased title to catch slight casing differences
		key := fmt.Sprintf("%s|%s", strings.ToLower(strings.TrimSpace(item.Title)), item.ReleaseDate)
		if !seenAlbums[key] {
			seenAlbums[key] = true
			uniqueItems = append(uniqueItems, item)
		}
	}
	resp.Albums.Items = uniqueItems

	return &TidalArtistPage{
		Albums: resp.Albums,
		Tracks: resp.Tracks,
	}, nil
}

func (p *Pool) SearchTracks(ctx context.Context, query string, limit, offset int) ([]TidalTrack, error) {
	q := url.Values{
		"s":      {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}

	log.Printf("[SEARCH:TRACKS] Querying mirrors for: %q (limit=%d, offset=%d)", query, limit, offset)

	// Use independent context with timeout - don't let client cancellation abort the search
	searchCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Gather results from all mirrors
	bodies := p.gatherFromMirrors(searchCtx, q)
	if len(bodies) == 0 {
		return nil, fmt.Errorf("no search results from any mirror")
	}

	// Parse and aggregate results from all mirrors
	var allItems []TidalTrack
	seen := make(map[int]bool) // dedupe by track ID

	for i, body := range bodies {
		var result struct {
			Items []TidalTrack `json:"items"`
		}

		// Try envelope first, then direct
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Data) > 0 {
			if err := json.Unmarshal(envelope.Data, &result); err != nil {
				log.Printf("[SEARCH:TRACKS] Body %d: envelope unmarshal error: %v", i, err)
				continue
			}
		} else if err := json.Unmarshal(body, &result); err != nil {
			log.Printf("[SEARCH:TRACKS] Body %d: direct unmarshal error: %v", i, err)
			continue
		}

		log.Printf("[SEARCH:TRACKS] Body %d: parsed %d tracks", i, len(result.Items))

		for _, item := range result.Items {
			if !seen[item.ID] {
				seen[item.ID] = true
				allItems = append(allItems, item)
			}
		}
	}

	log.Printf("[SEARCH:TRACKS] Total unique tracks found: %d", len(allItems))

	if len(allItems) == 0 {
		return nil, fmt.Errorf("no search results from any mirror")
	}

	// Apply limit
	if limit > 0 && len(allItems) > limit {
		allItems = allItems[:limit]
	}

	return allItems, nil
}

// gatherFromMirrors executes a request against all healthy mirrors in parallel
// and returns all successful response bodies. This is the core "gather shotgun" pattern.
func (p *Pool) gatherFromMirrors(ctx context.Context, query url.Values) [][]byte {
	// Get all healthy mirrors
	var mirrors []*Mirror
	p.mu.RLock()
	if p.mirrorMgr != nil {
		for _, m := range p.mirrorMgr.GetAllMirrors() {
			if m.GetState() == StateHealthy || m.GetState() == StateProbing {
				mirrors = append(mirrors, m)
			}
		}
	}
	p.mu.RUnlock()

	if len(mirrors) == 0 {
		log.Printf("[GATHER] No healthy mirrors available")
		return nil
	}

	// Limit to first 5 mirrors to avoid overload
	if len(mirrors) > 5 {
		mirrors = mirrors[:5]
	}

	// Create search-specific context with timeout
	// Must be longer than HTTP client timeout (10s) so we can receive responses
	searchCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	// Debug: Check context deadline and cancellation
	if deadline, ok := searchCtx.Deadline(); ok {
		log.Printf("[GATHER] Context deadline: %v (from now: %v)", deadline, time.Until(deadline))
	} else {
		log.Printf("[GATHER] No deadline set on context")
	}
	select {
	case <-ctx.Done():
		log.Printf("[GATHER] WARNING: Parent context already canceled before gather started!")
	default:
		log.Printf("[GATHER] Parent context is active")
	}
	select {
	case <-searchCtx.Done():
		log.Printf("[GATHER] WARNING: Search context already canceled before gather started!")
	default:
		log.Printf("[GATHER] Search context is active")
	}

	type mirrorResult struct {
		mirrorURL string
		body      []byte
		err       error
	}

	results := make(chan mirrorResult, len(mirrors))

	// Query all mirrors in parallel
	for _, m := range mirrors {
		go func(mirror *Mirror) {
			body, err := p.searchFromMirror(searchCtx, "/search/", query, mirror)
			results <- mirrorResult{mirrorURL: mirror.URL, body: body, err: err}
		}(m)
	}

	// Collect all successful responses
	var allBodies [][]byte
	successCount := 0
	errorCount := 0
	for i := 0; i < len(mirrors); i++ {
		select {
		case res := <-results:
			if res.err == nil && len(res.body) > 0 {
				allBodies = append(allBodies, res.body)
				successCount++
				// Log preview of response for debugging
				preview := string(res.body)
				if len(preview) > 200 {
					preview = preview[:200] + "..."
				}
				log.Printf("[GATHER] Mirror %s returned %d bytes: %s", res.mirrorURL, len(res.body), preview)
			} else if res.err != nil {
				errorCount++
				log.Printf("[GATHER] Mirror %s error: %v", res.mirrorURL, res.err)
			} else {
				errorCount++
				log.Printf("[GATHER] Mirror %s returned empty body", res.mirrorURL)
			}
		case <-searchCtx.Done():
			// Timeout reached, stop waiting for more
			log.Printf("[GATHER] Timeout reached, got %d/%d responses (success=%d, error=%d)",
				len(allBodies), len(mirrors), successCount, errorCount)
			return allBodies
		}
	}

	log.Printf("[GATHER] Completed: %d mirrors queried, %d success, %d error, %d bodies collected",
		len(mirrors), successCount, errorCount, len(allBodies))
	return allBodies
}

// searchFromMirror performs a raw search request to a specific mirror and returns the body
func (p *Pool) searchFromMirror(ctx context.Context, path string, query url.Values, m *Mirror) ([]byte, error) {
	u := m.URL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	start := time.Now()
	mirrorHost := m.URL
	log.Printf("[SEARCH:MIRROR] %s -> Starting request: %s", mirrorHost, u)

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		log.Printf("[SEARCH:MIRROR] %s -> Request build error: %v (took %v)", mirrorHost, err, time.Since(start))
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

	resp, err := p.client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("[SEARCH:MIRROR] %s -> HTTP error: %v (took %v)", mirrorHost, err, elapsed)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[SEARCH:MIRROR] %s -> Status error: %d (took %v)", mirrorHost, resp.StatusCode, elapsed)
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[SEARCH:MIRROR] %s -> Read body error: %v (took %v)", mirrorHost, err, elapsed)
		return nil, err
	}

	log.Printf("[SEARCH:MIRROR] %s -> Success: %d bytes in %v", mirrorHost, len(body), elapsed)
	return body, nil
}

func (p *Pool) SearchArtists(ctx context.Context, query string, limit, offset int) ([]TidalArtist, error) {
	q := url.Values{
		"a":      {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}

	log.Printf("[SEARCH:ARTISTS] Querying mirrors for: %q (limit=%d, offset=%d)", query, limit, offset)

	// Use independent context with timeout - don't let client cancellation abort the search
	searchCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Gather results from all mirrors
	bodies := p.gatherFromMirrors(searchCtx, q)
	if len(bodies) == 0 {
		return nil, fmt.Errorf("no search results from any mirror")
	}

	// Parse and aggregate results from all mirrors
	var allItems []TidalArtist
	seen := make(map[int]bool) // dedupe by artist ID

	for i, body := range bodies {
		var result struct {
			Artists struct {
				Items []TidalArtist `json:"items"`
			} `json:"artists"`
			Items []TidalArtist `json:"items"`
		}

		// Try envelope first, then direct
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Data) > 0 {
			if err := json.Unmarshal(envelope.Data, &result); err != nil {
				log.Printf("[SEARCH:ARTISTS] Body %d: envelope unmarshal error: %v", i, err)
				continue
			}
		} else if err := json.Unmarshal(body, &result); err != nil {
			log.Printf("[SEARCH:ARTISTS] Body %d: direct unmarshal error: %v", i, err)
			continue
		}

		// Use Artists.Items if available, fallback to flat Items
		items := result.Artists.Items
		if len(items) == 0 {
			items = result.Items
		}

		log.Printf("[SEARCH:ARTISTS] Body %d: parsed %d artists (Artists.Items=%d, flat Items=%d)",
			i, len(items), len(result.Artists.Items), len(result.Items))

		for _, item := range items {
			if !seen[item.ID] {
				seen[item.ID] = true
				allItems = append(allItems, item)
			}
		}
	}

	log.Printf("[SEARCH:ARTISTS] Total unique artists found: %d", len(allItems))

	if len(allItems) == 0 {
		return nil, fmt.Errorf("no search results from any mirror")
	}

	// Apply limit
	if limit > 0 && len(allItems) > limit {
		allItems = allItems[:limit]
	}

	return allItems, nil
}

func (p *Pool) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error) {
	q := url.Values{
		"al":     {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}

	log.Printf("[SEARCH:ALBUMS] Querying mirrors for: %q (limit=%d, offset=%d)", query, limit, offset)

	// Use independent context with timeout - don't let client cancellation abort the search
	searchCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Gather results from all mirrors
	bodies := p.gatherFromMirrors(searchCtx, q)
	if len(bodies) == 0 {
		return nil, fmt.Errorf("no search results from any mirror")
	}

	// Parse and aggregate results from all mirrors
	seenAlbums := make(map[string]bool) // dedupe by title+date (albums can have same name)
	var uniqueItems []TidalAlbum

	for i, body := range bodies {
		var result struct {
			Albums struct {
				Items []TidalAlbum `json:"items"`
			} `json:"albums"`
			Items []TidalAlbum `json:"items"`
		}

		// Try envelope first, then direct
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Data) > 0 {
			if err := json.Unmarshal(envelope.Data, &result); err != nil {
				log.Printf("[SEARCH:ALBUMS] Body %d: envelope unmarshal error: %v", i, err)
				continue
			}
		} else if err := json.Unmarshal(body, &result); err != nil {
			log.Printf("[SEARCH:ALBUMS] Body %d: direct unmarshal error: %v", i, err)
			continue
		}

		// Use Albums.Items if available, fallback to flat Items
		items := result.Albums.Items
		if len(items) == 0 {
			items = result.Items
		}

		log.Printf("[SEARCH:ALBUMS] Body %d: parsed %d albums (Albums.Items=%d, flat Items=%d)",
			i, len(items), len(result.Albums.Items), len(result.Items))

		for _, item := range items {
			key := fmt.Sprintf("%s|%s", strings.ToLower(strings.TrimSpace(item.Title)), item.ReleaseDate)
			if !seenAlbums[key] {
				seenAlbums[key] = true
				uniqueItems = append(uniqueItems, item)
			}
		}
	}

	log.Printf("[SEARCH:ALBUMS] Total unique albums found: %d", len(uniqueItems))

	if len(uniqueItems) == 0 {
		return nil, fmt.Errorf("no search results from any mirror")
	}

	// Apply limit
	if limit > 0 && len(uniqueItems) > limit {
		uniqueItems = uniqueItems[:limit]
	}

	return uniqueItems, nil
}

type streamTask struct {
	Mirror       *Mirror
	ManifestType string
	Quality      string
	Formats      []string
}

type streamResult struct {
	URL          string
	Mirror       *Mirror
	ManifestType string
	Quality      string
	Latency      time.Duration
	Err          error
}

// filterMirrorError perdona al mirror si Tidal rechaza la pista por licencias/tier
func filterMirrorError(err error) error {
	if err == nil {
		return nil
	}
	errStr := strings.ToLower(err.Error())
	// Solo castigar por errores reales de red o rate limit (429, 5xx, timeouts)
	if strings.Contains(errStr, "429") || strings.Contains(errStr, "500") ||
		strings.Contains(errStr, "502") || strings.Contains(errStr, "503") ||
		strings.Contains(errStr, "504") || strings.Contains(errStr, "timeout") {
		return err
	}
	// Perdonar 400, 401, 403, 404, Previews, y context canceled (cuando SHOTGUN cancela peticiones pendientes)
	if strings.Contains(errStr, "context canceled") {
		return nil
	}
	return nil
}

// tryProxyURL attempts a HEAD request to validate the proxied URL
// Returns the working URL (proxied or direct) and true if successful
// Replicates verification headers to match the original request
func tryProxyURL(proxiedURL string, client *http.Client, clientIP string) (string, bool) {
	// If it's already a direct URL (not rewritten to a proxy endpoint), it's immediately valid
	if !strings.Contains(proxiedURL, "proxy-audio") {
		return proxiedURL, true
	}

	// Quick HEAD check to proxy-audio (500ms timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "HEAD", proxiedURL, nil)
	if err != nil {
		return proxiedURL, true // Can't even create request, just return and let it fail later
	}

	// Replicate verification headers (matching what Tidal/monochrome expect)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "identity") // HEAD requests typically don't need compression
	req.Header.Set("Referer", "https://monochrome.tf/")

	// Pass client IP for geo-locking validation (if proxy checks this)
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
		req.Header.Set("X-Real-IP", clientIP)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[PROXY:FALLBACK] Proxy HEAD failed (%v), will try direct Tidal URL", err)
		return "", false
	}
	defer resp.Body.Close()

	// If proxy returns 2xx, it's working
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return proxiedURL, true
	}

	// Proxy error (4xx/5xx), try to extract direct URL
	log.Printf("[PROXY:FALLBACK] Proxy returned %d, will try direct Tidal URL", resp.StatusCode)
	return "", false
}

// extractDirectURL extracts the original Tidal URL from a proxied URL
// Format: https://monochrome.tf/proxy-audio?url=<encoded_url>
func extractDirectURL(proxiedURL string) string {
	if !strings.Contains(proxiedURL, "proxy-audio") {
		return proxiedURL // Not a proxied URL
	}

	u, err := url.Parse(proxiedURL)
	if err != nil {
		return proxiedURL
	}

	directURL := u.Query().Get("url")
	if directURL == "" {
		return proxiedURL
	}

	return directURL
}

func (p *Pool) executeStreamTask(ctx context.Context, task streamTask, trackID int, clientIP string) streamResult {
	start := time.Now()
	res := streamResult{
		Mirror:       task.Mirror,
		ManifestType: task.ManifestType,
		Quality:      task.Quality,
	}

	// [TIMEOUT ESTRICTO] ¡No esperaremos 10 segundos por un zombi! Cortamos a los 3.5s
	reqCtx, cancel := context.WithTimeout(ctx, 3500*time.Millisecond)
	defer cancel()

	/*
		// [TEMP DISABLED] --- RUTA V1: Exclusiva para BTS (Idéntico a Python) ---
		// Desactivado temporalmente para priorizar HLS y evitar dar de baja mirrors HLS-only
		if task.ManifestType == "BTS" {
			qV1 := url.Values{
				"id":      {fmt.Sprint(trackID)},
				"quality": {task.Quality},
			}

			var v1Response struct {
				Data struct {
					TrackID           int    `json:"trackId"`
					AudioQuality      string `json:"audioQuality"`
					ManifestMimeType  string `json:"manifestMimeType"`
					Manifest          string `json:"manifest"`
					AssetPresentation string `json:"assetPresentation"`
				} `json:"data"`
			}

			err := p.doFetchRawWithMirror(reqCtx, "/track/", qV1, clientIP, task.Mirror, &v1Response)
			res.Latency = time.Since(start)
			res.Err = err

			if err != nil {
				log.Printf("[BTS:DEBUG] Mirror %s error for track %d: %v", task.Mirror.URL, trackID, err)
				return res
			}

			if v1Response.Data.AssetPresentation == "PREVIEW" {
				log.Printf("[BTS:DEBUG] Mirror %s returned PREVIEW for track %d", task.Mirror.URL, trackID)
				res.Err = fmt.Errorf("preview track")
				return res
			}

			if v1Response.Data.Manifest == "" {
				log.Printf("[BTS:DEBUG] Mirror %s empty manifest for track %d (quality: %s)", task.Mirror.URL, trackID, task.Quality)
				res.Err = fmt.Errorf("empty manifest")
				return res
			}

			audioURL, parseErr := parseManifestURL(trackID, "V1-BTS", v1Response.Data.ManifestMimeType, v1Response.Data.Manifest)
			if parseErr != nil {
				log.Printf("[BTS:DEBUG] Mirror %s parse error for track %d: %v", task.Mirror.URL, trackID, parseErr)
				res.Err = fmt.Errorf("parse error: %w", parseErr)
				return res
			}
			if audioURL == "" {
				log.Printf("[BTS:DEBUG] Mirror %s empty URL after parse for track %d", task.Mirror.URL, trackID)
				res.Err = fmt.Errorf("empty url after parse")
				return res
			}

			// [PROXY VALIDATION] Check if proxied URL works, fallback to direct if not
			finalURL, ok := tryProxyURL(audioURL, p.client, clientIP)
			if !ok {
				// Fallback to direct Tidal URL
				directURL := extractDirectURL(audioURL)
				log.Printf("[BTS:FALLBACK] Using direct Tidal URL for track %d: %s...", trackID, directURL[:min(50, len(directURL))])
				finalURL = directURL
			} else if finalURL != audioURL {
				// Proxy worked but returned different URL (shouldn't happen with our logic)
				log.Printf("[BTS:PROXY] Proxy validated for track %d", trackID)
			}

			urlPreview := finalURL
			if len(urlPreview) > 80 {
				urlPreview = urlPreview[:80]
			}
			log.Printf("[BTS:DEBUG] Mirror %s SUCCESS for track %d: %s...", task.Mirror.URL, trackID, urlPreview)
			res.URL = finalURL
			return res
		}
	*/

	// --- RUTA V2: Exclusiva para HLS ---
	qV2 := url.Values{
		"id":           {fmt.Sprint(trackID)},
		"manifestType": {task.ManifestType},
	}
	for _, f := range task.Formats {
		qV2.Add("formats", f)
	}

	var v2Response struct {
		Data struct {
			Attributes struct {
				Manifest          string   `json:"manifest"`
				ManifestMimeType  string   `json:"manifestMimeType"`
				URI               string   `json:"uri"`
				Formats           []string `json:"formats"`
				TrackPresentation string   `json:"trackPresentation"`
			} `json:"attributes"`
		} `json:"data"`
	}

	err := p.doFetchRawWithMirror(reqCtx, "/trackManifests/", qV2, clientIP, task.Mirror, &v2Response)
	res.Latency = time.Since(start)
	res.Err = err

	if err != nil {
		return res
	}

	if v2Response.Data.Attributes.TrackPresentation == "PREVIEW" {
		res.Err = fmt.Errorf("preview track")
		return res
	}

	// Extraer URL de un JSON Base64 (A veces V2 lo envía)
	manifestB64 := v2Response.Data.Attributes.Manifest
	if manifestB64 != "" {
		audioURL, parseErr := parseManifestURL(trackID, "V2-"+task.ManifestType, v2Response.Data.Attributes.ManifestMimeType, manifestB64)
		if parseErr == nil && audioURL != "" {
			// [PROXY VALIDATION] Check if proxied URL works, fallback to direct if not
			finalURL, ok := tryProxyURL(audioURL, p.client, clientIP)
			if !ok {
				// Fallback to direct Tidal URL
				directURL := extractDirectURL(audioURL)
				log.Printf("[HLS:FALLBACK] Using direct Tidal URL for track %d", trackID)
				res.URL = directURL
			} else {
				res.URL = finalURL
			}
			return res
		}
	}

	// Extraer de URI directa (Típico de HLS en V2)
	uri := v2Response.Data.Attributes.URI
	if uri != "" {
		res.URL = uri
		return res
	}

	res.Err = fmt.Errorf("no valid hls stream url found in v2 response")
	return res
}

func (p *Pool) GetStreamURL(ctx context.Context, trackID int, requestedQuality string, clientIP string) (string, error) {
	if requestedQuality == "" {
		requestedQuality = p.quality
	}

	// [MICRO-CACHE] Verificar si tenemos una URL reciente para este track
	p.streamCacheMu.RLock()
	if entry, ok := p.streamCache[trackID]; ok {
		if time.Since(entry.Timestamp) < streamCacheTTL {
			// Calidad compatible? Si el cache es igual o mejor calidad, lo usamos
			qualityOrder := map[string]int{"HI_RES_LOSSLESS": 4, "LOSSLESS": 3, "HIGH": 2, "LOW": 1}
			requestedOrder := qualityOrder[requestedQuality]
			cachedOrder := qualityOrder[entry.Quality]
			if cachedOrder >= requestedOrder {
				p.streamCacheMu.RUnlock()
				log.Printf("[CACHE-HIT] Track %d: usando URL cacheada (calidad=%s, age=%v)",
					trackID, entry.Quality, time.Since(entry.Timestamp))
				return entry.URL, nil
			}
		}
	}
	p.streamCacheMu.RUnlock()

	// Obtener TODOS los mirrors sanos sin importar el Tier
	var mirrors []*Mirror
	p.mu.RLock()
	if p.mirrorMgr != nil {
		for _, m := range p.mirrorMgr.GetAllMirrors() {
			if m.GetState() == StateHealthy || m.GetState() == StateProbing {
				mirrors = append(mirrors, m)
			}
		}
	}
	p.mu.RUnlock()

	if len(mirrors) == 0 {
		return "", fmt.Errorf("no healthy mirrors available")
	}

	// Ordenar mirrors por STREAM SCORE (Historial de Códecs + Tiempos de Respuesta)
	// Si un Low Tier falla, un Mid Tier con buen historial lo sobrepasará y será usado.
	for i := 0; i < len(mirrors)-1; i++ {
		for j := i + 1; j < len(mirrors); j++ {
			if mirrors[j].GetStreamScore() > mirrors[i].GetStreamScore() {
				mirrors[i], mirrors[j] = mirrors[j], mirrors[i]
			}
		}
	}

	// [ROUND-ROBIN] Rotar el orden de mirrors basado en trackID para distribuir
	// peticiones concurrentes (gapless playback) entre diferentes mirrors.
	// Esto evita sobrecargar el mismo mirror con múltiples peticiones simultáneas.
	if len(mirrors) > 1 {
		offset := trackID % len(mirrors)
		if offset > 0 {
			// Rotar: [offset:] + [:offset]
			mirrors = append(mirrors[offset:], mirrors[:offset]...)
			log.Printf("[SHOTGUN] 🔄 Round-robin: track %d usando offset %d para distribuir carga", trackID, offset)
		}
	}

	// Generar el arsenal de combinaciones (Calidad + Formato)
	type combo struct {
		mType   string
		quality string
		formats []string
	}
	var combos []combo

	switch requestedQuality {
	case "HI_RES_LOSSLESS":
		// HLS first (primary), BTS as fallback
		combos = append(combos, combo{"HLS", "HI_RES_LOSSLESS", []string{"FLAC_HIRES"}})
		combos = append(combos, combo{"HLS", "LOSSLESS", []string{"FLAC"}})
		combos = append(combos, combo{"BTS", "HI_RES_LOSSLESS", []string{"FLAC_HIRES"}})
		combos = append(combos, combo{"BTS", "LOSSLESS", []string{"FLAC"}})
	case "LOSSLESS":
		// HLS first (primary), BTS as fallback
		combos = append(combos, combo{"HLS", "LOSSLESS", []string{"FLAC"}})
		combos = append(combos, combo{"BTS", "LOSSLESS", []string{"FLAC"}})
	default:
		// HLS first (primary), BTS as fallback
		combos = append(combos, combo{"HLS", requestedQuality, []string{"AACLC", "HEAACV1"}})
		combos = append(combos, combo{"BTS", requestedQuality, []string{"AACLC", "HEAACV1"}})
	}

	// Asignar combinaciones a los proxies disponibles
	var tasks []streamTask
	for i, c := range combos {
		if i >= len(mirrors) {
			break // No saturar si hay menos mirrors que combos
		}
		tasks = append(tasks, streamTask{
			Mirror:       mirrors[i],
			ManifestType: c.mType,
			Quality:      c.quality,
			Formats:      c.formats,
		})
	}

	// Si sobran proxies, duplicamos las peticiones principales (BTS FLAC) para ganar la carrera
	if len(mirrors) > len(combos) {
		for i := len(combos); i < len(mirrors); i++ {
			c := combos[(i-len(combos))%len(combos)] // Repartir en round-robin
			tasks = append(tasks, streamTask{
				Mirror:       mirrors[i],
				ManifestType: c.mType,
				Quality:      c.quality,
				Formats:      c.formats,
			})
		}
	}

	// [FAST-PATH] Intentar el primer task (mejor mirror + mejor combo) con timeout corto (300ms)
	// Esto optimiza para clientes HLS nativos (Supersonic) que tienen timeouts cortos.
	// Si el mejor mirror responde rápido, evitamos el overhead del shotgun hedge completo.
	if len(tasks) > 0 {
		bestTask := tasks[0]
		fastCtx, fastCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer fastCancel()

		start := time.Now()
		res := p.executeStreamTask(fastCtx, bestTask, trackID, clientIP)
		latency := time.Since(start)

		if res.Err == nil && res.URL != "" {
			// Fast-path exitoso! Guardar en caché y retornar inmediatamente
			p.streamCacheMu.Lock()
			p.streamCache[trackID] = streamCacheEntry{
				URL:       res.URL,
				Timestamp: time.Now(),
				Quality:   res.Quality,
			}
			p.streamCacheMu.Unlock()

			if p.mirrorMgr != nil {
				p.mirrorMgr.ReportResult(res.Mirror, latency, nil)
			}
			if res.ManifestType == "BTS" {
				res.Mirror.ReportBTS()
			} else {
				res.Mirror.ReportHLS()
			}

			log.Printf("[FAST-PATH] Track %d: %s (%s) desde %s en %v",
				trackID, res.Quality, res.ManifestType, res.Mirror.URL, latency)
			return res.URL, nil
		}

		// Fast-path falló, continuar con shotgun hedge
		log.Printf("[FAST-PATH] Track %d: fallback a shotgun hedge (%v)", trackID, res.Err)
	}

	// ¡DISPARAR EL ESCOPETAZO CON BATCHING PROGRESIVO!
	// Procesar en batches de 3 para no saturar Tidal (mismo patrón que playlists)
	// [OPTIMIZACIÓN KARMA] Si el mejor mirror tiene alto karma, usamos 2 concurrentes
	// para balance entre confiabilidad y velocidad de fallback (nunca 1, eso es lento).
	defaultBatchSize := 3
	batchSize := defaultBatchSize

	// [OPTIMIZACIÓN] Track de mirrors fallados para no reintentarlos en fallback
	failedMirrorURLs := make(map[string]bool)

	// [NOTA] Los mirrors ya están ordenados por StreamScore (alto karma primero)
	// El hedging a 150ms automáticamente favorece a los rápidos sin bloqueos previos
	// No se necesita lógica especial - la licuadora procesa a todos por igual

	var errorsList []string
	var isUnavailable bool
	var bestResult *streamResult

	// [HEDGING] Canal global de resultados - todos los batches compiten aquí
	type batchOutcome struct {
		result  *streamResult
		errors  []string
		unavail bool
		found   bool
		failed  []string
	}
	resultsChan := make(chan batchOutcome, len(tasks))

	// Contexto para cancelar todas las goroutines cuando encontremos resultado
	hedgeCtx, hedgeCancel := context.WithCancel(ctx)
	defer hedgeCancel()

	// [HEDGING] Lanzar batches solapados: cada 150ms lanzamos siguiente si no hay ganador
	// Un espejo sano responde en <100ms. Si pasan 150ms sin respuesta, están colgados.
	hedgeDelay := 150 * time.Millisecond
	batchNum := 0
	batchCount := 0
	allFailedMirrors := make(map[string]bool)

	// Función para lanzar un batch sin bloquear
	launchBatch := func(batch []streamTask, batchIdx int) {
		actualBatchSize := len(batch)
		log.Printf("[SHOTGUN-HEDGE] Batch %d: Lanzando %d peticiones para track %d",
			batchIdx, actualBatchSize, trackID)

		// Lanzar cada task del batch en su propia goroutine
		for _, t := range batch {
			go func(task streamTask) {
				res := p.executeStreamTask(hedgeCtx, task, trackID, clientIP)

				// Reportar a MirrorManager
				if p.mirrorMgr != nil {
					p.mirrorMgr.ReportResult(res.Mirror, res.Latency, filterMirrorError(res.Err))
				}

				// Reportar métricas de streaming
				if res.Err == nil && res.URL != "" {
					if res.ManifestType == "BTS" {
						res.Mirror.ReportBTS()
					} else {
						res.Mirror.ReportHLS()
					}
				} else if res.Err != nil {
					// Castigo a mirrors que se quedan colgados
					errStr := strings.ToLower(res.Err.Error())
					if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") {
						res.Mirror.ReportStreamFail()
					}
				}

				// Enviar resultado al canal global
				outcome := batchOutcome{
					result: &res,
				}

				if res.Err != nil {
					outcome.errors = []string{fmt.Sprintf("%s (%s/%s): %v",
						res.Mirror.URL, res.ManifestType, res.Quality, res.Err)}
					outcome.failed = []string{res.Mirror.URL}

					errStr := strings.ToLower(res.Err.Error())
					if strings.Contains(errStr, "preview") ||
						strings.Contains(errStr, "region") ||
						strings.Contains(errStr, "restricted") {
						outcome.unavail = true
					}
				} else if res.URL != "" && res.Quality == requestedQuality && res.ManifestType == "HLS" {
					// FAST PATH: Calidad exacta en HLS
					outcome.found = true
				}

				select {
				case resultsChan <- outcome:
				case <-hedgeCtx.Done():
					// Contexto cancelado, no enviar
				}
			}(t)
		}
	}

	// Lanzar primer batch inmediatamente
	if batchNum < len(tasks) {
		end := batchNum + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		launchBatch(tasks[batchNum:end], batchCount+1)
		batchCount++
		batchNum = end
	}

	// Loop de hedging: recolectar resultados y lanzar más batches si es necesario
	hedgeTimer := time.NewTimer(hedgeDelay)
	defer hedgeTimer.Stop()

	for {
		select {
		case outcome := <-resultsChan:
			// Acumular errores y mirrors fallados
			errorsList = append(errorsList, outcome.errors...)
			if outcome.unavail {
				isUnavailable = true
			}
			for _, m := range outcome.failed {
				allFailedMirrors[m] = true
				failedMirrorURLs[m] = true
			}

			// FAST PATH: Calidad exacta encontrada
			if outcome.found && outcome.result != nil {
				hedgeCancel() // Cancelar todas las peticiones pendientes

				formatType := "AAC"
				if strings.Contains(outcome.result.Quality, "LOSSLESS") || strings.Contains(outcome.result.Quality, "FLAC") {
					formatType = "FLAC"
				}
				log.Printf("[SHOTGUN] 🎯 FAST PATH track %d: %s (%s/%s) desde %s en %v",
					trackID, formatType, outcome.result.ManifestType, outcome.result.Quality,
					outcome.result.Mirror.URL, outcome.result.Latency)

				// [MICRO-CACHE] Guardar resultado exitoso
				p.streamCacheMu.Lock()
				p.streamCache[trackID] = streamCacheEntry{
					URL:       outcome.result.URL,
					Timestamp: time.Now(),
					Quality:   outcome.result.Quality,
				}
				p.streamCacheMu.Unlock()

				return outcome.result.URL, nil
			}

			// Actualizar bestResult para fallback
			if outcome.result != nil && outcome.result.URL != "" {
				if bestResult == nil || isBetterResult(outcome.result, bestResult) {
					bestResult = outcome.result
				}
			}

		case <-hedgeTimer.C:
			// [HEDGING] Pasaron 150ms sin ganador. ¿Hay más batches por lanzar?
			if batchNum < len(tasks) {
				// Lanzar siguiente batch inmediatamente (sin lógica de fallback compleja)
				end := batchNum + batchSize
				if end > len(tasks) {
					end = len(tasks)
				}
				launchBatch(tasks[batchNum:end], batchCount+1)
				batchCount++
				batchNum = end

				// Reiniciar timer para siguiente hedging (si quedan más batches)
				if batchNum < len(tasks) {
					hedgeTimer.Reset(hedgeDelay)
				}
			} else {
				// No hay más batches por lanzar, seguir esperando resultados
				hedgeTimer.Reset(50 * time.Millisecond) // Polling ultra-rápido al final
			}

		case <-ctx.Done():
			hedgeCancel()
			return "", ctx.Err()
		}

		// Condición de salida: todos los batches lanzados y tenemos resultado
		if batchNum >= len(tasks) && bestResult != nil {
			break
		}
	}

	// Todos los batches procesados. Retornar el mejor resultado si lo hay.
	if bestResult != nil {
		formatType := "AAC/HLS"
		if strings.Contains(bestResult.Quality, "LOSSLESS") || strings.Contains(bestResult.Quality, "FLAC") {
			formatType = "FLAC"
		}
		log.Printf("[SHOTGUN] 🏆 track %d: %s (%s/%s) desde %s en %v",
			trackID, formatType, bestResult.ManifestType, bestResult.Quality, bestResult.Mirror.URL, bestResult.Latency)

		// [MICRO-CACHE] Guardar resultado exitoso para requests futuros
		p.streamCacheMu.Lock()
		p.streamCache[trackID] = streamCacheEntry{
			URL:       bestResult.URL,
			Timestamp: time.Now(),
			Quality:   bestResult.Quality,
		}
		p.streamCacheMu.Unlock()

		return bestResult.URL, nil
	}

	if isUnavailable {
		log.Printf("[SHOTGUN] 🚫 Track %d no disponible globalmente (Preview/Region Locked)", trackID)
	}

	return "", fmt.Errorf("escopetazo fallido, %d batches procesados. Logs: %s",
		(len(tasks)+batchSize-1)/batchSize, strings.Join(errorsList, " | "))
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// isBetterResult determina si un resultado de stream es mejor que otro
// Prioridad: BTS > HLS, FLAC > AAC
func isBetterResult(new, current *streamResult) bool {
	// 1. Siempre preferir BTS (Progresivo) sobre HLS (mejor para gapless)
	if new.ManifestType == "BTS" && current.ManifestType == "HLS" {
		return true
	}

	// 2. Si ambos son del mismo tipo de manifiesto, preferir mejor calidad
	if new.ManifestType == current.ManifestType {
		// HI_RES > LOSSLESS > HIGH > LOW
		qualityOrder := map[string]int{
			"HI_RES_LOSSLESS": 4,
			"LOSSLESS":        3,
			"HIGH":            2,
			"LOW":             1,
		}
		newOrder := qualityOrder[new.Quality]
		currentOrder := qualityOrder[current.Quality]
		if newOrder > currentOrder {
			return true
		}
	}
	return false
}

func (p *Pool) GetCoverURL(coverUUID string, size int) string {
	if coverUUID == "" {
		return ""
	}
	slug := strings.ReplaceAll(coverUUID, "-", "/")
	return fmt.Sprintf("https://resources.tidal.com/images/%s/%dx%d.jpg", slug, size, size)
}

func (p *Pool) GetCoverByTrackID(ctx context.Context, trackID int) (*TidalCover, error) {
	body, err := p.apiGetRaw(ctx, "/cover/", url.Values{"id": {fmt.Sprint(trackID)}}, "")
	if err != nil {
		return nil, err
	}
	var resp struct {
		Covers []struct {
			URL1280 string `json:"1280"`
			URL640  string `json:"640"`
			URL80   string `json:"80"`
		} `json:"covers"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if len(resp.Covers) == 0 {
		return nil, fmt.Errorf("no covers found")
	}
	return &TidalCover{
		URL1280: resp.Covers[0].URL1280,
		URL640:  resp.Covers[0].URL640,
		URL80:   resp.Covers[0].URL80,
	}, nil
}

func (p *Pool) GetRecommendations(ctx context.Context, trackID int) ([]TidalTrack, error) {
	q := url.Values{
		"id": {fmt.Sprint(trackID)},
	}
	body, err := p.apiGetRaw(ctx, "/recommendations/", q, "")
	if err != nil {
		log.Printf("[TIDAL] GetRecommendations ERROR for track %d: %v", trackID, err)
		return nil, err
	}

	// [Resilience] Support multiple Tidal API response formats (Direct Items or Data Wrapper)
	var resp struct {
		Items []TidalTrack `json:"items"`
		Data  struct {
			Items []struct {
				Track TidalTrack `json:"track"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode recommendations: %w", err)
	}

	var tracks []TidalTrack
	if len(resp.Items) > 0 {
		tracks = resp.Items
	} else if len(resp.Data.Items) > 0 {
		for _, item := range resp.Data.Items {
			if item.Track.ID != 0 {
				tracks = append(tracks, item.Track)
			}
		}
	}

	log.Printf("[TIDAL] GetRecommendations track %d: got %d recommendations", trackID, len(tracks))
	return tracks, nil
}

func (p *Pool) GetTopTracks(ctx context.Context, limit int) ([]TidalTrack, error) {
	// Fallback to searching for popular tracks if no direct top-tracks endpoint
	q := url.Values{
		"s":     {"top"}, // Search for 'top' songs
		"limit": {fmt.Sprint(limit)},
	}
	var result struct {
		Items []TidalTrack `json:"items"`
	}
	// Use /search/ instead of /search/tracks/ to avoid 404 in hifi-api
	if err := p.apiGet(ctx, "/search/", q, &result, ""); err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (p *Pool) GetArtistTopTracks(ctx context.Context, artistID int, limit int) ([]TidalTrack, error) {
	// Our proxy (hifi-api) aggregation returns top tracks in the 'tracks' field.
	q := url.Values{
		"f":           {fmt.Sprint(artistID)},
		"skip_tracks": {"true"},
	}

	var result struct {
		Tracks []TidalTrack `json:"tracks"`
	}

	if err := p.apiGet(ctx, "/artist/", q, &result, ""); err != nil {
		return nil, err
	}

	if len(result.Tracks) > limit && limit > 0 {
		return result.Tracks[:limit], nil
	}
	return result.Tracks, nil
}

// GetArtistAlbumCount returns the number of albums for an artist
// This is used by CachedProxy to cache the count
func (p *Pool) GetArtistAlbumCount(ctx context.Context, artistID int) int {
	page, err := p.GetArtistAlbums(ctx, artistID, true)
	if err != nil || page == nil {
		return 0
	}
	return len(page.Albums.Items)
}

func (p *Pool) GetSimilarArtists(ctx context.Context, artistID int) ([]TidalArtist, error) {
	var result struct {
		Artists []TidalArtist `json:"artists"`
	}
	if err := p.apiGet(ctx, "/artist/similar/", url.Values{"id": {fmt.Sprint(artistID)}}, &result, ""); err != nil {
		return nil, err
	}
	return result.Artists, nil
}

func (p *Pool) GetLyrics(ctx context.Context, trackID int) (*TidalLyrics, error) {
	var resp struct {
		Lyrics TidalLyrics `json:"lyrics"`
	}
	if err := p.apiGet(ctx, "/lyrics/", url.Values{"id": {fmt.Sprint(trackID)}}, &resp, ""); err != nil {
		return nil, err
	}
	return &resp.Lyrics, nil
}

// rewriteAudioURL returns direct Tidal audio URLs (monochrome proxy-audio is defunct)
func rewriteAudioURL(originalURL string) string {
	return originalURL
}

func parseManifestURL(trackID int, version, mimeType, manifest string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(manifest)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(manifest)
		if err != nil {
			decoded = []byte(manifest)
		}
	}

	content := string(decoded)
	contentPreview := content
	if len(contentPreview) > 100 {
		contentPreview = contentPreview[:100] + "..."
	}

	// DASH manifest: look for <BaseURL>
	if strings.Contains(content, "<BaseURL>") {
		start := strings.Index(content, "<BaseURL>") + len("<BaseURL>")
		end := strings.Index(content[start:], "</BaseURL>")
		if end > 0 {
			u := content[start : start+end]
			url := strings.ReplaceAll(u, "&amp;", "&")
			url = rewriteAudioURL(url)
			log.Printf("[TIDAL] parseManifestURL track=%d %s: DASH BaseURL found (proxied): %s...", trackID, version, url[:min(50, len(url))])
			return url, nil
		}
	}

	// Fallback for DASH or other XMLs: find first http URL that isn't a schema
	if strings.HasPrefix(content, "<") || strings.Contains(content, "xml") {
		lines := strings.Split(content, ">")
		for _, line := range lines {
			if strings.Contains(line, "http") && !strings.Contains(line, "w3.org") && !strings.Contains(line, "mpeg.org") {
				start := strings.Index(line, "http")
				end := strings.IndexAny(line[start:], " \"'<>")
				if end > 0 {
					u := line[start : start+end]
					url := strings.ReplaceAll(u, "&amp;", "&")
					url = rewriteAudioURL(url)
					log.Printf("[TIDAL] parseManifestURL track=%d %s: XML fallback found (proxied): %s...", trackID, version, url[:min(50, len(url))])
					return url, nil
				}
				url := strings.ReplaceAll(line[start:], "&amp;", "&")
				url = rewriteAudioURL(url)
				log.Printf("[TIDAL] parseManifestURL track=%d %s: XML fallback (no end, proxied): %s...", trackID, version, url[:min(50, len(url))])
				return url, nil
			}
		}
	}

	// JSON manifest (BTS): find "url" or "urls" array
	if strings.HasPrefix(content, "{") {
		var manifestData struct {
			URL  string   `json:"url"`
			URLs []string `json:"urls"`
		}
		if err := json.Unmarshal(decoded, &manifestData); err == nil {
			if manifestData.URL != "" {
				proxiedURL := rewriteAudioURL(manifestData.URL)
				log.Printf("[TIDAL] parseManifestURL track=%d %s: JSON url found (proxied): %s...", trackID, version, proxiedURL[:min(50, len(proxiedURL))])
				return proxiedURL, nil
			}
			if len(manifestData.URLs) > 0 {
				proxiedURL := rewriteAudioURL(manifestData.URLs[0])
				log.Printf("[TIDAL] parseManifestURL track=%d %s: JSON urls[0] found (proxied): %s...", trackID, version, proxiedURL[:min(50, len(proxiedURL))])
				return proxiedURL, nil
			}
		}
	}

	// M3U8: first line that starts with http (skip proxying for HLS manifests)
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http") {
			log.Printf("[TIDAL] parseManifestURL track=%d %s: M3U8 found (not proxied): %s...", trackID, version, line[:min(50, len(line))])
			return line, nil
		}
	}

	return "", fmt.Errorf("could not extract URL from manifest (type: %s content preview: %.50s)", mimeType, content)
}

// GetPlaylist fetches playlist metadata and items from hifi-api using shotgun approach
// Parallel requests to tier HIGH mirrors with progressive batching, first successful wins.
// Supports pagination.
func (p *Pool) GetPlaylist(ctx context.Context, playlistUUID string) (*TidalPlaylist, error) {
	const maxLimit = 100 // Reduced from 500 - some proxies reject high limit values

	var allTracks []TidalTrack
	var playlistInfo TidalPlaylist
	offset := 0

	for {
		q := url.Values{
			"id":     {playlistUUID},
			"limit":  {fmt.Sprintf("%d", maxLimit)},
			"offset": {fmt.Sprintf("%d", offset)},
		}

		log.Printf("[TIDAL:PLAYLIST] Fetching playlist %s offset=%d limit=%d", playlistUUID, offset, maxLimit)

		// Use shotgun approach with progressive batching (3 mirrors at a time)
		// Background priority: usa los mirrors más lentos/penalizados primero
		body, err := p.shotgunRequest(ctx, "/playlist/", q, PriorityBackground)
		if err != nil {
			// Provide more context for common errors
			errStr := err.Error()
			if strings.Contains(errStr, "400") {
				return nil, fmt.Errorf("playlist unavailable (may be private, deleted, or Tidal API error): %w", err)
			}
			if strings.Contains(errStr, "404") {
				return nil, fmt.Errorf("playlist not found: %w", err)
			}
			if strings.Contains(errStr, "401") || strings.Contains(errStr, "403") {
				return nil, fmt.Errorf("access denied - playlist may be private: %w", err)
			}
			return nil, fmt.Errorf("fetch playlist page at offset %d: %w", offset, err)
		}

		var resp struct {
			Playlist TidalPlaylist `json:"playlist"`
			Items    []struct {
				Item TidalTrack `json:"item"`
			} `json:"items"`
		}

		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode playlist page at offset %d: %w", offset, err)
		}

		// Save playlist info from first page
		if offset == 0 {
			playlistInfo = resp.Playlist
		}

		// Extract tracks from this page
		pageTracks := make([]TidalTrack, 0, len(resp.Items))
		for _, item := range resp.Items {
			pageTracks = append(pageTracks, item.Item)
		}

		allTracks = append(allTracks, pageTracks...)

		// If we got fewer than maxLimit tracks, we've reached the end
		if len(pageTracks) < maxLimit {
			break
		}

		offset += maxLimit
	}

	playlistInfo.Tracks = allTracks
	return &playlistInfo, nil
}

// shotgunRequest fires parallel requests across mirrors with progressive fallback.
// It relies on GetMirrorsByPriority to automatically arrange the absolute best order for the task.
func (p *Pool) shotgunRequest(ctx context.Context, path string, query url.Values, priority TaskPriority) ([]byte, error) {
	const batchSize = 3
	var allErrors []string

	mirrors := p.mirrorMgr.GetMirrorsByPriority(priority)
	if len(mirrors) == 0 {
		return nil, fmt.Errorf("no healthy mirrors available for shotgun")
	}

	// Process mirrors in progressive batches.
	// Because the list is already sorted by priority (Urgent=Best, Background=Worst),
	// we just walk down the array naturally!
	for batchNum := 0; batchNum < len(mirrors); batchNum += batchSize {
		end := batchNum + batchSize
		if end > len(mirrors) {
			end = len(mirrors)
		}
		batch := mirrors[batchNum:end]

		log.Printf("[PLAYLIST:SHOTGUN] Priority %v batch %d/%d: firing %d parallel requests to %s",
			priority, batchNum/batchSize+1, (len(mirrors)+batchSize-1)/batchSize, len(batch), path)

		body, err := p.shotgunWithMirrors(ctx, path, query, batch)
		if err == nil {
			return body, nil
		}

		// This batch failed, add to errors and continue to next batch
		allErrors = append(allErrors, fmt.Sprintf("batch %d: %v", batchNum/batchSize+1, err))

		if end < len(mirrors) {
			select {
			case <-time.After(100 * time.Millisecond):
				// Brief pause between batches
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("all batches failed: %s", strings.Join(allErrors, "; "))
}

// shotgunWithMirrors performs shotgun request with specific mirror list
func (p *Pool) shotgunWithMirrors(ctx context.Context, path string, query url.Values, mirrors []*Mirror) ([]byte, error) {
	if len(mirrors) == 0 {
		return nil, fmt.Errorf("no mirrors available")
	}

	// Shotgun context - cancels all pending requests when first succeeds
	shotgunCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		body []byte
		err  error
		m    *Mirror
	}

	results := make(chan result, len(mirrors))

	// Fire all requests in parallel
	for _, m := range mirrors {
		go func(mirror *Mirror) {
			body, err := p.fetchFromMirror(shotgunCtx, path, query, mirror)
			results <- result{body: body, err: err, m: mirror}
		}(m)
	}

	// Collect results - first successful wins
	var errorsList []string
	for i := 0; i < len(mirrors); i++ {
		select {
		case res := <-results:
			if res.err == nil && len(res.body) > 0 {
				cancel()
				log.Printf("[PLAYLIST:SHOTGUN] Winner: %s", res.m.URL)
				return res.body, nil
			}
			if res.err != nil {
				errorsList = append(errorsList, fmt.Sprintf("%s: %v", res.m.URL, res.err))
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("all %d mirrors failed: %s", len(mirrors), strings.Join(errorsList, "; "))
}

// fetchFromMirror performs a single request to a specific mirror
func (p *Pool) fetchFromMirror(ctx context.Context, path string, query url.Values, m *Mirror) ([]byte, error) {
	// Track active requests
	m.activeRequests.Add(1)
	defer m.activeRequests.Add(-1)

	u := m.URL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	// Debug: log the exact URL being requested (truncate query params for privacy if needed)
	log.Printf("[TIDAL:FETCH] %s -> URL: %s", m.URL, u)

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	start := time.Now()
	resp, err := p.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		log.Printf("[TIDAL:FETCH] %s -> Network error: %v (took %v)", m.URL, err, latency)
		if p.mirrorMgr != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		bodyPreview := string(body)
		if len(bodyPreview) > 200 {
			bodyPreview = bodyPreview[:200] + "..."
		}
		log.Printf("[TIDAL:FETCH] %s -> HTTP %d: %s (took %v)", m.URL, resp.StatusCode, bodyPreview, latency)
		err := fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body))
		if p.mirrorMgr != nil {
			// Don't penalize 4xx errors - they're API/auth issues not mirror issues
			if resp.StatusCode >= 500 || resp.StatusCode == 429 {
				p.mirrorMgr.ReportResult(m, latency, err)
			} else {
				p.mirrorMgr.ReportResult(m, latency, nil)
			}
		}
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		if p.mirrorMgr != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return nil, err
	}

	// Validate JSON
	if len(body) > 0 && body[0] == '<' {
		htmlPreview := string(body)
		if len(htmlPreview) > 100 {
			htmlPreview = htmlPreview[:100] + "..."
		}
		err := fmt.Errorf("returned HTML instead of JSON: %s", htmlPreview)
		if p.mirrorMgr != nil {
			p.mirrorMgr.ReportResult(m, latency, err)
		}
		return nil, err
	}

	if p.mirrorMgr != nil {
		p.mirrorMgr.ReportResult(m, latency, nil)
	}

	return body, nil
}

// ClearAll is a no-op for Pool as it has no internal caches
func (p *Pool) ClearAll() {
	// Pool has no in-memory caches to clear
}

// Stats returns empty stats as Pool has no internal caches
func (p *Pool) Stats() CacheStats {
	return CacheStats{}
}
