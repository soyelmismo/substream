package hls

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// SegmentCache manages in-memory caching of downloaded audio segments with TTL
type SegmentCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	data      []byte
	expiresAt time.Time
}

// NewSegmentCache creates a thread-safe segment cache
func NewSegmentCache() *SegmentCache {
	c := &SegmentCache{
		entries: make(map[string]cacheEntry),
	}
	go c.cleanupLoop()
	return c
}

func (c *SegmentCache) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for k, v := range c.entries {
			if now.After(v.expiresAt) {
				delete(c.entries, k)
			}
		}
		c.mu.Unlock()
	}
}

// Get retrieves cached segment data if present and not expired
func (c *SegmentCache) Get(url string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[url]
	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return entry.data, true
}

// Set stores segment data with the given TTL
func (c *SegmentCache) Set(url string, data []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[url] = cacheEntry{
		data:      data,
		expiresAt: time.Now().Add(ttl),
	}
}

// GlobalSegmentCache is the default shared segment cache
var GlobalSegmentCache = NewSegmentCache()

// Options controls HLS segment prefetching and streaming
type Options struct {
	ClientIP       string
	HTTPClient     *http.Client
	Cache          *SegmentCache
	WorkerCount    int
	PrefetchWindow int
}

// DownloadSegment downloads a single segment with retry logic and exponential backoff
func DownloadSegment(ctx context.Context, client *http.Client, url, clientIP string) ([]byte, error) {
	const maxRetries = 3
	var lastErr error

	if client == nil {
		client = http.DefaultClient
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(100*(1<<attempt)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		if clientIP != "" {
			req.Header.Set("X-Forwarded-For", clientIP)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 404 && resp.StatusCode != 429 {
				return nil, lastErr
			}
			continue
		}

		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		return data, nil
	}

	return nil, fmt.Errorf("download failed after %d retries: %w", maxRetries, lastErr)
}

type segmentResult struct {
	index int
	data  []byte
	err   error
}

// Stitch downloads segments in parallel and streams them in sequential order.
// onInit callback is called once the init segment and first media segment are downloaded,
// allowing the caller to inspect format, set response headers, and replace the destination writer (e.g. for remuxing).
func Stitch(
	ctx context.Context,
	segments []SegmentInfo,
	startIdx int,
	dest io.Writer,
	opts Options,
	onInit func(initData, firstMediaData []byte) (io.Writer, error),
) error {
	if len(segments) == 0 {
		return fmt.Errorf("no segments to stitch")
	}

	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	if opts.Cache == nil {
		opts.Cache = GlobalSegmentCache
	}
	if opts.WorkerCount <= 0 {
		opts.WorkerCount = 4
	}
	if opts.PrefetchWindow <= 0 {
		opts.PrefetchWindow = 8
	}

	hasInit := len(segments) > 0 && segments[0].IsInit
	firstMediaIdx := 0
	if hasInit {
		firstMediaIdx = 1
		if startIdx > 1 {
			firstMediaIdx = startIdx
		}
	} else if startIdx > 0 {
		firstMediaIdx = startIdx
	}

	var targetWriter io.Writer = dest

	// If init segment exists, fetch init + first media segment upfront
	if hasInit {
		initCtx, cancelInit := context.WithTimeout(ctx, 15*time.Second)
		initData, err := getOrDownload(initCtx, opts.HTTPClient, segments[0].URL, opts.ClientIP, opts.Cache)
		cancelInit()
		if err != nil {
			return fmt.Errorf("init segment failed: %w", err)
		}

		firstCtx, cancelFirst := context.WithTimeout(ctx, 15*time.Second)
		firstData, err := getOrDownload(firstCtx, opts.HTTPClient, segments[firstMediaIdx].URL, opts.ClientIP, opts.Cache)
		cancelFirst()
		if err != nil {
			return fmt.Errorf("first media segment failed: %w", err)
		}

		if onInit != nil {
			w, err := onInit(initData, firstData)
			if err != nil {
				return err
			}
			if w != nil {
				targetWriter = w
			}
		}

		// Write init segment and first media segment to targetWriter
		if _, err := targetWriter.Write(initData); err != nil {
			return err
		}
		if _, err := targetWriter.Write(firstData); err != nil {
			return err
		}
	}

	// Stream remaining media segments concurrently with ordered delivery
	nextToWrite := firstMediaIdx + 1
	if !hasInit && firstMediaIdx > 0 {
		nextToWrite = firstMediaIdx
	}

	if nextToWrite >= len(segments) {
		return nil
	}

	results := make(chan segmentResult, opts.WorkerCount*2)
	segmentQueue := make(chan int, opts.PrefetchWindow)

	prefetchCtx, cancelPrefetch := context.WithCancel(ctx)
	defer cancelPrefetch()

	var wg sync.WaitGroup
	for i := 0; i < opts.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range segmentQueue {
				data, err := getOrDownload(prefetchCtx, opts.HTTPClient, segments[idx].URL, opts.ClientIP, opts.Cache)
				select {
				case <-prefetchCtx.Done():
					return
				case results <- segmentResult{index: idx, data: data, err: err}:
				}
			}
		}()
	}

	go func() {
		defer close(segmentQueue)
		for i := nextToWrite; i < len(segments); i++ {
			select {
			case <-prefetchCtx.Done():
				return
			case segmentQueue <- i:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	buffer := make(map[int][]byte)
	for res := range results {
		if res.err != nil {
			return fmt.Errorf("segment %d failed: %w", res.index, res.err)
		}

		buffer[res.index] = res.data

		for {
			data, ready := buffer[nextToWrite]
			if !ready {
				break
			}
			if _, err := targetWriter.Write(data); err != nil {
				return err
			}
			delete(buffer, nextToWrite)
			nextToWrite++
		}

		if nextToWrite >= len(segments) {
			break
		}
	}

	return nil
}

func getOrDownload(ctx context.Context, client *http.Client, url, clientIP string, cache *SegmentCache) ([]byte, error) {
	if cache != nil {
		if data, ok := cache.Get(url); ok {
			return data, nil
		}
	}
	data, err := DownloadSegment(ctx, client, url, clientIP)
	if err != nil {
		return nil, err
	}
	if cache != nil && len(data) > 0 {
		cache.Set(url, data, 2*time.Minute)
	}
	return data, nil
}
