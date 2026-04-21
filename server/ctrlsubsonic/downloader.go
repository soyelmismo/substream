package ctrlsubsonic

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/tidalproxy"
)

// segmentInfo holds HLS segment URL and duration
type segmentInfo struct {
	url      string
	duration float64 // in seconds
}

// fixMp4Durations traverses the MP4 box hierarchy and writes the correct total
// duration into mvhd and mdhd boxes. For fMP4 streams from Tidal, these boxes
// have duration=0 (unknown), which causes Psysonic to treat EVERY seek as
// "beyond end" and enter a buggy code path that panics with frac >= 1.0.
//
// We also ensure the written duration is never an exact multiple of the
// timescale, preventing a secondary bug where frac == 0.0 triggers the same
// panic via an arithmetic sign error in Psysonic's seek-end adjustment.
//
// Box layouts (version 0):
//
//	[size:4][name:4][ver:1][flags:3][ctime:4][mtime:4][timescale:4][duration:4]
//	offsets from box start: timescale=20, duration=24
//
// Box layouts (version 1):
//
//	[size:4][name:4][ver:1][flags:3][ctime:8][mtime:8][timescale:4][duration:8]
//	offsets from box start: timescale=28, duration=32
func fixMp4Durations(data []byte, totalDurationSecs float64) {
	i := 0
	for i+8 <= len(data) {
		size := int(binary.BigEndian.Uint32(data[i : i+4]))
		if size < 8 {
			break
		}
		if i+size > len(data) {
			break
		}
		name := string(data[i+4 : i+8])

		if name == "moov" || name == "trak" || name == "mdia" {
			i += 8
			continue
		}

		if name == "mvhd" || name == "mdhd" {
			version := data[i+8]
			if version == 0 && i+28 <= len(data) {
				ts := binary.BigEndian.Uint32(data[i+20 : i+24])
				if ts > 0 {
					dur := uint32(totalDurationSecs*float64(ts)) + 1 // +1 ensures frac != 0.0
					binary.BigEndian.PutUint32(data[i+24:i+28], dur)
				}
			} else if version == 1 && i+40 <= len(data) {
				ts := binary.BigEndian.Uint32(data[i+28 : i+32])
				if ts > 0 {
					dur := uint64(totalDurationSecs*float64(ts)) + 1
					binary.BigEndian.PutUint64(data[i+32:i+40], dur)
				}
			}
		}

		i += size
	}
}

// createFakeSidx builds an sidx box using the parsed HLS segments and a fake bitrate
// This makes Android ExoPlayer believe the fragmented MP4 stream is seekable.
func createFakeSidx(segments []segmentInfo, bytesPerSec int64, firstOffset uint32) []byte {
	timescale := uint32(1000000)
	refCount := len(segments)

	// Size = 4 (length) + 4 (name) + 4 (version/flags) + 4 (refID) + 4 (timescale)
	// + 4 (ept) + 4 (firstOffset) + 2 (reserved) + 2 (refCount) = 32 bytes header
	// + refCount * 12 bytes per entry
	boxSize := 32 + (refCount * 12)

	buf := make([]byte, boxSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(boxSize))
	copy(buf[4:8], []byte("sidx"))
	buf[8] = 0                                // version 0
	buf[9], buf[10], buf[11] = 0, 0, 0        // flags
	binary.BigEndian.PutUint32(buf[12:16], 1) // reference_ID
	binary.BigEndian.PutUint32(buf[16:20], timescale)
	binary.BigEndian.PutUint32(buf[20:24], 0)           // earliest_presentation_time
	binary.BigEndian.PutUint32(buf[24:28], firstOffset) // first_offset (relative to anchor)

	// reserved = 0, reference_count
	binary.BigEndian.PutUint16(buf[30:32], uint16(refCount))

	offset := 32
	for _, seg := range segments {
		durFunc := uint32(seg.duration * float64(timescale))
		sizeFunc := uint32(seg.duration * float64(bytesPerSec))

		// reference_type = 0 (1 bit), referenced_size = sizeFunc (31 bits)
		if sizeFunc > 0x7FFFFFFF {
			sizeFunc = 0x7FFFFFFF
		}
		binary.BigEndian.PutUint32(buf[offset:offset+4], sizeFunc)
		binary.BigEndian.PutUint32(buf[offset+4:offset+8], durFunc)
		// starts_with_SAP (1 bit) = 1, SAP_type (3 bits) = 1, SAP_delta_time (28 bits) = 0
		binary.BigEndian.PutUint32(buf[offset+8:offset+12], 0x90000000)

		offset += 12
	}

	return buf
}

// downloadAndStitchHLS fetches an M3U8 manifest and streams all its segments to w
// offsetSeconds allows time-based seeking (skip segments until offset)
// clientName is used to determine if Content-Type fix is needed for clients like supersonic
func (c *Controller) downloadAndStitchHLS(ctx context.Context, manifestURL string, w io.Writer, clientIP string, track *tidalproxy.TidalTrack, offsetSeconds float64, startByte int64, totalBytes int64, clientName string) error {
	// 1. Fetch the manifest
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := c.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("manifest fetch failed: %d", resp.StatusCode)
	}

	// 2. Parse segments with duration info
	var segments []segmentInfo
	var variantURL string
	scanner := bufio.NewScanner(resp.Body)
	// Increase scanner buffer to handle large manifests (default 64KB may be too small)
	const maxCapacity = 512 * 1024 // 512KB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	baseURL := manifestURL
	if lastSlash := strings.LastIndex(baseURL, "/"); lastSlash != -1 {
		baseURL = baseURL[:lastSlash+1]
	}

	isMaster := false
	var currentDuration float64
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-STREAM-INF") {
			isMaster = true
			continue
		}
		// Parse segment duration from #EXTINF tag
		if strings.HasPrefix(line, "#EXTINF:") {
			durStr := strings.TrimPrefix(line, "#EXTINF:")
			durStr = strings.TrimSuffix(durStr, ",")
			fmt.Sscanf(durStr, "%f", &currentDuration)
			continue
		}
		if strings.HasPrefix(line, "#") {
			if strings.HasPrefix(line, "#EXT-X-MAP:URI=") {
				mapURL := strings.Trim(strings.TrimPrefix(line, "#EXT-X-MAP:URI="), "\"")
				if !strings.HasPrefix(mapURL, "http") {
					mapURL = baseURL + mapURL
				}
				// Init segments (fMP4) have no duration in manifest, use 0
				segments = append(segments, segmentInfo{url: mapURL, duration: 0})
			}
			continue
		}

		// URL line
		fullURL := line
		if !strings.HasPrefix(fullURL, "http") {
			fullURL = baseURL + fullURL
		}

		if isMaster {
			variantURL = fullURL
			break
		}
		segments = append(segments, segmentInfo{url: fullURL, duration: currentDuration})
		currentDuration = 0
	}

	// Check for scanner errors (buffer overflow, etc.)
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("manifest parse error: %w", err)
	}

	if isMaster && variantURL != "" {
		log.Printf("[DOWNLOAD] HLS Master detected, following variant: %s", variantURL)
		return c.downloadAndStitchHLS(ctx, variantURL, w, clientIP, track, offsetSeconds, startByte, totalBytes, clientName)
	}

	// Calculate which segment to start from based on time offset
	startIdx := 0
	if offsetSeconds > 0 {
		elapsed := 0.0
		for i, seg := range segments {
			if elapsed+seg.duration > offsetSeconds {
				startIdx = i
				break
			}
			elapsed += seg.duration
		}
		if startIdx > 0 {
			log.Printf("[DOWNLOAD] Time-based seek: offset=%.2fs, skipping %d/%d segments (%.2fs elapsed)",
				offsetSeconds, startIdx, len(segments), elapsed)
		}
	}

	log.Printf("[DOWNLOAD] HLS Media playlist parsed: %d segments found", len(segments))
	if len(segments) > 0 {
		log.Printf("[DOWNLOAD] First segment URL: %s", segments[0].url)
	}
	if len(segments) == 0 {
		return fmt.Errorf("no segments found in manifest")
	}

	// 3. Download and stream segments
	// [ON-THE-FLY STREAMING] Instead of downloading all segments aggressively in parallel,
	// we download on-demand with a small prefetch buffer (2 segments). This dramatically
	// reduces upstream bandwidth usage while still allowing concurrent clients to share
	// segments via the segmentCache.
	flusher, canFlush := w.(http.Flusher)
	tagger := &flacTagger{w: w, track: track, startByte: startByte, totalBytes: totalBytes}

	// [CRITICAL FIX] Download first segment (or init+first media) SYNCHRONOUSLY
	// This ensures we start streaming immediately without waiting for workers
	hasInitSegment := len(segments) > 0 && segments[0].duration == 0
	nextToWrite := startIdx

	if hasInitSegment {
		// fMP4: need init segment first, then start from media segment 1 or startIdx
		firstMediaIdx := 1
		if startIdx > 1 {
			firstMediaIdx = startIdx
		}
		log.Printf("[DOWNLOAD] fMP4 with init segment detected, downloading init + segment %d first", firstMediaIdx)

		// Download init segment (index 0) synchronously (with cache)
		downloadStart := time.Now()
		segmentCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		initData, err := c.getCachedSegment(segmentCtx, segments[0].url, clientIP)
		cancel()
		if err != nil {
			return fmt.Errorf("init segment failed: %w", err)
		}
		initElapsed := time.Since(downloadStart)
		log.Printf("[DOWNLOAD] Init segment fetched in %v (%.2f KB)", initElapsed, float64(len(initData))/1024)

		// Download first media segment synchronously too (with cache)
		segmentCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
		firstData, err := c.getCachedSegment(segmentCtx, segments[firstMediaIdx].url, clientIP)
		cancel()
		if err != nil {
			return fmt.Errorf("first media segment %d failed: %w", firstMediaIdx, err)
		}

		// [CRITICAL FIX] Correct Content-Type for fMP4 streams
		// Tidal FLAC streams are now in fMP4 containers, not raw FLAC
		// Without this fix, client receives audio/flac header but fMP4 data and fails
		if rw, ok := w.(http.ResponseWriter); ok {
			// Check for MP4 signature ("ftyp" at offset 4)
			if len(initData) >= 8 && string(initData[4:8]) == "ftyp" {
				// [MPV FIX] Use audio/x-m4a which MPV/ffmpeg handles better for seeking
				rw.Header().Set("Content-Type", "audio/x-m4a")
				log.Printf("[DOWNLOAD] fMP4 container detected, using Content-Type audio/x-m4a for MPV compatibility")
				// Remove any incorrect Content-Disposition from ServeStream
				if rw.Header().Get("Content-Disposition") != "" && track != nil {
					cleanName := strings.ReplaceAll(fmt.Sprintf("%s - %s", track.Artist.Name, track.Title), "/", "_")
					rw.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", cleanName+".m4a"))
				}

				// Inject fake sidx to force ExoPlayer to show seek bar
				ftypLen := binary.BigEndian.Uint32(initData[0:4])
				if ftypLen > 0 && ftypLen < uint32(len(initData)) && track != nil && track.Duration > 0 && startByte == 0 {
					bps := int64(125000)
					if totalBytes > 0 {
						bps = totalBytes / int64(track.Duration)
					}

					fixMp4Durations(initData, float64(track.Duration))

					firstOffset := uint32(len(initData)) - ftypLen
					fakeSidx := createFakeSidx(segments, bps, firstOffset)

					newInitData := make([]byte, 0, len(initData)+len(fakeSidx))
					newInitData = append(newInitData, initData[:ftypLen]...)
					newInitData = append(newInitData, fakeSidx...)
					newInitData = append(newInitData, initData[ftypLen:]...)

					initData = newInitData
					if totalBytes > 0 {
						totalBytes += int64(len(fakeSidx))
					}
					log.Printf("[DOWNLOAD] Injected simulated sidx index box (%d bytes) to support seeking", len(fakeSidx))
				}
			}

			if totalBytes > 0 {
				rw.Header().Set("Accept-Ranges", "bytes")
			}
			if startByte > 0 && totalBytes > 0 {
				rw.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startByte, totalBytes-1, totalBytes))
				rw.WriteHeader(http.StatusPartialContent)
			}
		}

		// Write init segment first (required for fMP4 decoding)
		if _, err := w.Write(initData); err != nil {
			return err
		}
		// Then write first media segment
		if _, err := w.Write(firstData); err != nil {
			return err
		}
		if canFlush {
			flusher.Flush()
		}
		log.Printf("[DOWNLOAD] First segment sent to client in %v total (%.2f KB)", time.Since(downloadStart), float64(len(firstData))/1024)

		// Set nextToWrite to the segment after the one we just wrote
		nextToWrite = firstMediaIdx + 1
	} else {
		// No init segment - download first media segment synchronously (with cache)
		log.Printf("[DOWNLOAD] No init segment, downloading segment %d first", startIdx)
		downloadStart := time.Now()
		segmentCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		firstData, err := c.getCachedSegment(segmentCtx, segments[startIdx].url, clientIP)
		cancel()
		if err != nil {
			return fmt.Errorf("first segment %d failed: %w", startIdx, err)
		}
		if startByte > 0 {
			if rw, ok := w.(http.ResponseWriter); ok {
				rw.Header().Set("Content-Type", "audio/flac")
				if totalBytes > 0 {
					rw.Header().Set("Accept-Ranges", "bytes")
					rw.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startByte, totalBytes-1, totalBytes))
				}
				rw.WriteHeader(http.StatusPartialContent)
			}
			if _, err := w.Write(firstData); err != nil {
				return err
			}
			log.Printf("[DOWNLOAD] First segment (partial) sent to client in %v", time.Since(downloadStart))
		} else {
			// [FIX] Set Content-Type before writing any data (only for clients that need help)
			if rw, ok := w.(http.ResponseWriter); ok && doesntSupportHLS(clientName) {
				// Detect format from data signature
				contentType := "audio/flac" // default
				if len(firstData) >= 8 {
					// Check for MP4/fMP4 signature ("ftyp" at offset 4)
					if string(firstData[4:8]) == "ftyp" {
						// [MPV FIX] Use audio/x-m4a for better MPV/ffmpeg compatibility
						contentType = "audio/x-m4a"
						log.Printf("[DOWNLOAD] fMP4 container detected in first segment, using Content-Type audio/x-m4a for %s", clientName)
					} else if string(firstData[0:4]) == "fLaC" {
						contentType = "audio/flac"
						log.Printf("[DOWNLOAD] Native FLAC detected in first segment, using Content-Type audio/flac for %s", clientName)
					}
				}
				rw.Header().Set("Content-Type", contentType)
				if totalBytes > 0 {
					rw.Header().Set("Accept-Ranges", "bytes")
				}
			}
			// Try tagging if FLAC, then write
			if err := tagger.process(bytes.NewReader(firstData), c); err != nil {
				log.Printf("[DOWNLOAD] tagging failed, writing raw: %v", err)
				if _, err := w.Write(firstData); err != nil {
					return err
				}
			}
			log.Printf("[DOWNLOAD] First segment sent to client in %v", time.Since(downloadStart))
		}
		if canFlush {
			flusher.Flush()
		}
		nextToWrite = startIdx + 1
	}

	// [ON-THE-FLY STREAMING] Stream remaining segments with controlled prefetch
	// Only prefetch 2 segments ahead to minimize bandwidth while avoiding buffer underrun
	const prefetchAhead = 2

	var wg sync.WaitGroup
	prefetchCtx, prefetchCancel := context.WithCancel(ctx)
	defer prefetchCancel()

	// Prefetch worker: downloads segments (nextToWrite + prefetchAhead) ahead
	wg.Add(1)
	go func() {
		defer wg.Done()
		for idx := nextToWrite; idx < len(segments); idx++ {
			// Check if this segment is already being requested by main loop
			// We prefetch (prefetchAhead) segments ahead
			targetIdx := idx + prefetchAhead
			if targetIdx >= len(segments) {
				continue
			}

			segmentCtx, cancel := context.WithTimeout(prefetchCtx, 30*time.Second)
			data, err := c.getCachedSegment(segmentCtx, segments[targetIdx].url, clientIP)
			cancel()

			if err != nil {
				// Prefetch errors are non-fatal, just log and continue
				log.Printf("[DOWNLOAD] Prefetch failed for segment %d: %v", targetIdx, err)
			} else {
				log.Printf("[DOWNLOAD] Prefetched segment %d (%.2f KB)", targetIdx, float64(len(data))/1024)
			}

			// Check if we should stop prefetching
			select {
			case <-prefetchCtx.Done():
				return
			default:
			}
		}
	}()

	// Main streaming loop: download and write sequentially
	for idx := nextToWrite; idx < len(segments); idx++ {
		// Try cache first
		key := segmentCacheKey(segments[idx].url)
		if cached := c.segmentCache.Get(key); cached != nil {
			log.Printf("[DOWNLOAD] Segment %d from cache (%.2f KB)", idx, float64(len(cached))/1024)
			if _, err := w.Write(cached); err != nil {
				prefetchCancel() // Stop prefetching on client disconnect
				return err
			}
			if canFlush {
				flusher.Flush()
			}
			continue
		}

		// Not in cache - download synchronously
		log.Printf("[DOWNLOAD] Segment %d downloading...", idx)
		segmentCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		data, err := downloadSegment(segmentCtx, c.streamClient, segments[idx].url, clientIP)
		cancel()

		if err != nil {
			prefetchCancel()
			return fmt.Errorf("segment %d failed: %w", idx, err)
		}

		// Store in cache for other clients
		c.segmentCache.Set(key, data, segmentCacheTTL)

		// Write to client
		log.Printf("[DOWNLOAD] Segment %d streaming (%.2f KB)", idx, float64(len(data))/1024)
		if _, err := w.Write(data); err != nil {
			prefetchCancel()
			return err
		}
		if canFlush {
			flusher.Flush()
		}
	}

	prefetchCancel()
	wg.Wait()
	return nil
}

// downloadSegment fetches a single segment with retry logic for transient failures
func downloadSegment(ctx context.Context, client *http.Client, url, clientIP string) ([]byte, error) {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms
			delay := time.Duration(100*(1<<attempt)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		data, err := downloadSegmentOnce(ctx, client, url, clientIP)
		if err == nil {
			return data, nil
		}

		lastErr = err
		// Only retry on transient errors (network, 5xx, timeout)
		errStr := err.Error()
		if !isRetryableError(errStr) {
			break
		}
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// segmentCacheKey generates a unique cache key for an HLS segment URL
// Uses MD5 hash to create a compact, URL-safe key
func segmentCacheKey(url string) string {
	// Simple hash of the URL - segments are unique by their full URL (includes CDN host, path, token)
	hash := md5.Sum([]byte(url))
	return hex.EncodeToString(hash[:])
}

// getCachedSegment attempts to get a segment from cache, or downloads and caches it.
// This enables segment sharing between multiple clients streaming the same track,
// reducing upstream bandwidth and improving seeking performance for late joiners.
func (c *Controller) getCachedSegment(ctx context.Context, url, clientIP string) ([]byte, error) {
	// Generate cache key from URL
	key := segmentCacheKey(url)

	// Try to get from cache first
	if cached := c.segmentCache.Get(key); cached != nil {
		log.Printf("[SEGMENT_CACHE] HIT for %s (%.2f KB)", url[:min(50, len(url))], float64(len(cached))/1024)
		return cached, nil
	}

	// Cache miss - download the segment
	log.Printf("[SEGMENT_CACHE] MISS for %s", url[:min(50, len(url))])
	data, err := downloadSegment(ctx, c.streamClient, url, clientIP)
	if err != nil {
		return nil, err
	}

	// Store in cache for other clients
	c.segmentCache.Set(key, data, segmentCacheTTL)
	log.Printf("[SEGMENT_CACHE] STORED %s (%.2f KB, TTL=%v)", url[:min(50, len(url))], float64(len(data))/1024, segmentCacheTTL)

	return data, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// isRetryableError returns true for transient errors that are worth retrying
func isRetryableError(errStr string) bool {
	lower := strings.ToLower(errStr)
	retryable := []string{
		"timeout", "deadline exceeded", "connection refused",
		"connection reset", "no such host", "temporary",
		"502", "503", "504", "429",
	}
	for _, pattern := range retryable {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

func downloadSegmentOnce(ctx context.Context, client *http.Client, url, clientIP string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// Pre-allocate buffer based on Content-Length if available
	var buf bytes.Buffer
	if cl := resp.ContentLength; cl > 0 && cl < 10*1024*1024 { // Max 10MB per segment
		buf.Grow(int(cl))
	}

	_, err = io.Copy(&buf, resp.Body)
	return buf.Bytes(), err
}

// downloadAndStitchDASH fetches a DASH manifest and streams all segments to w
// offsetSeconds allows time-based seeking (skip segments until offset)
func (c *Controller) downloadAndStitchDASH(ctx context.Context, manifestURL string, w io.Writer, clientIP string, track *tidalproxy.TidalTrack, offsetSeconds float64) error {
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := c.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	content := string(body)
	baseURL := manifestURL
	if lastSlash := strings.LastIndex(baseURL, "/"); lastSlash != -1 {
		baseURL = baseURL[:lastSlash+1]
	}

	var segments []string
	parts := strings.Split(content, "<BaseURL>")
	for _, part := range parts[1:] {
		end := strings.Index(part, "</BaseURL>")
		if end > 0 {
			u := strings.ReplaceAll(part[:end], "&amp;", "&")
			if !strings.HasPrefix(u, "http") {
				u = baseURL + u
			}
			segments = append(segments, u)
		}
	}

	if len(segments) == 0 {
		return fmt.Errorf("no segments found in DASH")
	}

	// Calculate starting segment for time-based seeking
	startIdx := 0
	if offsetSeconds > 0 && track != nil && track.Duration > 0 {
		// Estimate which segment to start from based on track duration
		segmentDuration := float64(track.Duration) / float64(len(segments))
		startIdx = int(offsetSeconds / segmentDuration)
		if startIdx >= len(segments) {
			startIdx = len(segments) - 1
		}
		if startIdx > 0 {
			log.Printf("[DOWNLOAD] DASH time-based seek: offset=%.2fs, starting at segment %d/%d",
				offsetSeconds, startIdx, len(segments))
		}
	}

	log.Printf("[DOWNLOAD] DASH: %d segments found, starting parallel download", len(segments))

	// Parallel download with worker pool (same pattern as HLS)
	const maxConcurrency = 6

	// Get flusher for low-latency streaming if writer supports it
	flusher, canFlush := w.(http.Flusher)

	tagger := &flacTagger{w: w, track: track, startByte: 0, totalBytes: 0}

	type segmentResult struct {
		index int
		data  []byte
		err   error
	}

	results := make(chan segmentResult, maxConcurrency*2)
	var wg sync.WaitGroup

	// Start worker pool - only download segments from startIdx onwards
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// Start from startIdx instead of 0 for seeking support
			firstIdx := startIdx + workerID
			if firstIdx < startIdx {
				firstIdx = startIdx
			}
			for idx := firstIdx; idx < len(segments); idx += maxConcurrency {
				// Per-segment timeout
				segmentCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
				data, err := downloadSegment(segmentCtx, c.streamClient, segments[idx], clientIP)
				cancel()

				// Prevent goroutine leak on context cancellation
				select {
				case results <- segmentResult{idx, data, err}:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Close results channel when all workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and write segments in order (start from startIdx for seeking)
	resultMap := make(map[int][]byte)
	nextToWrite := startIdx
	firstWriteDone := false

	for result := range results {
		if result.err != nil {
			// Fail fast like HLS - don't create corrupted audio files
			return fmt.Errorf("DASH segment %d failed: %w", result.index, result.err)
		}

		resultMap[result.index] = result.data

		// Write all consecutive segments that are ready
		for {
			if data, ok := resultMap[nextToWrite]; ok {
				if !firstWriteDone {
					// First segment written (may not be index 0 if seeking): try tagging if it's FLAC
					firstWriteDone = true
					if err := tagger.process(bytes.NewReader(data), c); err != nil {
						log.Printf("[DOWNLOAD] DASH tagging failed, writing raw: %v", err)
						if _, err := w.Write(data); err != nil {
							return err
						}
					}
				} else {
					if _, err := w.Write(data); err != nil {
						return err
					}
				}
				delete(resultMap, nextToWrite)
				nextToWrite++
				// Flush after each segment for low-latency delivery
				if canFlush {
					flusher.Flush()
				}
			} else {
				break
			}
		}

		// Safety check
		if nextToWrite >= len(segments) {
			break
		}
	}

	log.Printf("[DOWNLOAD] DASH: completed %d/%d segments", nextToWrite, len(segments))
	return nil
}

type flacTagger struct {
	w          io.Writer
	track      *tidalproxy.TidalTrack
	startByte  int64
	totalBytes int64
}

func (t *flacTagger) process(r io.Reader, c *Controller) error {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	rw, isResponse := t.w.(http.ResponseWriter)
	isFlac := string(header) == "fLaC"

	// Resolve the headers dynamically before flushing any bytes
	if isResponse {
		cleanName := strings.ReplaceAll(fmt.Sprintf("%s - %s", t.track.Artist.Name, t.track.Title), "/", "_")
		if isFlac {
			rw.Header().Set("Content-Type", "audio/flac")
			rw.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", cleanName+".flac"))
		} else {
			rw.Header().Set("Content-Type", "audio/mp4")
			rw.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", cleanName+".m4a"))
		}

		if t.totalBytes > 0 {
			rw.Header().Set("Accept-Ranges", "bytes")
		} else {
			rw.Header().Set("Transfer-Encoding", "chunked")
		}

		if t.startByte > 0 && t.totalBytes > 0 {
			rw.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", t.startByte, t.totalBytes-1, t.totalBytes))
			rw.WriteHeader(http.StatusPartialContent)
		}
	}

	if !isFlac {
		// Not a FLAC file
		t.w.Write(header)
		_, err := io.Copy(t.w, r)
		return err
	}

	// We have a FLAC header
	t.w.Write(header)

	var blocks [][]byte

	// Read all original metadata blocks
	for {
		blockHead := make([]byte, 4)
		if _, err := io.ReadFull(r, blockHead); err != nil {
			return err
		}

		last := blockHead[0]&0x80 != 0
		typ := blockHead[0] & 0x7F
		length := int(blockHead[1])<<16 | int(blockHead[2])<<8 | int(blockHead[3])

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}

		// Keep blocks except existing VORBIS_COMMENT (4) and PICTURE (6)
		if typ != 4 && typ != 6 {
			blocks = append(blocks, append(blockHead, data...))
		}

		if last {
			break
		}
	}

	// Inject new blocks
	commentBlock := t.makeVorbisComment()
	pictureBlock := t.makePictureBlock(c)

	blocks = append(blocks, commentBlock)
	if pictureBlock != nil {
		blocks = append(blocks, pictureBlock)
	}

	// Write reconstructed blocks
	for i, b := range blocks {
		if i == len(blocks)-1 {
			b[0] |= 0x80 // Set last block flag
		} else {
			b[0] &= 0x7F // Clear last block flag
		}
		t.w.Write(b)
	}

	// Copy the audio frames
	_, err := io.Copy(t.w, r)
	return err
}

func (t *flacTagger) makeVorbisComment() []byte {
	vendor := "Substream"

	tags := map[string]string{
		"TITLE": t.track.Title,
	}

	if t.track.Artist.Name != "" {
		tags["ARTIST"] = t.track.Artist.Name
	}
	if t.track.Album.Title != "" {
		tags["ALBUM"] = t.track.Album.Title
	}
	if t.track.TrackNumber > 0 {
		tags["TRACKNUMBER"] = fmt.Sprint(t.track.TrackNumber)
	}
	if t.track.VolumeNumber > 0 {
		tags["DISCNUMBER"] = fmt.Sprint(t.track.VolumeNumber)
	}
	if t.track.Album.ReleaseDate != "" {
		tags["DATE"] = t.track.Album.ReleaseDate
	}
	if t.track.ISRC != "" {
		tags["ISRC"] = t.track.ISRC
	}
	if len(t.track.Artists) > 0 {
		tags["ALBUMARTIST"] = t.track.Artists[0].Name
	} else if t.track.Artist.Name != "" {
		tags["ALBUMARTIST"] = t.track.Artist.Name
	}

	buf := new(bytes.Buffer)
	// vendor length (4 bytes le)
	binary.Write(buf, binary.LittleEndian, uint32(len(vendor)))
	buf.WriteString(vendor)

	// list length (4 bytes le)
	binary.Write(buf, binary.LittleEndian, uint32(len(tags)))
	for k, v := range tags {
		entry := k + "=" + v
		binary.Write(buf, binary.LittleEndian, uint32(len(entry)))
		buf.WriteString(entry)
	}

	data := buf.Bytes()
	header := make([]byte, 4)
	header[0] = 4 // type VORBIS_COMMENT
	header[1] = byte(len(data) >> 16)
	header[2] = byte(len(data) >> 8)
	header[3] = byte(len(data))

	return append(header, data...)
}

func (t *flacTagger) makePictureBlock(c *Controller) []byte {
	coverUUID := t.track.Album.Cover
	if coverUUID == "" {
		return nil
	}

	coverURL := c.proxy.GetCoverURL(coverUUID, 640)

	// Use controller's HTTP client with proper timeout instead of default http.Get
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", coverURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := c.httpClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		return nil
	}
	defer resp.Body.Close()
	imgData, _ := io.ReadAll(resp.Body)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(3)) // type: front cover

	mime := "image/jpeg"
	binary.Write(buf, binary.BigEndian, uint32(len(mime)))
	buf.WriteString(mime)

	desc := "Cover"
	binary.Write(buf, binary.BigEndian, uint32(len(desc)))
	buf.WriteString(desc)

	// width, height, depth, colors (dummy values for now)
	binary.Write(buf, binary.BigEndian, uint32(640))
	binary.Write(buf, binary.BigEndian, uint32(640))
	binary.Write(buf, binary.BigEndian, uint32(24))
	binary.Write(buf, binary.BigEndian, uint32(0))

	binary.Write(buf, binary.BigEndian, uint32(len(imgData)))
	buf.Write(imgData)

	data := buf.Bytes()
	header := make([]byte, 4)
	header[0] = 6 // type PICTURE
	header[1] = byte(len(data) >> 16)
	header[2] = byte(len(data) >> 8)
	header[3] = byte(len(data))

	return append(header, data...)
}

// proxyHLSManifest fetches the M3U8 manifest, rewrites relative URLs to absolute,
// and serves it directly to the client as an HLS playlist.
func (c *Controller) proxyHLSManifest(ctx context.Context, manifestURL string, w http.ResponseWriter, clientIP string) error {
	urlPreview := manifestURL
	if len(urlPreview) > 80 {
		urlPreview = urlPreview[:80]
	}
	log.Printf("[HLS-PROXY-START] URL=%s", urlPreview)
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := c.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("manifest fetch failed: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	bodyStr := string(body)
	// Log first 500 chars of manifest for debugging
	previewLen := len(bodyStr)
	if previewLen > 500 {
		previewLen = 500
	}
	log.Printf("[HLS-MANIFEST] URL=%s ContentPreview=%s", manifestURL, bodyStr[:previewLen])

	baseURL := manifestURL
	if lastSlash := strings.LastIndex(baseURL, "/"); lastSlash != -1 {
		baseURL = baseURL[:lastSlash+1]
	}

	var rewritten []string
	for _, line := range strings.Split(bodyStr, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			if strings.HasPrefix(trimmed, "#EXT-X-MAP:URI=") {
				parts := strings.SplitN(trimmed, "\"", 3)
				if len(parts) >= 3 && !strings.HasPrefix(parts[1], "http") {
					parts[1] = baseURL + parts[1]
					line = strings.Join(parts, "\"")
				}
			}
			rewritten = append(rewritten, line)
		} else {
			if !strings.HasPrefix(trimmed, "http") {
				trimmed = baseURL + trimmed
			}
			rewritten = append(rewritten, trimmed)
		}
	}

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Content-Disposition", "inline; filename=\"stream.m3u8\"")
	w.WriteHeader(http.StatusOK)

	_, err = w.Write([]byte(strings.Join(rewritten, "\n")))
	return err
}

func (c *Controller) proxyDASHManifest(ctx context.Context, manifestURL string, w http.ResponseWriter, clientIP string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := c.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("manifest fetch failed: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/dash+xml")
	w.Header().Set("Content-Disposition", "inline; filename=\"stream.mpd\"")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(body)
	return err
}
