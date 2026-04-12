package ctrlsubsonic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
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

// downloadAndStitchHLS fetches an M3U8 manifest and streams all its segments to w
// offsetSeconds allows time-based seeking (skip segments until offset)
func (c *Controller) downloadAndStitchHLS(ctx context.Context, manifestURL string, w io.Writer, clientIP string, track *tidalproxy.TidalTrack, offsetSeconds float64) error {
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
		return c.downloadAndStitchHLS(ctx, variantURL, w, clientIP, track, offsetSeconds)
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

	// 3. Download and stream segments concurrently with progressive streaming
	// Use the shared streaming client for connection reuse and optimal pooling
	client := c.streamClient

	const maxConcurrency = 6 // Limit concurrent downloads

	// Get flusher for low-latency streaming if writer supports it
	flusher, canFlush := w.(http.Flusher)
	tagger := &flacTagger{w: w, track: track}

	// Download and stream segments in order with look-ahead buffering
	// This allows us to start streaming immediately while downloading ahead
	type segmentResult struct {
		index int
		data  []byte
		err   error
	}

	results := make(chan segmentResult, maxConcurrency*2) // Buffer for look-ahead
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for idx := workerID; idx < len(segments); idx += maxConcurrency {
				// Per-segment timeout to prevent hanging on slow CDN
				segmentCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
				data, err := downloadSegment(segmentCtx, client, segments[idx].url, clientIP)
				cancel()

				// Use select to prevent goroutine leak if context cancelled
				select {
				case results <- segmentResult{idx, data, err}:
				case <-ctx.Done():
					return // Context cancelled, exit worker
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
	writtenInit := false
	initSegmentData := []byte(nil)

	for result := range results {
		if result.err != nil {
			return fmt.Errorf("segment %d failed: %w", result.index, result.err)
		}

		// Capture init segment (index 0, duration 0) immediately when it arrives
		if result.index == 0 && len(segments) > 0 && segments[0].duration == 0 {
			initSegmentData = result.data
			continue // Don't store in resultMap, we'll write it at the right time
		}

		resultMap[result.index] = result.data

		// Write init segment first if we have it and haven't written it yet
		if initSegmentData != nil && !writtenInit {
			if _, err := w.Write(initSegmentData); err != nil {
				return err
			}
			writtenInit = true
			initSegmentData = nil
		}

		// Write all consecutive segments that are ready
		for {
			if data, ok := resultMap[nextToWrite]; ok {
				if nextToWrite == startIdx && track != nil {
					// First media segment, try tagging if FLAC
					if err := tagger.process(bytes.NewReader(data), c); err != nil {
						log.Printf("[DOWNLOAD] tagging failed, writing raw: %v", err)
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

func (c *Controller) downloadAndStitchDASH(ctx context.Context, manifestURL string, w io.Writer, clientIP string, track *tidalproxy.TidalTrack) error {
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

	log.Printf("[DOWNLOAD] DASH: %d segments found, starting parallel download", len(segments))

	// Parallel download with worker pool (same pattern as HLS)
	const maxConcurrency = 6

	// Get flusher for low-latency streaming if writer supports it
	flusher, canFlush := w.(http.Flusher)

	tagger := &flacTagger{w: w, track: track}

	type segmentResult struct {
		index int
		data  []byte
		err   error
	}

	results := make(chan segmentResult, maxConcurrency*2)
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for idx := workerID; idx < len(segments); idx += maxConcurrency {
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

	// Collect and write segments in order
	resultMap := make(map[int][]byte)
	nextToWrite := 0

	for result := range results {
		if result.err != nil {
			// Fail fast like HLS - don't create corrupted audio files
			return fmt.Errorf("DASH segment %d failed: %w", result.index, result.err)
		}

		resultMap[result.index] = result.data

		// Write all consecutive segments that are ready
		for {
			if data, ok := resultMap[nextToWrite]; ok {
				if nextToWrite == 0 {
					// First segment: try tagging if it's FLAC
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
	w     io.Writer
	track *tidalproxy.TidalTrack
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
		rw.Header().Set("Transfer-Encoding", "chunked")
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
