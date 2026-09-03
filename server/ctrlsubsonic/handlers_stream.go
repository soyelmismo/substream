package ctrlsubsonic

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// doesntSupportHLS returns true for clients that cannot natively decode HLS manifests.
// These clients require the server to stitch segments into a continuous raw audio stream.
// The list can be pruned as developers add proper HLS support to their clients.
func doesntSupportHLS(clientName string) bool {
	lower := strings.ToLower(clientName)
	switch lower {
	case "psysonic", "tempus", "symfonium":
		// These clients don't properly handle HLS manifests yet
		// Forcing HLS stitching provides continuous audio stream with proper gapless support
		return true
	// [NOTE] supersonic removed - it handles HLS natively via MPV
	default:
		return false
	}
}

// isClientDisconnectError returns true when the error is caused by the client
// closing the connection (not an upstream/server error). These should NOT be
// propagated to deduplicated requests.
func isClientDisconnectError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	clientErrors := []string{
		"connection reset by peer",
		"broken pipe",
		"write: connection reset",
		"read: connection reset",
		"http: request body closed",
	}
	for _, pattern := range clientErrors {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	// Also check for context cancellation from the request
	if err == context.Canceled || strings.Contains(errStr, "context canceled") {
		return true
	}
	return false
}

func (c *Controller) ServeStream(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	// Global concurrent stream limiting - prevents resource exhaustion
	select {
	case c.userStreamSem <- struct{}{}:
		defer func() { <-c.userStreamSem }()
	case <-r.Context().Done():
		return spec.NewError(0, "request cancelled")
	default:
		// Server at capacity
		log.Printf("[STREAM] Server at capacity, rejecting stream request")
		w.Header().Set("Retry-After", "5")
		http.Error(w, "server busy, try again later", http.StatusServiceUnavailable)
		return nil
	}

	// Per-user stream limiting - prevent a single user from consuming all slots
	user := r.Context().Value(CtxUser).(*db.User)
	const maxStreamsPerUser = 5
	userSemVal, _ := c.userStreamLimits.LoadOrStore(user.ID, make(chan struct{}, maxStreamsPerUser))
	userSem := userSemVal.(chan struct{})
	select {
	case userSem <- struct{}{}:
		defer func() { <-userSem }()
	case <-r.Context().Done():
		return spec.NewError(0, "request cancelled")
	default:
		log.Printf("[STREAM] User %d at stream limit (%d), rejecting request", user.ID, maxStreamsPerUser)
		return spec.NewError(0, "too many concurrent streams for this user")
	}

	// [Deduplication] Prevent concurrent identical stream requests from hitting upstream
	// multiple times. If another request is already streaming this track, wait for it.
	// Key matches prepareStream cacheKey format for consistency
	// [LATENCY PRIORITY] For clients that need HLS stitching (supersonic, symfonium, etc.)
	// we skip deduplication entirely. Each client gets their own stitched stream immediately
	// rather than waiting for another request to complete. This prioritizes latency over bandwidth.
	clientNeedsHelp := doesntSupportHLS(p.GetOr("c", ""))

	streamKey := fmt.Sprintf("stream:%s:%d:dedup", id.String(), p.GetOrInt("maxBitRate", 0))
	type streamLock struct {
		done   chan struct{}
		err    error          // Propagate error from the first request to waiting requests
		prep   *streamRequest // Cached result for gapless playback
		cached time.Time      // When the result was cached
	}

	// Skip deduplication for clients that need stitching - prioritize latency
	var lockVal interface{}
	var loaded bool
	if clientNeedsHelp {
		// For HLS-stitching clients, don't wait - process immediately
		log.Printf("[STREAM] LATENCY PRIORITY: Processing track=%d immediately for %s (no dedup)", id.Value(), p.GetOr("c", ""))
		loaded = false
	} else {
		lockVal, loaded = c.streamLocks.LoadOrStore(streamKey, &streamLock{done: make(chan struct{})})
	}

	if loaded {
		// Another request is already streaming this track, wait for it to complete
		lock := lockVal.(*streamLock)
		<-lock.done
		// If the first request failed, propagate the error (unless it's a client disconnect)
		if lock.err != nil {
			log.Printf("[STREAM] Deduplicated request failed (original error: %v)", lock.err)
			c.streamLocks.Delete(streamKey)
			return spec.NewError(0, "stream failed: %v", lock.err)
		}
		// Note: Client disconnection errors (connection reset, broken pipe) are NOT stored
		// in lock.err, so they won't propagate to other waiting requests
		// [GAPLESS] Return cached result if within 10-minute window (HLS manifests last ~10 min)
		if lock.prep != nil && time.Since(lock.cached) < 10*time.Minute {
			// Safe to redirect to cached URL (client supports HLS natively)
			log.Printf("[STREAM] GAPLESS reusing cached stream for track=%d (age=%v)", id.Value(), time.Since(lock.cached))
			urlPreview := lock.prep.StreamURL
			if len(urlPreview) > 100 {
				urlPreview = urlPreview[:100]
			}
			log.Printf("[STREAM] REDIRECT track=%d IsHLS=%v URL=%s (cached)",
				id.Value(), lock.prep.IsHLS, urlPreview)
			http.Redirect(w, r, lock.prep.StreamURL, http.StatusFound)
			return nil
		}
		// Cache expired, clean up and proceed with new request
		c.streamLocks.Delete(streamKey)
	}
	// Cleanup lock when done - capture err in named return to propagate to waiters
	var streamErr error
	var streamPrep *streamRequest
	defer func() {
		if !loaded && lockVal != nil {
			// Only the first request cleans up and sets error if any
			// (lockVal is nil when we skipped dedup for latency priority)
			lock := lockVal.(*streamLock)
			// Don't propagate client disconnection errors to other waiting requests
			// If a client closes the connection, that doesn't mean the stream is broken
			if isClientDisconnectError(streamErr) {
				log.Printf("[STREAM] Client disconnected for track=%d, not propagating error to waiters", id.Value())
				lock.err = nil // Don't store client errors
			} else {
				lock.err = streamErr
			}
			lock.prep = streamPrep
			lock.cached = time.Now()
			close(lock.done)
			// Keep the lock entry for 10 minutes for gapless playback, then clean up
			go func() {
				time.Sleep(10 * time.Minute)
				c.streamLocks.Delete(streamKey)
			}()
		}
	}()

	prep, err := c.prepareStream(r.Context(), r, &id)
	if err != nil {
		streamErr = err
		log.Printf("[STREAM] ERROR: prepare failed: %v", err)
		return spec.NewError(0, "error preparing stream: %v", err)
	}
	// [GAPLESS] Capture successful prep for deduplication cache
	streamPrep = prep

	// [Safety] Add 10-minute timeout for stream operations to prevent indefinite hangs
	streamCtx, streamCancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer streamCancel()

	// 1. Ingest metadata for virtual library
	trackURI := fmt.Sprintf("td:tr:%d", id.Value())
	c.dbc.Exec(`INSERT OR REPLACE INTO track_metadata (uri, album_uri, artist_uri, updated_at) VALUES (?, ?, ?, ?)`,
		trackURI, fmt.Sprintf("td:al:%d", prep.Track.Album.ID), fmt.Sprintf("td:ar:%d", prep.Track.Artist.ID), time.Now())
	c.dbc.Exec(`INSERT INTO plays (user_id, uri, provider, played_at, count) VALUES (?, ?, 'tidal', ?, 1) ON CONFLICT(user_id, uri) DO UPDATE SET count=count+1, played_at=?`,
		user.ID, trackURI, time.Now(), time.Now())

	// 2. Route based on client HLS support
	proxyStreams := c.getCachedSetting("proxy_streams", "false")
	clientNeedsHelp = doesntSupportHLS(prep.ClientName) // Already declared earlier
	streamURL := prep.StreamURL

	if clientNeedsHelp && prep.IsHLS {
		// Client can't handle HLS natively - try to unwrap to progressive URL
		if directURL := c.unwrapTidalManifest(r.Context(), prep.StreamURL, prep.ClientIP, 0); directURL != "" {
			log.Printf("[STREAM] Unwrapped HLS to progressive for %s track=%d", prep.ClientName, id.Value())
			streamURL = directURL
			prep.IsHLS = false // Now it's a direct progressive stream
		} else {
			// Unwrap failed - must stitch for this client
			urlPreview := prep.StreamURL
			if len(urlPreview) > 100 {
				urlPreview = urlPreview[:100] + "..."
			}
			log.Printf("[STREAM] HLS stitch mode for %s track=%d URL=%s", prep.ClientName, id.Value(), urlPreview)
			// Fall through to stitching logic below
		}
	}

	// Redirect if not proxying and not stitching
	needsStitch := clientNeedsHelp && prep.IsHLS
	if proxyStreams != "true" && !needsStitch {
		urlPreview := streamURL
		if len(urlPreview) > 100 {
			urlPreview = urlPreview[:100]
		}
		log.Printf("[STREAM] REDIRECT track=%d IsHLS=%v URL=%s",
			id.Value(), prep.IsHLS, urlPreview)
		http.Redirect(w, r, streamURL, http.StatusFound)
		return nil
	}

	// 3. Proxy/Stitch stream
	if !prep.IsHLS && !prep.IsDASH {
		contentType := "audio/flac"
		if prep.Ext == "m4a" {
			contentType = "audio/mp4"
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Accept-Ranges", "bytes")
		if strings.Contains(r.URL.Path, "download") {
			cleanName := strings.ReplaceAll(fmt.Sprintf("%s - %s.%s", prep.Track.Artist.Name, prep.Track.Title, prep.Ext), "/", "_")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", cleanName))
		}
	}

	if prep.IsHLS && needsStitch {
		// Stitching required: download and stitch HLS segments
		urlPreview := prep.StreamURL
		if len(urlPreview) > 100 {
			urlPreview = urlPreview[:100]
		}
		log.Printf("[STREAM] Starting HLS stitch for %s track=%d", prep.ClientName, id.Value())

		// --- RANGE CALCULATION LOGIC ---
		rangeHdr := r.Header.Get("Range")
		offsetSeconds := 0.0
		startByte := int64(0)
		var totalBytes int64 = 0

		bytesPerSec := int64(125000)
		if prep.Quality == "HIGH" || prep.Quality == "LOW" || prep.Ext == "m4a" {
			bytesPerSec = 40000
		} else if prep.Quality == "HI_RES_LOSSLESS" {
			bytesPerSec = 175000
		}

		if prep.Track != nil && prep.Track.Duration > 0 {
			totalBytes = int64(prep.Track.Duration) * bytesPerSec
		}

		timeOffsetParam := p.GetOrFloat("timeOffset", 0.0)
		if timeOffsetParam > 0 {
			offsetSeconds = timeOffsetParam
		} else if rangeHdr != "" && strings.HasPrefix(rangeHdr, "bytes=") && totalBytes > 0 {
			parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
			if len(parts) > 0 && parts[0] != "" {
				if parsedByte, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
					startByte = parsedByte
					offsetSeconds = float64(startByte) / float64(bytesPerSec)
					if offsetSeconds > float64(prep.Track.Duration) {
						offsetSeconds = float64(prep.Track.Duration) - 1
					}
					if offsetSeconds < 0 {
						offsetSeconds = 0
					}
				}
			}
		}

		err = c.downloadAndStitchHLS(streamCtx, prep.StreamURL, w, prep.ClientIP, prep.Track, offsetSeconds, startByte, totalBytes, prep.ClientName)
		if err != nil {
			streamErr = err
			log.Printf("[STREAM] ERROR: HLS stitch failed for track %d: %v", id.Value(), err)
		} else {
			log.Printf("[STREAM] HLS stitch succeeded for track %d", id.Value())
		}
	} else if prep.IsDASH {
		// [NEW] Serve DASH manifest directly
		err = c.proxyDASHManifest(streamCtx, prep.StreamURL, w, prep.ClientIP)
		if err != nil {
			streamErr = err
			log.Printf("[STREAM] ERROR: DASH proxy failed for track %d: %v", id.Value(), err)
		}
	} else {
		// Direct proxy with proper error handling and header passthrough
		err = c.proxyDirectStream(streamCtx, w, r, prep)
		if err != nil {
			streamErr = err
			log.Printf("[STREAM] ERROR: direct proxy failed for track %d: %v", id.Value(), err)
		}
	}
	return nil
}

// ServeHLS handles the /hls.m3u8 endpoint according to OpenSubsonic spec
// Returns an HLS playlist (M3U8) that clients can use for HLS streaming
// The client is responsible for downloading and playing the segments
func (c *Controller) ServeHLS(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	// Get stream info using existing prepareStream logic
	ctx := r.Context()
	prep, err := c.prepareStream(ctx, r, &id)
	if err != nil {
		log.Printf("[HLS] Failed to prepare stream for track %d: %v", id.Value(), err)
		return spec.NewError(0, "failed to get stream: %v", err)
	}

	// Verify we got an HLS stream
	if !prep.IsHLS {
		log.Printf("[HLS] Track %d is not available as HLS (URL: %s)", id.Value(), prep.StreamURL)
		return spec.NewError(0, "track not available as HLS stream")
	}

	// Proxy the HLS manifest - this rewrites relative URLs to absolute
	// and ensures the client gets a valid M3U8 that works from their IP
	urlPreview := prep.StreamURL
	if len(urlPreview) > 100 {
		urlPreview = urlPreview[:100]
	}
	log.Printf("[HLS] Proxying M3U8 manifest for track=%d URL=%s client=%s",
		id.Value(), urlPreview, prep.ClientName)

	if err := c.proxyHLSManifest(ctx, prep.StreamURL, w, prep.ClientIP); err != nil {
		log.Printf("[HLS] Failed to proxy manifest for track %d: %v", id.Value(), err)
		return spec.NewError(0, "failed to proxy HLS manifest: %v", err)
	}
	return nil
}

// proxyDirectStream handles direct proxying of audio streams with proper header passthrough,
// range request support, retry logic, and optimized buffer pooling for minimal CPU usage.
func (c *Controller) proxyDirectStream(ctx context.Context, w http.ResponseWriter, r *http.Request, prep *streamRequest) error {
	// Build request with proper headers
	req, err := http.NewRequestWithContext(ctx, "GET", prep.StreamURL, nil)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return fmt.Errorf("create request: %w", err)
	}

	// Forward Range header for seeking support
	if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}

	// Set client IP for Tidal's geo-locking
	if prep.ClientIP != "" {
		req.Header.Set("X-Forwarded-For", prep.ClientIP)
		req.Header.Set("X-Real-IP", prep.ClientIP)
	}

	// Retry logic for transient failures
	const maxRetries = 3
	var resp *http.Response
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms
			delay := time.Duration(100*(1<<attempt)) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			// Recreate request for retry (body was consumed)
			req, _ = http.NewRequestWithContext(ctx, "GET", prep.StreamURL, nil)
			if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
				req.Header.Set("Range", rangeHdr)
			}
			if prep.ClientIP != "" {
				req.Header.Set("X-Forwarded-For", prep.ClientIP)
				req.Header.Set("X-Real-IP", prep.ClientIP)
			}
		}

		resp, err = c.streamClient.Do(req)
		if err == nil {
			// Check if we got a successful response
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
				break // Success!
			}
			// Non-success status, close and retry if retryable
			resp.Body.Close()
			if !isRetryableError(fmt.Sprintf("HTTP %d", resp.StatusCode)) {
				break // Don't retry client errors (4xx)
			}
		} else {
			lastErr = err
			if !isRetryableError(err.Error()) {
				break // Non-retryable error
			}
		}
	}

	if err != nil {
		http.Error(w, "stream unavailable", http.StatusBadGateway)
		return fmt.Errorf("fetch stream (retried %d): %w", maxRetries, lastErr)
	}
	defer resp.Body.Close()

	// Handle non-success status codes
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		http.Error(w, "stream unavailable", http.StatusBadGateway)
		return fmt.Errorf("stream returned %d", resp.StatusCode)
	}

	// Pass through critical headers for proper seeking and metadata
	// Content-Type already set by caller, but we can enhance with upstream info
	if ct := resp.Header.Get("Content-Type"); ct != "" && ct != "application/octet-stream" {
		w.Header().Set("Content-Type", ct)
	}

	// Content-Length is critical for seek bar calculation in clients
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}

	// Content-Range for partial content responses (seek responses)
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		w.Header().Set("Content-Range", cr)
	}

	// Accept-Ranges already set by caller, but confirm support
	if ar := resp.Header.Get("Accept-Ranges"); ar != "" {
		w.Header().Set("Accept-Ranges", ar)
	}

	// ETag for caching (optional but helpful)
	if etag := resp.Header.Get("ETag"); etag != "" {
		w.Header().Set("ETag", etag)
	}

	// Write status code before body
	w.WriteHeader(resp.StatusCode)

	// Use buffer pool for efficient copying with minimal GC pressure
	bufPtr := c.streamBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer c.streamBufPool.Put(bufPtr)

	// Stream with periodic flushes for low-latency delivery
	// Use http.Flusher if available to send data immediately to client
	flusher, canFlush := w.(http.Flusher)

	_, err = io.CopyBuffer(w, resp.Body, buf)
	if err != nil {
		// Client likely disconnected, don't treat as error
		if ctx.Err() == context.Canceled {
			return nil
		}
		return fmt.Errorf("copy stream: %w", err)
	}

	// Final flush to ensure all data sent
	if canFlush {
		flusher.Flush()
	}

	return nil
}

// getCachedSetting retrieves a setting from cache or DB if not cached.
// Uses 5-second TTL to avoid DB pressure during high-traffic streaming.
func (c *Controller) getCachedSetting(key, defaultVal string) string {
	if cached := c.settingsCache.Get(key); cached != "" {
		return cached
	}
	val := c.dbc.GetSetting(key, defaultVal)
	c.settingsCache.Set(key, val, 5*time.Second)
	return val
}

func (c *Controller) ServeGetCoverArt(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil {
		log.Printf("[SUBS] getCoverArt: invalid id %q", p.GetOr("id", ""))
		w.WriteHeader(http.StatusNotFound)
		return nil
	}

	size := p.GetOrInt("size", 600)
	if id.Type() == specid.Artist {
		switch {
		case size <= 160:
			size = 160
		case size <= 320:
			size = 320
		case size <= 480:
			size = 480
		case size <= 750:
			size = 750
		default:
			size = 1000
		}
	} else {
		switch {
		case size <= 80:
			size = 80
		case size <= 160:
			size = 160
		case size <= 320:
			size = 320
		case size <= 640:
			size = 640
		default:
			size = 1280
		}
	}

	cacheKey := fmt.Sprintf("%s-%d-%d", id.Type(), id.Value(), size)
	cachePath := filepath.Join(c.cachePath, cacheKey+".jpg")

	// check disk cache first (fast path)
	if data, err := os.ReadFile(cachePath); err == nil {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(data)
		return nil
	}

	// negative cache: avoid re-fetching covers we know are missing
	negKey := fmt.Sprintf("neg-%s-%d", id.Type(), id.Value())
	if c.negCoverCache.Get(negKey) {
		w.Header().Set("Content-Type", "image/gif")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Write(transparentPixel)
		return nil
	}

	// deduplication: only one request fetches, others wait
	type lockPair struct {
		mu     sync.Mutex
		done   chan struct{}
		data   []byte
		status int // 0=pending, 1=success, 2=error
	}

	lockVal, loaded := c.coverLocks.LoadOrStore(cacheKey, &lockPair{done: make(chan struct{})})
	lp := lockVal.(*lockPair)

	if loaded {
		// another request is in progress, wait for it
		<-lp.done
		lp.mu.Lock()
		if lp.status == 1 {
			lp.mu.Unlock()
			w.Header().Set("Content-Type", "image/jpeg")
			w.Header().Set("Cache-Control", "public, max-age=86400")
			w.Write(lp.data)
		} else {
			lp.mu.Unlock()
			w.Header().Set("Content-Type", "image/gif")
			w.Header().Set("Cache-Control", "public, max-age=3600")
			w.Write(transparentPixel)
		}
		return nil
	}

	// we are the fetcher - do the work
	coverData, err := c.fetchAndCacheCover(r.Context(), &id, size, cachePath, negKey)

	lp.mu.Lock()
	if err == nil && coverData != nil {
		lp.status = 1
		lp.data = coverData
	} else {
		lp.status = 2
	}
	close(lp.done)
	lp.mu.Unlock()

	// cleanup lock after request completes
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.coverLocks.Delete(cacheKey)
	}()

	if err != nil {
		w.Header().Set("Content-Type", "image/gif")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Write(transparentPixel)
		return nil
	}

	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Write(coverData)
	return nil
}

func (c *Controller) fetchAndCacheCover(ctx context.Context, id *specid.ID, size int, cachePath, negKey string) ([]byte, error) {
	// resolve cover UUID
	var coverUUID string
	var coverURL string

	switch id.Type() {
	case specid.Album:
		coverUUID = c.proxy.GetCoverUUIDForAlbum(ctx, id.Value())
	case specid.Artist:
		info, err := c.proxy.GetArtistInfo(ctx, id.Value())
		if err == nil {
			coverUUID = info.Artist.Picture
		}
	case specid.Track:
		track, err := c.proxy.GetTrackInfo(ctx, id.Value())
		if err == nil {
			coverUUID = track.Album.Cover
		}
	case specid.Playlist:
		// Fetch playlist cover from DB
		var pl db.Playlist
		if err := c.dbc.Where("id = ?", id.Value()).First(&pl).Error; err == nil {
			log.Printf("[COVER] Playlist %d: CoverURL=%q CoverPath=%q", id.Value(), pl.CoverURL, pl.CoverPath)
			if pl.CoverPath != "" {
				// Local custom cover - read from filesystem
				if data, err := os.ReadFile(pl.CoverPath); err == nil {
					log.Printf("[COVER] Serving local cover from %s", pl.CoverPath)
					os.MkdirAll(filepath.Dir(cachePath), 0755)
					os.WriteFile(cachePath, data, 0644)
					return data, nil
				}
				log.Printf("[COVER] Failed to read local cover: %v", err)
			}
			if pl.CoverURL != "" {
				// Check if CoverURL is a UUID (not a full URL)
				if !strings.HasPrefix(pl.CoverURL, "http") {
					// It's a UUID, convert to full URL using proxy
					coverURL = c.proxy.GetCoverURL(pl.CoverURL, size)
					log.Printf("[COVER] Converted UUID %s to URL: %s", pl.CoverURL, coverURL)
				} else {
					// It's already a full URL
					coverURL = pl.CoverURL
					log.Printf("[COVER] Using external URL: %s", coverURL)
				}
			}
			// Check for auto-generated composite cover
			compositePath := filepath.Join(c.cachePath, "playlist-covers", fmt.Sprintf("pl-%d.jpg", id.Value()))
			if data, err := os.ReadFile(compositePath); err == nil {
				log.Printf("[COVER] Serving composite cover from %s", compositePath)
				os.MkdirAll(filepath.Dir(cachePath), 0755)
				os.WriteFile(cachePath, data, 0644)
				return data, nil
			}
			log.Printf("[COVER] No composite cover found at %s", compositePath)
		} else {
			log.Printf("[COVER] Playlist %d not found in DB: %v", id.Value(), err)
		}
	}

	if coverUUID == "" && coverURL == "" {
		c.negCoverCache.Set(negKey, true, 0)
		return nil, fmt.Errorf("no cover found")
	}

	if coverURL == "" {
		coverURL = c.proxy.GetCoverURL(coverUUID, size)
	}
	if coverURL == "" {
		return nil, fmt.Errorf("no cover URL")
	}

	proxyClient := &http.Client{Timeout: 8 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, "GET", coverURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := proxyClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, fmt.Errorf("fetch failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// write to disk cache
	os.MkdirAll(filepath.Dir(cachePath), 0755)
	os.WriteFile(cachePath, data, 0644)

	return data, nil
}

// 1x1 transparent GIF — returned when cover art is unavailable so clients stop retrying
var transparentPixel = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00,
	0x80, 0x00, 0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21,
	0xf9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00,
	0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3b,
}
