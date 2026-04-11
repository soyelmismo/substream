package ctrlsubsonic

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeStream(w http.ResponseWriter, r *http.Request) *spec.Response {
	start := time.Now()
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Track {
		log.Printf("[STREAM] ERROR: invalid track id: %v", err)
		return spec.NewError(10, "provide a track `id` parameter")
	}

	// Log raw request details for debugging
	rawID := p.GetOr("id", "")
	client := p.GetOr("c", "unknown")
	log.Printf("[STREAM] REQUEST: client=%s raw_id=%s parsed_track_id=%d", client, rawID, id.Value)

	bitrate := p.GetOrInt("maxBitRate", 0)
	tidalQuality := ""
	switch {
	case bitrate == 0:
		// Default to LOSSLESS (CD Quality FLAC) instead of HI_RES
		// because HI_RES often uses DASH containers which break clients.
		tidalQuality = "LOSSLESS"
	case bitrate <= 128:
		tidalQuality = "LOW"
	case bitrate <= 320:
		tidalQuality = "HIGH"
	case bitrate >= 900:
		tidalQuality = "HI_RES_LOSSLESS"
	default:
		tidalQuality = "LOSSLESS"
	}

	// Use a detached context with timeout for upstream API fetching so
	// we don't spam if client disconnects quickly.
	// The download itself will use r.Context() to stop if client aborts.
	metaCtx, metaCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer metaCancel()

	clientIP := r.Header.Get("X-Forwarded-For")
	if clientIP == "" {
		clientIP = r.Header.Get("X-Real-IP")
	}
	if clientIP == "" {
		clientIP = r.RemoteAddr
		if pos := strings.LastIndex(clientIP, ":"); pos != -1 {
			clientIP = clientIP[:pos]
		}
		clientIP = strings.Trim(clientIP, "[]")
	}

	proxyStart := time.Now()
	streamURL, err := c.getStreamURLWithCache(metaCtx, id.Value, tidalQuality, clientIP)
	proxyDuration := time.Since(proxyStart)
	if err != nil {
		log.Printf("[STREAM] ERROR: GetStreamURL failed for track %d after %v: %v", id.Value, proxyDuration, err)
		return spec.NewError(0, "error getting stream URL: %v", err)
	}

	// Log stream URL hash for debugging (to identify if same URL is returned for different tracks)
	urlHash := ""
	if len(streamURL) > 20 {
		urlHash = streamURL[:20] + "..." + streamURL[len(streamURL)-10:]
	}
	log.Printf("[STREAM] URL: track=%d quality=%s url_hash=%s", id.Value, tidalQuality, urlHash)

	// Debug: log full URL if it looks suspicious (no query params or very short)
	if len(streamURL) < 50 || !strings.Contains(streamURL, "?") {
		log.Printf("[STREAM] DEBUG: track=%d full_url=%q", id.Value, streamURL)
	}

	// Determine content type and extensions early to check if we need proxy
	ext := "flac"
	if strings.Contains(streamURL, ".m4a") || strings.Contains(tidalQuality, "HIGH") || strings.Contains(tidalQuality, "LOW") {
		ext = "m4a"
	}

	// Force proxy for HLS/DASH manifests - clients can't play manifests directly
	isHLS := strings.Contains(streamURL, ".m3u8") || strings.Contains(streamURL, "manifestType=HLS")
	isDASH := strings.Contains(streamURL, ".mpd") || strings.Contains(streamURL, "manifestType=MPEG_DASH")

	// Check if we should proxy or redirect based on settings (cached)
	settingStart := time.Now()
	proxyStreams := c.getCachedSetting("proxy_streams", "false")
	// Force proxy for HLS/DASH streams regardless of setting
	if isHLS || isDASH {
		proxyStreams = "true"
	}
	settingDuration := time.Since(settingStart)
	if proxyStreams != "true" {
		// Redirect directly to tidal CDN - better performance but may cause CORS issues
		if streamURL == "" {
			log.Printf("[STREAM] track %d → error: empty stream URL for redirect", id.Value)
			return spec.NewError(0, "empty stream URL from tidal")
		}
		totalDuration := time.Since(start)
		log.Printf("[STREAM] REDIRECT: track=%d → 302 to CDN (proxy=%s) total=%v proxy=%v setting=%v url=%s",
			id.Value, proxyStreams, totalDuration, proxyDuration, settingDuration, urlHash)
		http.Redirect(w, r, streamURL, http.StatusFound) // 302 redirect, no body
		return nil
	}

	// Use a proxy instead of redirect to avoid CORS issues for web clients
	// and certificate issues for clients using self-signed certs.

	// Determine content type and extensions
	track, err := c.proxy.GetTrackInfo(metaCtx, id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching track meta: %v", err)
	}

	contentType := "audio/flac"
	if ext == "m4a" {
		contentType = "audio/mp4"
	}

	filename := fmt.Sprintf("%s - %s.%s", track.Artist.Name, track.Title, ext)
	filename = strings.ReplaceAll(filename, "/", "_") // basic sanitization

	streamType := "direct"
	if isHLS {
		streamType = "HLS"
	} else if isDASH {
		streamType = "DASH"
	}
	log.Printf("[STREAM] PROXY: track=%d type=%s format=%s artist=%q title=%q", id.Value, streamType, contentType, track.Artist.Name, track.Title)

	w.Header().Set("Content-Type", contentType)
	if strings.Contains(r.URL.Path, "download") {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	}

	var stitchErr error
	if isHLS {
		stitchErr = c.downloadAndStitchHLS(r.Context(), streamURL, w, clientIP, track)
	} else if isDASH {
		stitchErr = c.downloadAndStitchDASH(r.Context(), streamURL, w, clientIP, track)
	} else {
		// Forward Range headers for seeking support in web players
		req, _ := http.NewRequestWithContext(r.Context(), "GET", streamURL, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
		if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
			req.Header.Set("Range", rangeHdr)
		}
		if clientIP != "" {
			req.Header.Set("X-Forwarded-For", clientIP)
		}

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			defer resp.Body.Close()

			// Forward important headers back to client
			w.Header().Set("Accept-Ranges", "bytes")
			if ct := resp.Header.Get("Content-Type"); ct != "" {
				w.Header().Set("Content-Type", ct)
			}
			if cr := resp.Header.Get("Content-Range"); cr != "" {
				w.Header().Set("Content-Range", cr)
			}
			if cl := resp.Header.Get("Content-Length"); cl != "" {
				w.Header().Set("Content-Length", cl)
			}
			w.WriteHeader(resp.StatusCode)
			_, stitchErr = io.Copy(w, resp.Body)
		} else {
			stitchErr = err
		}
	}

	if stitchErr != nil {
		log.Printf("[STREAM] ERROR: proxy failed for track %d: %v", id.Value, stitchErr)
	} else {
		log.Printf("[STREAM] COMPLETE: track=%d type=%s", id.Value, streamType)
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

// streamURLLockPair is used for deduplicating concurrent stream URL requests
type streamURLLockPair struct {
	done chan struct{}
	url  string
	err  error
}

// getStreamURLWithCache retrieves stream URL with caching and deduplication.
// If multiple requests ask for the same track simultaneously, only one calls Tidal API.
func (c *Controller) getStreamURLWithCache(ctx context.Context, trackID int, quality, clientIP string) (string, error) {
	cacheKey := fmt.Sprintf("stream:%d:%s", trackID, quality)

	// Fast path: check cache
	if cached := c.streamURLCache.Get(cacheKey); cached != "" {
		return cached, nil
	}

	// Deduplication: only one request fetches, others wait
	lockVal, loaded := c.streamURLLocks.LoadOrStore(cacheKey, &streamURLLockPair{done: make(chan struct{})})
	lp := lockVal.(*streamURLLockPair)

	if loaded {
		// Another request is in flight, wait for it
		<-lp.done
		if lp.err != nil {
			return "", lp.err
		}
		return lp.url, nil
	}

	// We are the fetcher - do the work
	url, err := c.proxy.GetStreamURL(ctx, trackID, quality, clientIP)

	lp.url = url
	lp.err = err
	close(lp.done)

	// Cache successful result
	if err == nil && url != "" {
		c.streamURLCache.Set(cacheKey, url, 30*time.Second)
	}

	// Cleanup lock after brief delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.streamURLLocks.Delete(cacheKey)
	}()

	return url, err
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
	if id.Type == specid.Artist {
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

	cacheKey := fmt.Sprintf("%s-%d-%d", id.Type, id.Value, size)
	cachePath := filepath.Join(c.cachePath, cacheKey+".jpg")

	// check disk cache first (fast path)
	if data, err := os.ReadFile(cachePath); err == nil {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(data)
		return nil
	}

	// negative cache: avoid re-fetching covers we know are missing
	negKey := fmt.Sprintf("neg-%s-%d", id.Type, id.Value)
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
	switch id.Type {
	case specid.Album:
		coverUUID = c.proxy.GetCoverUUIDForAlbum(ctx, id.Value)
	case specid.Artist:
		info, err := c.proxy.GetArtistInfo(ctx, id.Value)
		if err == nil {
			coverUUID = info.Artist.Picture
		}
	case specid.Track:
		track, err := c.proxy.GetTrackInfo(ctx, id.Value)
		if err == nil {
			coverUUID = track.Album.Cover
		}
	}

	if coverUUID == "" {
		c.negCoverCache.Set(negKey, true, 0)
		return nil, fmt.Errorf("no cover found")
	}

	coverURL := c.proxy.GetCoverURL(coverUUID, size)
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
