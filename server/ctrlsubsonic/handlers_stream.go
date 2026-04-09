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
	"time"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeStream(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

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

	streamURL, err := c.proxy.GetStreamURL(metaCtx, id.Value, tidalQuality, clientIP)
	if err != nil {
		log.Printf("stream error for track %d: %v", id.Value, err)
		return spec.NewError(0, "error getting stream URL: %v", err)
	}

	// Use a proxy instead of redirect to avoid CORS issues for web clients
	// and certificate issues for clients using self-signed certs.

	// Determine content type and extensions
	track, err := c.proxy.GetTrackInfo(metaCtx, id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching track meta: %v", err)
	}

	ext := "flac"
	contentType := "audio/flac"
	if strings.Contains(streamURL, ".m4a") || strings.Contains(tidalQuality, "HIGH") || strings.Contains(tidalQuality, "LOW") {
		ext = "m4a"
		contentType = "audio/mp4"
	}

	filename := fmt.Sprintf("%s - %s.%s", track.Artist.Name, track.Title, ext)
	filename = strings.ReplaceAll(filename, "/", "_") // basic sanitization

	isHLS := strings.Contains(streamURL, ".m3u8") || strings.Contains(streamURL, "manifestType=HLS")
	isDASH := strings.Contains(streamURL, ".mpd") || strings.Contains(streamURL, "manifestType=MPEG_DASH")

	log.Printf("[STREAM] track %d → proxying from tidal (type=%s)", id.Value, contentType)

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
		log.Printf("[STREAM] error for track %d: %v", id.Value, stitchErr)
	}
	return nil
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
		// Artist profile images use different sizes
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
		// Album/track covers
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

	// check disk cache first
	cacheKey := fmt.Sprintf("%s-%d-%d", id.Type, id.Value, size)
	cachePath := filepath.Join(c.cachePath, cacheKey+".jpg")
	if data, err := os.ReadFile(cachePath); err == nil {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(data)
		return nil
	}

	// negative cache: avoid re-fetching covers we know are missing
	negKey := fmt.Sprintf("neg-%s-%d", id.Type, id.Value)
	if _, negHit := c.searchCache.Load(negKey); negHit {
		// serve 1x1 transparent pixel so client stops retrying
		w.Header().Set("Content-Type", "image/gif")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Write(transparentPixel)
		return nil
	}

	// resolve cover UUID
	var coverUUID string
	switch id.Type {
	case specid.Album:
		coverUUID = c.proxy.GetCoverUUIDForAlbum(r.Context(), id.Value)
	case specid.Artist:
		info, err := c.proxy.GetArtistInfo(r.Context(), id.Value)
		if err == nil {
			coverUUID = info.Artist.Picture
		}
	case specid.Track:
		track, err := c.proxy.GetTrackInfo(r.Context(), id.Value)
		if err == nil {
			coverUUID = track.Album.Cover
		}
	}

	if coverUUID == "" {
		c.searchCache.Store(negKey, true) // mark as missing
		w.Header().Set("Content-Type", "image/gif")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Write(transparentPixel)
		return nil
	}

	// build CDN URL and proxy
	coverURL := c.proxy.GetCoverURL(coverUUID, size)
	if coverURL == "" {
		w.Header().Set("Content-Type", "image/gif")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Write(transparentPixel)
		return nil
	}

	// proxy from CDN with short timeout — no disk cache to keep stateless
	proxyClient := &http.Client{Timeout: 8 * time.Second}
	req, _ := http.NewRequestWithContext(r.Context(), "GET", coverURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := proxyClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		w.Header().Set("Content-Type", "image/gif")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Write(transparentPixel)
		return nil
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "image/jpeg"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	io.Copy(w, resp.Body)
	return nil
}

// 1x1 transparent GIF — returned when cover art is unavailable so clients stop retrying
var transparentPixel = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00,
	0x80, 0x00, 0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21,
	0xf9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00,
	0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3b,
}
