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
		tidalQuality = "HI_RES_LOSSLESS"
	case bitrate <= 128:
		tidalQuality = "LOW"
	case bitrate <= 320:
		tidalQuality = "HIGH"
	case bitrate <= 1411:
		tidalQuality = "LOSSLESS"
	default:
		tidalQuality = "HI_RES_LOSSLESS"
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

	// For /stream, we use a simple redirect (fastest for playback)
	if !strings.Contains(r.URL.Path, "download") {
		log.Printf("[STREAM] track %d → redirect to: %s", id.Value, streamURL)
		http.Redirect(w, r, streamURL, http.StatusTemporaryRedirect)
		return nil
	}

	// For /download, we proxy and stitch segments for a complete file
	log.Printf("[DOWNLOAD] track %d → proxying and stitching from: %s", id.Value, streamURL)
	
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

	var stitchErr error
	if isHLS {
		log.Printf("[DOWNLOAD] Detected HLS manifest, stitching...")
		stitchErr = c.downloadAndStitchHLS(r.Context(), streamURL, w, clientIP, track)
	} else if isDASH {
		log.Printf("[DOWNLOAD] Detected DASH manifest, stitching...")
		stitchErr = c.downloadAndStitchDASH(r.Context(), streamURL, w, clientIP, track)
	} else {
		// Write headers immediately for direct URL fallback
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		w.Header().Set("Transfer-Encoding", "chunked")

		log.Printf("[DOWNLOAD] Detected direct URL, proxying...")
		req, _ := http.NewRequestWithContext(r.Context(), "GET", streamURL, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
		if clientIP != "" {
			req.Header.Set("X-Forwarded-For", clientIP)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
			_, stitchErr = io.Copy(w, resp.Body)
		} else {
			stitchErr = err
		}
	}

	if stitchErr != nil {
		log.Printf("[DOWNLOAD] Stitch error for track %d: %v", id.Value, stitchErr)
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
		return spec.NewError(70, "cover art not found")
	}

	// build CDN URL and proxy
	coverURL := c.proxy.GetCoverURL(coverUUID, size)
	if coverURL == "" {
		return spec.NewError(70, "cover art not found")
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
		w.WriteHeader(http.StatusNotFound)
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


