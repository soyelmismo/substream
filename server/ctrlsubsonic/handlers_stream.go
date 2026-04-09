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

	// Use a detached context with timeout so client disconnections
	// don't cancel the upstream proxy call. Clients like Supersonic
	// cancel and retry rapidly, killing in-flight requests.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

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

	streamURL, err := c.proxy.GetStreamURL(ctx, id.Value, tidalQuality, clientIP)
	if err != nil {
		log.Printf("stream error for track %d: %v", id.Value, err)
		return spec.NewError(0, "error getting stream URL: %v", err)
	}

	log.Printf("[STREAM] track %d → redirect to: %s", id.Value, streamURL)
	http.Redirect(w, r, streamURL, http.StatusTemporaryRedirect)
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

	req, _ := http.NewRequest("GET", coverURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		status := 0
		if resp != nil {
			status = resp.StatusCode
			resp.Body.Close()
		}
		log.Printf("[SUBS] error fetching cover art from %s: err=%v status=%d", coverURL, err, status)
		return spec.NewError(0, "error fetching cover art")
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return spec.NewError(0, "error reading cover art")
	}

	// cache to disk
	_ = os.MkdirAll(c.cachePath, 0o755)
	_ = os.WriteFile(cachePath, data, 0o644)

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "image/jpeg"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Write(data)
	return nil
}


