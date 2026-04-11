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

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeStream(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	prep, err := c.prepareStream(r.Context(), r, id.Value())
	if err != nil {
		log.Printf("[STREAM] ERROR: prepare failed: %v", err)
		return spec.NewError(0, "error preparing stream: %v", err)
	}

	// 1. Ingest metadata for virtual library
	user := r.Context().Value(CtxUser).(*db.User)
	trackURI := fmt.Sprintf("td:tr:%d", id.Value())
	c.dbc.Exec(`INSERT OR REPLACE INTO track_metadata (uri, album_uri, artist_uri, updated_at) VALUES (?, ?, ?, ?)`,
		trackURI, fmt.Sprintf("td:al:%d", prep.Track.Album.ID), fmt.Sprintf("td:ar:%d", prep.Track.Artist.ID), time.Now())
	c.dbc.Exec(`INSERT INTO plays (user_id, uri, provider, played_at, count) VALUES (?, ?, 'tidal', ?, 1) ON CONFLICT(user_id, uri) DO UPDATE SET count=count+1, played_at=?`,
		user.ID, trackURI, time.Now(), time.Now())

	// 2. Redirect if not proxying
	proxyStreams := c.getCachedSetting("proxy_streams", "false")
	if proxyStreams != "true" && !prep.IsHLS && !prep.IsDASH {
		http.Redirect(w, r, prep.StreamURL, http.StatusFound)
		return nil
	}

	// 3. Proxy stream
	contentType := "audio/flac"
	if prep.Ext == "m4a" { contentType = "audio/mp4" }
	w.Header().Set("Content-Type", contentType)
	if !prep.IsHLS && !prep.IsDASH { w.Header().Set("Accept-Ranges", "bytes") }
	if strings.Contains(r.URL.Path, "download") {
		cleanName := strings.ReplaceAll(fmt.Sprintf("%s - %s.%s", prep.Track.Artist.Name, prep.Track.Title, prep.Ext), "/", "_")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", cleanName))
	}

	if prep.IsHLS {
		err = c.downloadAndStitchHLS(r.Context(), prep.StreamURL, w, prep.ClientIP, prep.Track, p.GetOrFloat("timeOffset", 0))
	} else if prep.IsDASH {
		err = c.downloadAndStitchDASH(r.Context(), prep.StreamURL, w, prep.ClientIP, prep.Track)
	} else {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", prep.StreamURL, nil)
		if rangeHdr := r.Header.Get("Range"); rangeHdr != "" { req.Header.Set("Range", rangeHdr) }
		req.Header.Set("X-Forwarded-For", prep.ClientIP)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("[STREAM] ERROR: direct fetch failed: %v", err)
			return nil
		}
		defer resp.Body.Close()
		for k, v := range resp.Header { w.Header()[k] = v }
		w.WriteHeader(resp.StatusCode)
		_, err = io.Copy(w, resp.Body)
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
