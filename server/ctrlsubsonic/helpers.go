package ctrlsubsonic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/tidalproxy"
)

// streamRequest bundle for unified stream preparation
type streamRequest struct {
	Quality    string
	ClientIP   string
	StreamURL  string
	Track      *tidalproxy.TidalTrack
	Ext        string
	IsHLS      bool
	IsDASH     bool
	ClientName string // for client-specific handling (e.g., tempus, symfonium)
}

// batchFetch is a generic concurrent fetcher with rate limiting and order preservation.
func batchFetch[T any, R any](
	ctx context.Context,
	sem chan struct{},
	ids []int,
	fetchFn func(context.Context, int) (*T, error),
	mapFn func(*T, int) *R,
) []*R {
	if len(ids) == 0 {
		return nil
	}

	type result struct {
		idx  int
		data *R
	}

	results := make(chan result, len(ids))
	var wg sync.WaitGroup

	for i, id := range ids {
		wg.Add(1)
		go func(idx, tid int) {
			defer wg.Done()

			// Block on semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results <- result{idx: idx, data: nil}
				return
			}

			data, err := fetchFn(ctx, tid)
			if err == nil && data != nil {
				results <- result{idx: idx, data: mapFn(data, tid)}
			} else {
				results <- result{idx: idx, data: nil}
			}
		}(i, id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	ordered := make([]*R, len(ids))
	for r := range results {
		ordered[r.idx] = r.data
	}

	var final []*R
	for _, item := range ordered {
		if item != nil {
			final = append(final, item)
		}
	}
	return final
}

// batchFetchTracks fetches metadata for multiple tidal track IDs concurrently
func (c *Controller) batchFetchTracks(r *http.Request, tidalIDs []int) []*spec.TrackChild {
	user := r.Context().Value(CtxUser).(*db.User)
	return batchFetch(r.Context(), c.proxySem, tidalIDs, c.proxy.GetTrackInfo, func(t *tidalproxy.TidalTrack, tid int) *spec.TrackChild {
		tc := spec.NewTrackFromTidal(t)
		c.applyTrackStar(user.ID, tc)
		c.applyTrackPlayCount(user.ID, tc)
		tc.UserRating = c.getTrackRating(user.ID, fmt.Sprintf("td:tr:%d", tid))
		return tc
	})
}

// batchFetchAlbums fetches metadata for multiple tidal album IDs efficiently
// Uses batch SQLite query (1 query) instead of N individual queries
func (c *Controller) batchFetchAlbums(r *http.Request, tidalIDs []int) []*spec.Album {
	user := r.Context().Value(CtxUser).(*db.User)
	ctx := r.Context()

	// Use batch method: single SQL query + parallel API fallback
	albumMap := c.proxy.GetAlbumsInfoBatch(ctx, tidalIDs)
	if albumMap == nil {
		return nil
	}

	// Convert map to ordered slice
	results := make([]*spec.Album, 0, len(albumMap))
	for _, id := range tidalIDs {
		if info, ok := albumMap[id]; ok {
			a := spec.NewAlbumFromTidal(info)
			c.applyAlbumStar(user.ID, a)
			a.UserRating = c.getAlbumRating(user.ID, fmt.Sprintf("td:al:%d", id))
			results = append(results, a)
		}
	}

	return results
}

// prepareStream centralizes the logic for preparing a stream (quality, IP, URL, meta)
// shared by ServeStream and ServeDownload
func (c *Controller) prepareStream(ctx context.Context, r *http.Request, trackID int) (*streamRequest, error) {
	p := r.Context().Value(CtxParams).(params.Params)

	// 1. Quality
	bitrate := p.GetOrInt("maxBitRate", 0)
	quality := "LOSSLESS"
	switch {
	case bitrate == 0:
		quality = "LOSSLESS"
	case bitrate <= 128:
		quality = "LOW"
	case bitrate <= 320:
		quality = "HIGH"
	case bitrate >= 900:
		quality = "HI_RES_LOSSLESS"
	}

	// 2. Client IP
	clientIP := r.Header.Get("X-Forwarded-For")
	if clientIP == "" {
		clientIP = r.Header.Get("X-Real-IP")
	}
	if clientIP == "" {
		clientIP = r.RemoteAddr
	}
	if pos := strings.LastIndex(clientIP, ":"); pos != -1 {
		clientIP = clientIP[:pos]
	}
	clientIP = strings.Trim(clientIP, "[]")

	// 2a. Client name (for client-specific handling)
	clientName := p.GetOr("c", "")

	// 3 & 4. Fetch Stream URL and Track Info concurrently
	var url string
	var track *tidalproxy.TidalTrack
	var urlErr, trackErr error

	// [CRITICAL FIX] Include clientIP in cache key - Tidal returns IP-locked URLs!
	// Without this, different users share invalid URLs causing 403 errors
	cacheKey := fmt.Sprintf("stream:%d:%s:%s", trackID, quality, clientIP)

	var wg sync.WaitGroup
	wg.Add(2)

	// Fetch/Get Stream URL
	go func() {
		defer wg.Done()
		if cached := c.streamURLCache.Get(cacheKey); cached != "" {
			url = cached
			return
		}

		// deduplication: only one request fetches, others wait
		type lockPair struct {
			done chan struct{}
			url  string
			err  error
		}
		lockVal, loaded := c.streamURLLocks.LoadOrStore(cacheKey, &lockPair{done: make(chan struct{})})
		lp := lockVal.(*lockPair)

		if loaded {
			<-lp.done
			url, urlErr = lp.url, lp.err
		} else {
			url, urlErr = c.proxy.GetStreamURL(ctx, trackID, quality, clientIP)
			lp.url, lp.err = url, urlErr
			close(lp.done)
			if urlErr == nil && url != "" {
				c.streamURLCache.Set(cacheKey, url, 5*time.Minute) // Reduced to 5m, URLs are IP-locked
			}
			go func() {
				time.Sleep(100 * time.Millisecond)
				c.streamURLLocks.Delete(cacheKey)
			}()
		}
	}()

	// Fetch Track Info
	go func() {
		defer wg.Done()
		track, trackErr = c.proxy.GetTrackInfo(ctx, trackID)
	}()

	wg.Wait()

	if urlErr != nil {
		return nil, urlErr
	}
	if trackErr != nil {
		return nil, trackErr
	}

	// 5. Detection
	isHLS := strings.Contains(url, ".m3u8") || strings.Contains(url, "manifestType=HLS")
	isDASH := strings.Contains(url, ".mpd") || strings.Contains(url, "manifestType=MPEG_DASH")
	ext := "flac"
	if strings.Contains(url, ".m4a") || quality == "HIGH" || quality == "LOW" {
		ext = "m4a"
	}

	return &streamRequest{
		Quality:    quality,
		ClientIP:   clientIP,
		StreamURL:  url,
		Track:      track,
		Ext:        ext,
		IsHLS:      isHLS,
		IsDASH:     isDASH,
		ClientName: clientName,
	}, nil
}

// batchFetchAlbumsWithContext fetches metadata with custom context (for timeout control)
func (c *Controller) batchFetchAlbumsWithContext(ctx context.Context, tidalIDs []int) []*spec.Album {
	return batchFetch(ctx, c.proxySem, tidalIDs, c.proxy.GetAlbumMetadata, func(info *tidalproxy.TidalAlbum, _ int) *spec.Album {
		return spec.NewAlbumFromTidal(info)
	})
}

// batchFetchArtists fetches metadata for multiple tidal artist IDs concurrently
func (c *Controller) batchFetchArtists(r *http.Request, tidalIDs []int) []*spec.Artist {
	user := r.Context().Value(CtxUser).(*db.User)
	return batchFetch(r.Context(), c.proxySem, tidalIDs, func(ctx context.Context, id int) (*tidalproxy.TidalArtistDetail, error) {
		return c.proxy.GetArtistInfo(ctx, id)
	}, func(info *tidalproxy.TidalArtistDetail, tid int) *spec.Artist {
		a := spec.NewArtistFromTidal(&info.Artist)
		a.AlbumCount = c.proxy.GetArtistAlbumCount(r.Context(), tid)
		c.applyArtistStar(user.ID, a)
		return a
	})
}

// scrobbleTrackFromTidal converts a TidalTrack to a scrobble.Track
func scrobbleTrackFromTidal(t *tidalproxy.TidalTrack) scrobble.Track {
	artist := t.Artist.Name
	if len(t.Artists) > 0 {
		artist = t.Artists[0].Name
	}
	return scrobble.Track{
		Track:       t.Title,
		Artist:      artist,
		Album:       t.Album.Title,
		AlbumArtist: artist,
		TrackNumber: uint(t.TrackNumber),
		Duration:    time.Duration(t.Duration) * time.Second,
	}
}

// parseURIList parses a JSON array string of URIs
func parseURIList(jsonStr string) []string {
	var uris []string
	_ = json.Unmarshal([]byte(jsonStr), &uris)
	return uris
}

// encodeURIs encodes a slice of URIs to JSON
func encodeURIs(uris []string) string {
	if len(uris) == 0 {
		return "[]"
	}
	data, _ := json.Marshal(uris)
	return string(data)
}

// extractIDsFromURIs extracts numeric IDs from a slice of URIs (e.g., ["td:tr:123"] -> [123])
func extractIDsFromURIs(uris []string) []int {
	ids := make([]int, 0, len(uris))
	for _, uri := range uris {
		id := extractIDFromURI(uri)
		if id > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

// extractIDFromURI extracts the numeric ID from a URI string (e.g., "td:tr:12345" -> 12345)
func extractIDFromURI(uri string) int {
	if uri == "" {
		return 0
	}
	parts := strings.Split(uri, ":")
	if len(parts) >= 3 {
		id, _ := strconv.Atoi(parts[2])
		return id
	}
	return 0
}

func (c *Controller) hydrateTrackBackground(trackID int) {
	key := fmt.Sprintf("tr:%d", trackID)
	if c.hydratedCache.Has(key) {
		return // Already hydrated recently
	}
	c.hydratedCache.Set(key, true, 0) // Mark as hydrated with default TTL (24h)

	go func() {
		time.Sleep(50 * time.Millisecond) // yield
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c.proxy.GetTrackInfo(ctx, trackID)
	}()
}

func (c *Controller) hydrateAlbumBackground(albumID int) {
	key := fmt.Sprintf("al:%d", albumID)
	if c.hydratedCache.Has(key) {
		return // Already hydrated recently
	}
	c.hydratedCache.Set(key, true, 0)

	go func() {
		time.Sleep(50 * time.Millisecond) // yield
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		log.Printf("[HYDRATE] Deep hydrating album %d...", albumID)
		c.proxy.GetAlbumInfo(ctx, albumID) // GetAlbumInfo triggers caching of all its tracks
	}()
}

func (c *Controller) hydrateArtistBackground(artistID int) {
	key := fmt.Sprintf("ar:%d", artistID)
	if c.hydratedCache.Has(key) {
		return // Already hydrated recently
	}
	c.hydratedCache.Set(key, true, 0)

	go func() {
		time.Sleep(50 * time.Millisecond) // yield
		// This might take a while if the artist has many albums
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		log.Printf("[HYDRATE] Deep hydrating artist %d...", artistID)

		// 1. Get Artist Info
		c.proxy.GetArtistInfo(ctx, artistID)

		// 2. Hydrate Top Tracks
		c.proxy.GetArtistTopTracks(ctx, artistID, 50)

		// 3. Get Albums list (shallow)
		page, err := c.proxy.GetArtistAlbums(ctx, artistID, true)
		if err != nil || page == nil {
			log.Printf("[HYDRATE] Failed to get albums for artist %d: %v", artistID, err)
			return
		}

		// 4. Hydrate each album sequentially to avoid hammering proxy
		for i, a := range page.Albums.Items {
			if a.ID == 0 {
				continue
			}

			// Sleep slightly between fetches
			if i > 0 {
				time.Sleep(300 * time.Millisecond)
			}

			// Fetching album info automatically caches the album AND all its tracks via cacheAlbumJSON
			_, err := c.proxy.GetAlbumInfo(ctx, a.ID)
			if err != nil {
				log.Printf("[HYDRATE] Failed to hydrate album %d for artist %d: %v", a.ID, artistID, err)
			}
		}

		log.Printf("[HYDRATE] Completed deep hydration for artist %d (%d albums)", artistID, len(page.Albums.Items))
	}()
}
