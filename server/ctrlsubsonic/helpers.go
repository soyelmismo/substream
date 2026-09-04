package ctrlsubsonic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
	"go.senan.xyz/gonic/provider/tidal"
)

// streamRequest bundle for unified stream preparation
type streamRequest struct {
	Quality    string
	ClientIP   string
	StreamURL  string
	Track      *tidal.TidalTrack
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
	uris := make([]string, len(tidalIDs))
	for i, id := range tidalIDs {
		uris[i] = fmt.Sprintf("td:tr:%d", id)
	}
	return c.batchFetchTracksByURIs(r, uris)
}

// batchFetchTracksByURIs fetches metadata for multiple track URIs across all providers concurrently
func (c *Controller) batchFetchTracksByURIs(r *http.Request, uris []string) []*spec.TrackChild {
	user := r.Context().Value(CtxUser).(*db.User)
	tracks := c.providers.BatchGetTracksByURI(r.Context(), uris)
	for _, tc := range tracks {
		if tc != nil {
			c.applyTrackStar(user.ID, tc)
			c.applyTrackPlayCount(user.ID, tc)
			if tc.ID != nil {
				tc.UserRating = c.getTrackRating(user.ID, tc.ID.String())
			}
		}
	}
	return tracks
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
			a := NewAlbumFromTidal(info)
			c.applyAlbumStar(user.ID, a)
			a.UserRating = c.getAlbumRating(user.ID, fmt.Sprintf("td:al:%d", id))
			results = append(results, a)
		}
	}

	return results
}

// prepareStream centralizes the logic for preparing a stream (quality, IP, URL, meta)
// shared by ServeStream and ServeDownload
func (c *Controller) prepareStream(ctx context.Context, r *http.Request, id *specid.ID) (*streamRequest, error) {
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

	// 2b. Quality Upgrade / Provider Availability Check via track_sources
	if c.dbc != nil && c.providers != nil {
		if bestSource, _ := db.GetBestStreamableSource(c.dbc.DB, id.String(), c.providers.IsAudioEnabled); bestSource != nil && bestSource.URI != id.String() {
			if altID, err := specid.New(bestSource.URI); err == nil {
				if altProv := c.getProvider(altID.Provider()); altProv != nil {
					log.Printf("[STREAM] ⚡ Auto-upgrade/redirect: %s -> %s (Quality Tier: %d)",
						id.String(), bestSource.URI, bestSource.QualityTier)
					id = &altID
				}
			}
		}
	}

	prov := c.getProvider(id.Provider())
	if prov == nil {
		return nil, fmt.Errorf("provider %q not found", id.Provider())
	}

	// 3 & 4. Fetch Stream URL and Track Info concurrently
	var url string
	var track *tidal.TidalTrack
	var urlErr, trackErr error

	// [CRITICAL FIX] Include clientIP in cache key - Tidal returns IP-locked URLs!
	// Without this, different users share invalid URLs causing 403 errors
	cacheKey := fmt.Sprintf("stream:%s:%s:%s", id.String(), quality, clientIP)

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
		lockVal, loaded := c.streamURLLocks.LoadOrStore(cacheKey+":lock", &lockPair{done: make(chan struct{})})
		lp := lockVal.(*lockPair)

		if loaded {
			<-lp.done
			url, urlErr = lp.url, lp.err
		} else {
			if !c.providers.IsAudioEnabled(prov.ID()) {
				log.Printf("[STREAM] Audio disabled for provider %s, attempting automatic fallback...", prov.ID())
				fallbackProvID := prov.ID()
				if t, err := prov.GetTrack(ctx, id.RawID()); err == nil && t != nil {
					if len(t.ISRC) > 0 && t.ISRC[0] != "" {
						if fbURL, _, _, fbErr := c.providers.ResolveStreamByISRC(ctx, t.ISRC[0], quality, clientIP, fallbackProvID); fbErr == nil && fbURL != "" {
							url = fbURL
						}
					}
					if url == "" && c.providers.IsAudioEnabled("sp") && fallbackProvID != "sp" {
						if spProv, err := c.providers.Get("sp"); err == nil && spProv != nil {
							if matcher, ok := spProv.(interface {
								MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error)
							}); ok {
								if matched, mErr := matcher.MatchTrackByMetadata(ctx, t.Artist, t.Title, t.Duration); mErr == nil && matched != nil && matched.ID != nil {
									url, urlErr = spProv.GetStreamURL(ctx, matched.ID.RawID(), quality, clientIP)
								}
							}
						}
					}
					if url == "" && c.providers.IsAudioEnabled("yt") && fallbackProvID != "yt" {
						if ytProv, err := c.providers.Get("yt"); err == nil && ytProv != nil {
							if matcher, ok := ytProv.(interface {
								MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error)
							}); ok {
								if matched, mErr := matcher.MatchTrackByMetadata(ctx, t.Artist, t.Title, t.Duration); mErr == nil && matched != nil && matched.ID != nil {
									url, urlErr = ytProv.GetStreamURL(ctx, matched.ID.RawID(), quality, clientIP)
								}
							}
						}
					}
				}
				if url == "" && urlErr == nil {
					urlErr = fmt.Errorf("provider %s audio streaming is disabled by administrator and no fallback succeeded", prov.Name())
				}
			} else {
				url, urlErr = prov.GetStreamURL(ctx, id.RawID(), quality, clientIP)
			}
			lp.url, lp.err = url, urlErr
			close(lp.done)
			if urlErr == nil && url != "" {
				c.streamURLCache.Set(cacheKey, url, 5*time.Minute) // Manifests expire quickly
			}
			go func() {
				time.Sleep(100 * time.Millisecond)
				c.streamURLLocks.Delete(cacheKey + ":lock")
			}()
		}
	}()

	// Fetch Track Info
	go func() {
		defer wg.Done()
		if id.Provider() == "td" {
			track, trackErr = c.proxy.GetTrackInfo(ctx, id.Value())
		} else {
			specTrack, err := prov.GetTrack(ctx, id.RawID())
			if err == nil && specTrack != nil {
				albumIDVal := 0
				if specTrack.AlbumID != nil {
					albumIDVal = specTrack.AlbumID.Value()
				}
				artistIDVal := 0
				if specTrack.ArtistID != nil {
					artistIDVal = specTrack.ArtistID.Value()
				}
				isrcVal := ""
				if len(specTrack.ISRC) > 0 {
					isrcVal = specTrack.ISRC[0]
				}
				track = &tidal.TidalTrack{
					ID:       id.Value(),
					Title:    specTrack.Title,
					Duration: specTrack.Duration,
					ISRC:     isrcVal,
					Album: tidal.TidalAlbumRef{
						ID:    albumIDVal,
						Title: specTrack.Album,
					},
					Artist: tidal.TidalArtist{
						ID:   artistIDVal,
						Name: specTrack.Artist,
					},
				}
			} else {
				trackErr = err
			}
		}
	}()

	wg.Wait()

	// Cross-Provider Fallback:
	// If primary provider stream failed or returned a preview sample for a longer track:
	isSamplePreview := strings.Contains(url, "range=20-30") || strings.Contains(url, "range=0-30") || strings.Contains(url, "preview")
	needsFallback := urlErr != nil || url == "" || (isSamplePreview && track != nil && track.Duration > 45)

	// 1. Rescuing via mapped track_sources in local DB (0 ms fast path)
	if needsFallback && c.dbc != nil {
		if altSources, err := db.GetAlternativeSources(c.dbc.DB, id.String()); err == nil {
			for _, alt := range altSources {
				if alt.URI == id.String() || (c.providers != nil && !c.providers.IsAudioEnabled(alt.Provider)) {
					continue
				}
				if altID, err := specid.New(alt.URI); err == nil {
					// Guard against malformed/corrupted track sources (e.g. ISRC saved as YouTube videoID)
					if altID.Provider() == "yt" && len(altID.RawID()) != 11 {
						log.Printf("[STREAM] ⚠️ Skipping malformed YouTube track_source %s", alt.URI)
						continue
					}
					if altProv := c.getProvider(altID.Provider()); altProv != nil {
						log.Printf("[STREAM] 🔄 Rescuing stream via mapped track_source %s (Quality: %d)", alt.URI, alt.QualityTier)
						if altURL, altErr := altProv.GetStreamURL(ctx, altID.RawID(), quality, clientIP); altErr == nil && altURL != "" {
							altIsPreview := strings.Contains(altURL, "range=20-30") || strings.Contains(altURL, "range=0-30") || strings.Contains(altURL, "preview")
							if !altIsPreview {
								url = altURL
								urlErr = nil
								isSamplePreview = false
								needsFallback = false
								break
							}
						}
					}
				}
			}
		}
	}

	// 2. Cross-Provider ISRC Fallback across registered providers
	if needsFallback && track != nil && track.ISRC != "" && c.providers != nil {
		log.Printf("[STREAM] Triggering cross-provider ISRC fallback for %s (ISRC: %s)", id.String(), track.ISRC)
		if fallbackURL, fallbackProv, fallbackID, err := c.providers.ResolveStreamByISRC(ctx, track.ISRC, quality, clientIP, id.Provider()); err == nil && fallbackURL != "" {
			fallbackIsPreview := strings.Contains(fallbackURL, "range=20-30") || strings.Contains(fallbackURL, "range=0-30") || strings.Contains(fallbackURL, "preview")
			if !fallbackIsPreview {
				log.Printf("[STREAM] ISRC RESOLUTION: Resolved %s (ISRC %s) via %s", id.String(), track.ISRC, fallbackProv.Name())
				url = fallbackURL
				urlErr = nil
				isSamplePreview = false
				needsFallback = false

				// Memorize cross-provider link in track_sources with actual resolved track ID
				if c.dbc != nil && track != nil && fallbackID != nil {
					clusterKey := db.MakeClusterKey(track.ISRC, track.Artist.Name, track.Title)
					_ = db.SaveTrackSources(c.dbc.DB, clusterKey,
						db.TrackSource{ClusterKey: clusterKey, Provider: id.Provider(), URI: id.String(), QualityTier: db.QualityTierLossless, IsAvailable: true},
						db.TrackSource{ClusterKey: clusterKey, Provider: fallbackProv.ID(), URI: fallbackID.String(), QualityTier: db.QualityTierLossless, IsAvailable: true},
					)
				}
			}
		} else {
			log.Printf("[STREAM] Cross-provider ISRC fallback failed for %s (ISRC: %s): %v", id.String(), track.ISRC, err)
		}
	}

	// 3. Universal Fallback to Spotify if stream still failed or missing
	if (urlErr != nil || url == "" || isSamplePreview) && c.providers != nil && c.providers.IsAudioEnabled("sp") && track != nil && id.Provider() != "sp" {
		if spProv, err := c.providers.Get("sp"); err == nil && spProv != nil {
			if matcher, ok := spProv.(interface {
				MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error)
			}); ok {
				if matched, mErr := matcher.MatchTrackByMetadata(ctx, track.Artist.Name, track.Title, track.Duration); mErr == nil && matched != nil && matched.ID != nil {
					log.Printf("[STREAM] 🚀 Universal fallback to Spotify for %s (%s - %s) -> %s", id.String(), track.Artist.Name, track.Title, matched.ID.RawID())
					if spURL, spErr := spProv.GetStreamURL(ctx, matched.ID.RawID(), quality, clientIP); spErr == nil && spURL != "" {
						spIsPreview := strings.Contains(spURL, "range=20-30") || strings.Contains(spURL, "range=0-30") || strings.Contains(spURL, "preview")
						if !spIsPreview {
							url = spURL
							urlErr = nil
							isSamplePreview = false

							// Memorize cross-provider link in track_sources
							if c.dbc != nil && track != nil {
								clusterKey := db.MakeClusterKey(track.ISRC, track.Artist.Name, track.Title)
								spURI := fmt.Sprintf("sp:tr:%s", matched.ID.RawID())
								_ = db.SaveTrackSources(c.dbc.DB, clusterKey,
									db.TrackSource{ClusterKey: clusterKey, Provider: id.Provider(), URI: id.String(), QualityTier: db.QualityTierLossless, IsAvailable: true},
									db.TrackSource{ClusterKey: clusterKey, Provider: "sp", URI: spURI, QualityTier: db.QualityTierStandard, IsAvailable: true},
								)
							}
						}
					}
				}
			}
		}
	}

	// 4. Universal Fallback to YouTube Music if stream still failed or missing
	if (urlErr != nil || url == "" || isSamplePreview) && c.providers != nil && c.providers.IsAudioEnabled("yt") && track != nil {
		if ytProv, err := c.providers.Get("yt"); err == nil && ytProv != nil {
			if matcher, ok := ytProv.(interface {
				MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error)
			}); ok {
				if matched, mErr := matcher.MatchTrackByMetadata(ctx, track.Artist.Name, track.Title, track.Duration); mErr == nil && matched != nil && matched.ID != nil {
					log.Printf("[STREAM] 🚀 Universal fallback to YouTube Music for %s (%s - %s) -> %s", id.String(), track.Artist.Name, track.Title, matched.ID.RawID())
					if ytURL, ytErr := ytProv.GetStreamURL(ctx, matched.ID.RawID(), quality, clientIP); ytErr == nil && ytURL != "" {
						url = ytURL
						urlErr = nil
						isSamplePreview = false

						// Memorize cross-provider link in track_sources
						if c.dbc != nil && track != nil {
							clusterKey := db.MakeClusterKey(track.ISRC, track.Artist.Name, track.Title)
							ytURI := fmt.Sprintf("yt:tr:%s", matched.ID.RawID())
							_ = db.SaveTrackSources(c.dbc.DB, clusterKey,
								db.TrackSource{ClusterKey: clusterKey, Provider: id.Provider(), URI: id.String(), QualityTier: db.QualityTierLossless, IsAvailable: true},
								db.TrackSource{ClusterKey: clusterKey, Provider: "yt", URI: ytURI, QualityTier: db.QualityTierStandard, IsAvailable: true},
							)
						}
					}
				}
			}
		}
	}

	// 5. Universal Fallback to SoundCloud if stream still failed or missing
	if (urlErr != nil || url == "" || isSamplePreview) && c.providers != nil && c.providers.IsAudioEnabled("sc") && track != nil {
		if scProv, err := c.providers.Get("sc"); err == nil && scProv != nil {
			if matcher, ok := scProv.(interface {
				MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error)
			}); ok {
				if matched, mErr := matcher.MatchTrackByMetadata(ctx, track.Artist.Name, track.Title, track.Duration); mErr == nil && matched != nil && matched.ID != nil {
					log.Printf("[STREAM] 🚀 Universal fallback to SoundCloud for %s (%s - %s) -> %s", id.String(), track.Artist.Name, track.Title, matched.ID.RawID())
					if scURL, scErr := scProv.GetStreamURL(ctx, matched.ID.RawID(), quality, clientIP); scErr == nil && scURL != "" {
						url = scURL
						urlErr = nil
						isSamplePreview = false

						// Memorize cross-provider link in track_sources
						if c.dbc != nil && track != nil {
							clusterKey := db.MakeClusterKey(track.ISRC, track.Artist.Name, track.Title)
							scURI := fmt.Sprintf("sc:tr:%s", matched.ID.RawID())
							_ = db.SaveTrackSources(c.dbc.DB, clusterKey,
								db.TrackSource{ClusterKey: clusterKey, Provider: id.Provider(), URI: id.String(), QualityTier: db.QualityTierLossless, IsAvailable: true},
								db.TrackSource{ClusterKey: clusterKey, Provider: "sc", URI: scURI, QualityTier: db.QualityTierStandard, IsAvailable: true},
							)
						}
					}
				}
			}
		}
	}

	// Strictly reject previews - do not serve 30-second clips for full songs
	if isSamplePreview && track != nil && track.Duration > 45 {
		return nil, fmt.Errorf("track requires subscription: 30-second preview rejected")
	}

	if urlErr != nil {
		return nil, urlErr
	}
	if trackErr != nil {
		return nil, trackErr
	}

	if url != "" {
		c.streamURLCache.Set(cacheKey, url, 5*time.Minute)
	}

	// 5. Detection
	isHLS := strings.Contains(url, ".m3u8") || strings.Contains(url, "manifestType=HLS") || strings.HasPrefix(url, "data:")
	isDASH := strings.Contains(url, ".mpd") || strings.Contains(url, "manifestType=MPEG_DASH")
	ext := "flac"
	if strings.Contains(url, ".m4a") || strings.Contains(url, ".mp4") || strings.Contains(url, "mime=audio%2Fmp4") || strings.Contains(url, "mime=audio/mp4") {
		ext = "m4a"
	} else if strings.Contains(url, ".mp3") || strings.Contains(url, "mime=audio%2Fmpeg") {
		ext = "mp3"
	} else if strings.Contains(url, ".webm") || strings.Contains(url, "mime=audio%2Fwebm") || strings.Contains(url, "mime=audio/webm") {
		ext = "webm"
	} else if strings.Contains(url, ".opus") || strings.Contains(url, ".ogg") || strings.Contains(url, "mime=audio%2Fogg") {
		ext = "opus"
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
	return batchFetch(ctx, c.proxySem, tidalIDs, c.proxy.GetAlbumMetadata, func(info *tidal.TidalAlbum, _ int) *spec.Album {
		return NewAlbumFromTidal(info)
	})
}

// batchFetchArtists fetches metadata for multiple tidal artist IDs concurrently
func (c *Controller) batchFetchArtists(r *http.Request, tidalIDs []int) []*spec.Artist {
	user := r.Context().Value(CtxUser).(*db.User)
	return batchFetch(r.Context(), c.proxySem, tidalIDs, func(ctx context.Context, id int) (*tidal.TidalArtistDetail, error) {
		return c.proxy.GetArtistInfo(ctx, id)
	}, func(info *tidal.TidalArtistDetail, tid int) *spec.Artist {
		a := NewArtistFromTidal(&info.Artist)
		a.AlbumCount = c.proxy.GetArtistAlbumCount(r.Context(), tid)
		c.applyArtistStar(user.ID, a)
		return a
	})
}

// scrobbleTrackFromTidal converts a TidalTrack to a scrobble.Track
func scrobbleTrackFromTidal(t *tidal.TidalTrack) scrobble.Track {
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

// scrobbleTrackFromSubsonic converts a generic provider TrackChild to a scrobble.Track
func scrobbleTrackFromSubsonic(t *spec.TrackChild) scrobble.Track {
	return scrobble.Track{
		Track:       t.Title,
		Artist:      t.Artist,
		Album:       t.Album,
		AlbumArtist: t.Artist,
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
		// Enviar al fondo de la piscina (protege a los proxies de streaming)
		ctx = tidal.WithPriority(ctx, tidal.PriorityBackground)
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
		// Enviar al fondo de la piscina (protege a los proxies de streaming)
		ctx = tidal.WithPriority(ctx, tidal.PriorityBackground)
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

		// Enviar al fondo de la piscina (protege a los proxies de streaming)
		ctx = tidal.WithPriority(ctx, tidal.PriorityBackground)

		log.Printf("[HYDRATE] Deep hydrating artist %d... [Priority:Background]", artistID)

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

// hydratePlaylistBackground fetches and caches all tracks in a playlist
// This ensures playlist tracks are always available in the cache for offline playback
func (c *Controller) hydratePlaylistBackground(playlistID int, trackIDs []int) {
	key := fmt.Sprintf("pl:%d", playlistID)
	if c.hydratedCache.Has(key) {
		return // Already hydrated recently
	}
	c.hydratedCache.Set(key, true, 0)

	if len(trackIDs) == 0 {
		return
	}

	go func() {
		time.Sleep(50 * time.Millisecond) // yield
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		// Enviar al fondo de la piscina (protege a los proxies de streaming)
		ctx = tidal.WithPriority(ctx, tidal.PriorityBackground)

		log.Printf("[HYDRATE] Hydrating playlist %d with %d tracks...", playlistID, len(trackIDs))

		hydrated := 0
		for i, trackID := range trackIDs {
			if trackID == 0 {
				continue
			}

			// Sleep between fetches to avoid hammering
			if i > 0 {
				time.Sleep(100 * time.Millisecond)
			}

			// Fetch track info - this caches the track metadata
			_, err := c.proxy.GetTrackInfo(ctx, trackID)
			if err != nil {
				// Non-critical: track may be unavailable, continue with others
				continue
			}
			hydrated++
		}

		log.Printf("[HYDRATE] Completed playlist %d hydration (%d/%d tracks cached)", playlistID, hydrated, len(trackIDs))
	}()
}

// unwrapTidalManifest recursively extracts the underlying progressive media URL from HLS/DASH manifests.
// By doing this, we bypass the need to serve M3U8 files to the client or do server-side stitching,
// achieving zero-bandwidth gapless playback directly from Tidal's CDN.
func (c *Controller) unwrapTidalManifest(ctx context.Context, manifestURL string, clientIP string, depth int) string {
	// Prevent infinite recursion in malicious or broken manifests
	if depth > 3 {
		log.Printf("[UNWRAP] Max recursion depth reached for %s", manifestURL)
		return ""
	}

	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", manifestURL, nil)
	if err != nil {
		return ""
	}
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
		req.Header.Set("X-Real-IP", clientIP)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := c.streamClient.Do(req)
	if err != nil {
		log.Printf("[UNWRAP] Network error fetching manifest depth %d: %v", depth, err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[UNWRAP] HTTP %d fetching manifest depth %d", resp.StatusCode, depth)
		return ""
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	content := string(bodyBytes)

	// Robust URL resolution using net/url (handles absolute, relative, and root paths natively)
	baseURL, err := url.Parse(manifestURL)
	if err != nil {
		return ""
	}
	resolveURL := func(ref string) string {
		if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
			return ref
		}
		refURL, err := url.Parse(ref)
		if err != nil {
			return ref
		}
		return baseURL.ResolveReference(refURL).String()
	}

	// 1. DASH Bypass
	if strings.Contains(manifestURL, ".mpd") || strings.Contains(manifestURL, "MPEG_DASH") || strings.Contains(content, "<MPD") {
		if start := strings.Index(content, "<BaseURL>"); start != -1 {
			start += 9
			if end := strings.Index(content[start:], "</BaseURL>"); end != -1 {
				uri := content[start : start+end]
				uri = strings.ReplaceAll(uri, "&amp;", "&")
				resolved := resolveURL(uri)
				log.Printf("[UNWRAP] Extracted DASH BaseURL: %s", resolved)
				return resolved
			}
		}
		return ""
	}

	// 2. HLS Bypass
	lines := strings.Split(content, "\n")
	isMaster := false
	for _, line := range lines {
		if strings.HasPrefix(line, "#EXT-X-STREAM-INF") {
			isMaster = true
			break
		}
	}

	// If Master Playlist, extract the variant and recursively unwrap it
	if isMaster {
		for i, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "#EXT-X-STREAM-INF") {
				// The next non-empty, non-comment line is our variant URI
				for j := i + 1; j < len(lines); j++ {
					variantLine := strings.TrimSpace(lines[j])
					if variantLine != "" && !strings.HasPrefix(variantLine, "#") {
						resolved := resolveURL(variantLine)
						log.Printf("[UNWRAP] Recursing into HLS variant: %s", resolved)
						return c.unwrapTidalManifest(ctx, resolved, clientIP, depth+1)
					}
				}
			}
		}
		return ""
	}

	// If Media Playlist, ensure it's a single-file byterange manifest.
	// If it doesn't have BYTERANGE, it means it's a true multi-segment HLS.
	// We CANNOT unwrap true multi-segment files, so we abort and return empty.
	if !strings.Contains(content, "BYTERANGE") {
		log.Printf("[UNWRAP] HLS is multi-segment (no BYTERANGE). Aborting unwrap.")
		return ""
	}

	// Extract MAP URI (Contains the fMP4 Headers + Audio Data)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#EXT-X-MAP:URI=") {
			// Extract exactly what's inside the quotes: #EXT-X-MAP:URI="url"
			parts := strings.SplitN(strings.TrimPrefix(line, "#EXT-X-MAP:URI="), ",", 2)
			uri := strings.Trim(parts[0], "\"")
			resolved := resolveURL(uri)
			log.Printf("[UNWRAP] Extracted HLS MAP URI: %s", resolved)
			return resolved
		}
	}

	// Fallback: If no MAP exists but BYTERANGE is present, return the first segment URI.
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			resolved := resolveURL(line)
			log.Printf("[UNWRAP] Extracted HLS first segment URI: %s", resolved)
			return resolved
		}
	}

	return ""
}
