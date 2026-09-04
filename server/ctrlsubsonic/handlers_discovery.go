package ctrlsubsonic

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/recommendations"
	"go.senan.xyz/gonic/provider/tidal"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetRandomSongs(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)
	size := p.GetOrInt("size", 10)
	genre, _ := p.Get("genre")

	var uris []string

	// If genre specified, try to get genre tracks from hot.monochrome.tf
	if genre != "" {
		log.Printf("[RANDOM] Genre requested: %s", genre)
		tidalIDs := c.fetchHotGenreTracks(r.Context(), genre, size)
		if len(tidalIDs) > 0 {
			for _, id := range tidalIDs {
				uris = append(uris, fmt.Sprintf("td:tr:%d", id))
			}
			tracks := c.batchFetchTracksByURIs(r, uris)
			sub := spec.NewResponse()
			sub.RandomTracks = &spec.RandomTracks{List: tracks}
			return sub
		}
		log.Printf("[RANDOM] No tracks found for genre %s, falling back to random", genre)
	}

	// 1. Get some from starred (50%)
	favSize := size / 2
	if favSize < 1 {
		favSize = 1
	}

	var stars []db.TrackStar
	c.dbc.Where("user_id=?", user.ID).Order("RANDOM()").Limit(favSize).Find(&stars)

	for _, s := range stars {
		if s.URI != "" {
			uris = appendUniqueString(uris, s.URI)
		}
	}

	// 2. Get some from Discovery (Tidal Top Tracks)
	discoverySize := size - len(uris)
	if discoverySize > 0 {
		top, err := c.proxy.GetTopTracks(r.Context(), discoverySize+randomSongsDiscoveryExtra)
		if err == nil {
			for _, t := range top {
				if len(uris) >= size {
					break
				}
				uris = appendUniqueString(uris, fmt.Sprintf("td:tr:%d", t.ID))
			}
		}
	}

	tracks := c.batchFetchTracksByURIs(r, uris)

	sub := spec.NewResponse()
	sub.RandomTracks = &spec.RandomTracks{List: tracks}
	return sub
}

// fetchHotGenreTracks fetches tracks for a specific genre from hot.monochrome.tf.
// It first checks the cache for existing results, then fetches from the API if needed.
// The function tries to extract track IDs directly from sections, or falls back to
// extracting them from album data. Results are cached with a TTL to reduce API calls.
func (c *Controller) fetchHotGenreTracks(ctx context.Context, genre string, limit int) []int {
	// Validate and cap limit to prevent excessive requests
	const maxFetchLimit = 100
	if limit <= 0 {
		return nil
	}
	if limit > maxFetchLimit {
		limit = maxFetchLimit
		log.Printf("[RANDOM] Limit capped to %d for genre %s", maxFetchLimit, genre)
	}

	// Check cache first
	cacheKey := strings.ToLower(genre)
	if cached := c.genreCache.Get(cacheKey); len(cached) > 0 {
		log.Printf("[RANDOM] Cache hit for genre %s", genre)
		// Return cached tracks (up to limit)
		if len(cached) > limit {
			return cached[:limit]
		}
		return cached
	}

	// Deduplication: only one request fetches, others wait
	type hotTrackLock struct {
		done     chan struct{}
		trackIDs []int
	}
	lockVal, loaded := c.hotLocks.LoadOrStore(cacheKey, &hotTrackLock{done: make(chan struct{})})
	lp := lockVal.(*hotTrackLock)

	if loaded {
		// Another request is in flight, wait for it
		<-lp.done
		cached := c.genreCache.Get(cacheKey)
		if len(cached) > 0 {
			if len(cached) > limit {
				return cached[:limit]
			}
			return cached
		}
		// If cache is empty but we have the result from the lock, use it
		return lp.trackIDs
	}

	// Map genre name to hot.monochrome.tf format
	hotGenre, ok := hotGenreMapping[genre]
	if !ok {
		hotGenre = strings.ToLower(genre)
	}

	url := fmt.Sprintf("%s/explore/genre/?id=%s", hotMonochromeURL, hotGenre)
	log.Printf("[RANDOM] Fetching genre tracks from: %s", url)

	var result hotResponse
	if err := fetchJSON(ctx, c.httpClient, url, "RANDOM", &result); err != nil {
		log.Printf("[RANDOM] Error fetching genre tracks from hot.monochrome.tf: %v", err)
		close(lp.done)
		go func() { time.Sleep(100 * time.Millisecond); c.hotLocks.Delete(cacheKey) }()
		return nil
	}

	var trackIDs []int
	var albumIDs []int

	// Priority 1: Extract from top_tracks (direct tracks)
	for _, track := range result.TopTracks {
		if track.ID != 0 {
			trackIDs = append(trackIDs, track.ID)
			if len(trackIDs) >= limit {
				break
			}
		}
	}

	// Priority 2: Extract from new_releases (albums)
	if len(trackIDs) < limit {
		for _, album := range result.NewReleases {
			if album.ID != 0 && album.StreamReady {
				albumIDs = appendUnique(albumIDs, album.ID)
			}
		}
	}

	// Priority 3: Extract from sections (ALBUM_LIST)
	if len(trackIDs) < limit {
		for _, section := range result.Sections {
			if section.Type == "ALBUM_LIST" {
				for _, item := range section.Items {
					if item.ID != 0 {
						albumIDs = appendUnique(albumIDs, item.ID)
					}
				}
			}
		}
	}

	// Extract tracks from albums (limit to max albums to avoid too many API calls)
	if len(trackIDs) == 0 && len(albumIDs) > 0 {
		maxAlbums := hotFetchMaxAlbums
		if maxAlbums > len(albumIDs) {
			maxAlbums = len(albumIDs)
		}
		for i := 0; i < maxAlbums; i++ {
			albumInfo, err := c.proxy.GetAlbumInfo(ctx, albumIDs[i])
			if err == nil && albumInfo != nil && len(albumInfo.Items) > 0 {
				// Add first few tracks from album
				for j, item := range albumInfo.Items {
					if j >= hotFetchMaxTracks || len(trackIDs) >= limit {
						break
					}
					trackIDs = appendUnique(trackIDs, item.ID)
				}
			}
		}
	}

	log.Printf("[RANDOM] Found %d tracks for genre %s", len(trackIDs), genre)

	// Cache the result with TTL
	if len(trackIDs) > 0 {
		c.genreCache.Set(cacheKey, trackIDs, 0) // Use default TTL
	}
	lp.trackIDs = trackIDs
	close(lp.done)

	// Cleanup lock after brief delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.hotLocks.Delete(cacheKey)
	}()

	return trackIDs
}

func (c *Controller) ServeGetSimilarSongsTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	count := p.GetOrInt("count", 10)
	if count > similarSongsMaxCount {
		count = similarSongsMaxCount
	}

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "provide an `id` parameter")
	}

	tracks := c.getSimilarSongsForID(r, id, count)
	if len(tracks) == 0 {
		return c.ServeGetRandomSongs(r)
	}

	sub := spec.NewResponse()
	sub.SimilarSongsTwo = &spec.SimilarSongsTwo{Tracks: tracks}
	return sub
}

// getSimilarSongsForID resolves similar tracks / radio recommendations across any provider
func (c *Controller) getSimilarSongsForID(r *http.Request, id specid.ID, count int) []*spec.TrackChild {
	user := r.Context().Value(CtxUser).(*db.User)

	// If the ID belongs to a non-Tidal provider (e.g. yt:tr:... or yt:ar:...)
	if id.Provider() != "" && id.Provider() != "td" {
		prov := c.getProvider(id.Provider())
		if prov != nil {
			trackRawID := ""
			switch id.Type() {
			case specid.Track:
				trackRawID = id.RawID()
			case specid.Artist:
				ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
				top, err := prov.GetArtistTopTracks(ctx, id.RawID(), 1)
				cancel()
				if err == nil && len(top) > 0 && top[0].ID != nil {
					trackRawID = top[0].ID.RawID()
				}
			}

			if trackRawID != "" {
				ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
				recs, err := prov.GetRecommendations(ctx, trackRawID)
				cancel()
				if err == nil && len(recs) > 0 {
					if count > 0 && len(recs) > count {
						recs = recs[:count]
					}
					for _, t := range recs {
						c.applyTrackStar(user.ID, t)
						c.applyTrackPlayCount(user.ID, t)
						if t.ID != nil {
							t.UserRating = c.getTrackRating(user.ID, t.ID.String())
						}
					}
					return recs
				}
			}
		}
	}

	// Tidal / fallback resolution
	var trackID int
	switch id.Type() {
	case specid.Track:
		trackID = id.Value()
	case specid.Artist:
		ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
		top, err := c.proxy.GetArtistTopTracks(ctx, id.Value(), 1)
		cancel()
		if err != nil || len(top) == 0 {
			return nil
		}
		trackID = top[0].ID
	default:
		return nil
	}

	if trackID <= 0 {
		return nil
	}

	return c.getSimilarSongsMerged(r, trackID, count)
}

// getSimilarSongsMerged returns merged recommendations from Tidal native and external providers.
// It handles concurrent resolution of external recommendations with proper caching.
func (c *Controller) getSimilarSongsMerged(r *http.Request, trackID, count int) []*spec.TrackChild {
	user := r.Context().Value(CtxUser).(*db.User)

	// 1. Get native recommendations from Tidal
	ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
	nativeRecs, _ := c.proxy.GetRecommendations(ctx, trackID)
	cancel()

	// 2. Get external recommendations from registered providers (Last.fm, etc.)
	var externalRecs []tidal.TidalTrack
	if c.recEngine.HasProviders() {
		if baseTrack, err := c.proxy.GetTrackInfo(r.Context(), trackID); err == nil {
			extCtx, extCancel := context.WithTimeout(r.Context(), 2500*time.Millisecond)
			recs, _ := c.recEngine.GetSimilarTracks(extCtx, user, recommendations.TrackRef{
				ID:      fmt.Sprintf("td:tr:%d", trackID),
				Title:   baseTrack.Title,
				Artist:  baseTrack.Artist.Name,
				ISRC:    baseTrack.ISRC,
				TidalID: trackID,
			}, count)
			extCancel()

			// Convert recommendations to TidalTracks
			for _, rec := range recs {
				if rec.Track != nil && rec.Track.TidalID != 0 {
					if t, err := c.proxy.GetTrackInfo(r.Context(), rec.Track.TidalID); err == nil {
						externalRecs = append(externalRecs, *t)
					}
				}
			}
		}
	}

	// 3. Merge native and external recommendations
	recs := mergeRecommendations(nativeRecs, externalRecs, count)

	if len(recs) == 0 {
		log.Printf("[DISC] GetRecommendations EMPTY for track %d", trackID)
		return nil
	}

	ids := make([]int, len(recs))
	for i := range recs {
		ids[i] = recs[i].ID
	}

	return c.batchFetchTracks(r, ids)
}

func (c *Controller) ServeGetSimilarSongs(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	count := p.GetOrInt("count", 10)
	if count > similarSongsMaxCount {
		count = similarSongsMaxCount
	}

	id, err := p.GetID("id")
	if err != nil {
		return c.ServeGetRandomSongs(r)
	}

	tracks := c.getSimilarSongsForID(r, id, count)
	if len(tracks) == 0 {
		return c.ServeGetRandomSongs(r)
	}

	sub := spec.NewResponse()
	sub.SimilarSongs = &spec.SimilarSongs{Tracks: tracks}
	return sub
}

func (c *Controller) ServeGetTopSongs(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)
	count := p.GetOrInt("count", 10)

	artistName, err := p.Get("artist")
	if err != nil {
		return spec.NewError(10, "provide an `artist` parameter")
	}

	// search artist by name to get ID
	// fetch more candidates to find exact match if Tidal search is fuzzy
	candidates, err := c.proxy.SearchArtists(r.Context(), artistName, topSongsSearchCandidates, 0)
	var artistID int
	if err == nil && len(candidates) > 0 {
		for _, a := range candidates {
			if strings.EqualFold(a.Name, artistName) {
				artistID = a.ID
				break
			}
		}

		// If no exact match, fallback to first ONLY if it's a very similar name
		if artistID == 0 {
			first := candidates[0]
			if strings.Contains(strings.ToLower(first.Name), strings.ToLower(artistName)) ||
				strings.Contains(strings.ToLower(artistName), strings.ToLower(first.Name)) {
				artistID = first.ID
			}
		}
	}

	if artistID > 0 {
		// get artist top tracks (precise)
		topTracks, err := c.proxy.GetArtistTopTracks(r.Context(), artistID, count)
		if err == nil && len(topTracks) > 0 {
			tracks := make([]*spec.TrackChild, len(topTracks))
			for i := range topTracks {
				tracks[i] = NewTrackFromTidal(&topTracks[i])
				c.applyTrackStar(user.ID, tracks[i])
				c.applyTrackPlayCount(user.ID, tracks[i])
			}

			sub := spec.NewResponse()
			sub.TopSongs = &spec.TopSongs{Tracks: tracks}
			return sub
		}
	}

	// Fallback to providers (e.g. YouTube Music)
	if c.providers != nil {
		for _, prov := range c.providers.All() {
			if prov.ID() == "td" {
				continue
			}
			artists, err := prov.SearchArtists(r.Context(), artistName, 5, 0)
			if err == nil && len(artists) > 0 {
				targetArtist := artists[0]
				for _, a := range artists {
					if strings.EqualFold(a.Name, artistName) {
						targetArtist = a
						break
					}
				}
				if targetArtist.ID != nil {
					top, err := prov.GetArtistTopTracks(r.Context(), targetArtist.ID.RawID(), count)
					if err == nil && len(top) > 0 {
						for _, t := range top {
							c.applyTrackStar(user.ID, t)
							c.applyTrackPlayCount(user.ID, t)
							if t.ID != nil {
								t.UserRating = c.getTrackRating(user.ID, t.ID.String())
							}
						}
						sub := spec.NewResponse()
						sub.TopSongs = &spec.TopSongs{Tracks: top}
						return sub
					}
				}
			}
		}
	}

	return spec.NewResponse()
}

// mergeRecommendations interleaves and deduplicates native and external recommendations
func mergeRecommendations(native []tidal.TidalTrack, external []tidal.TidalTrack, limit int) []tidal.TidalTrack {
	var merged []tidal.TidalTrack
	seenIDs := make(map[int]bool)
	seenISRCs := make(map[string]bool)

	add := func(t tidal.TidalTrack) {
		if len(merged) >= limit {
			return
		}
		if seenIDs[t.ID] {
			return
		}
		if t.ISRC != "" && seenISRCs[t.ISRC] {
			return
		}
		seenIDs[t.ID] = true
		if t.ISRC != "" {
			seenISRCs[t.ISRC] = true
		}
		merged = append(merged, t)
	}

	maxLen := len(native)
	if len(external) > maxLen {
		maxLen = len(external)
	}

	// Interleave: prioritize external (e.g., Last.fm usually has better similarities)
	for i := 0; i < maxLen; i++ {
		if i < len(external) {
			add(external[i])
		}
		if i < len(native) {
			add(native[i])
		}
	}

	return merged
}

func appendUniqueString(slice []string, val string) []string {
	for _, v := range slice {
		if v == val {
			return slice
		}
	}
	return append(slice, val)
}

