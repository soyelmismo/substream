package ctrlsubsonic

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"go.senan.xyz/gonic/db"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetRandomSongs(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)
	size := p.GetOrInt("size", 10)
	genre, _ := p.Get("genre")

	var tidalIDs []int

	// If genre specified, try to get genre tracks from hot.monochrome.tf
	if genre != "" {
		log.Printf("[RANDOM] Genre requested: %s", genre)
		tidalIDs = c.fetchHotGenreTracks(r.Context(), genre, size)
		if len(tidalIDs) > 0 {
			tracks := c.batchFetchTracks(r, tidalIDs)
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
		tidalIDs = appendUnique(tidalIDs, s.TidalID)
	}

	// 2. Get some from Discovery (Tidal Top Tracks)
	discoverySize := size - len(tidalIDs)
	if discoverySize > 0 {
		top, err := c.proxy.GetTopTracks(r.Context(), discoverySize+randomSongsDiscoveryExtra)
		if err == nil {
			for _, t := range top {
				if len(tidalIDs) >= size {
					break
				}
				tidalIDs = appendUnique(tidalIDs, t.ID)
			}
		}
	}

	tracks := c.batchFetchTracks(r, tidalIDs)

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

	var trackID int
	switch id.Type {
	case specid.Track:
		trackID = id.Value
	case specid.Artist:
		// fast timeout for artist
		ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
		page, err := c.proxy.GetArtistAlbums(ctx, id.Value, false)
		cancel()
		if err != nil || page == nil || len(page.Tracks) == 0 {
			return c.ServeGetRandomSongs(r)
		}
		trackID = page.Tracks[0].ID
	default:
		return c.ServeGetRandomSongs(r)
	}

	// fast timeout for recommendations
	ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
	recs, err := c.proxy.GetRecommendations(ctx, trackID)
	cancel()

	if err != nil || len(recs) == 0 {
		log.Printf("[DISC] GetSimilarSongs2 fallback to random (track %d)", trackID)
		return c.ServeGetRandomSongs(r)
	}

	if len(recs) > count {
		recs = recs[:count]
	}

	ids := make([]int, len(recs))
	for i := range recs {
		ids[i] = recs[i].ID
	}

	tracks := c.batchFetchTracks(r, ids)

	sub := spec.NewResponse()
	sub.SimilarSongsTwo = &spec.SimilarSongsTwo{Tracks: tracks}
	return sub
}

func (c *Controller) ServeGetSimilarSongs(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	count := p.GetOrInt("count", 10)
	if count > similarSongsMaxCount {
		count = similarSongsMaxCount
	}

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Track {
		// Only works with tracks - fallback to random songs
		return c.ServeGetRandomSongs(r)
	}

	// Try recommendations with short timeout
	ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
	recs, err := c.proxy.GetRecommendations(ctx, id.Value)
	cancel()

	// If failed or empty, fallback to random songs quickly
	if err != nil {
		log.Printf("[DISC] GetRecommendations ERROR for track %d: %v", id.Value, err)
		return c.ServeGetRandomSongs(r)
	}
	if len(recs) == 0 {
		log.Printf("[DISC] GetRecommendations EMPTY for track %d", id.Value)
		return c.ServeGetRandomSongs(r)
	}

	if len(recs) > count {
		recs = recs[:count]
	}

	ids := make([]int, len(recs))
	for i := range recs {
		ids[i] = recs[i].ID
	}

	tracks := c.batchFetchTracks(r, ids)

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
	if err != nil || len(candidates) == 0 {
		return spec.NewResponse()
	}

	artistID := 0
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

	if artistID == 0 {
		return spec.NewResponse()
	}

	// get artist top tracks (precise)
	topTracks, err := c.proxy.GetArtistTopTracks(r.Context(), artistID, count)
	if err != nil {
		log.Printf("[DISC] error fetching artist top tracks for %d: %v", artistID, err)
		// Fallback to searching tracks by artist name if this fails?
		// For now just return empty or error.
		return spec.NewResponse()
	}

	tracks := make([]*spec.TrackChild, len(topTracks))
	for i := range topTracks {
		tracks[i] = spec.NewTrackFromTidal(&topTracks[i])
		c.applyTrackStar(user.ID, tracks[i])
		c.applyTrackPlayCount(user.ID, tracks[i])
	}

	sub := spec.NewResponse()
	sub.TopSongs = &spec.TopSongs{Tracks: tracks}
	return sub
}
