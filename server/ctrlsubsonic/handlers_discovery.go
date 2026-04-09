package ctrlsubsonic

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"go.senan.xyz/gonic/db"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetRandomSongs(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)
	size := p.GetOrInt("size", 10)

	// 1. Get some from starred (50%)
	favSize := size / 2
	if favSize < 1 { favSize = 1 }
	
	var stars []db.TrackStar
	c.dbc.Where("user_id=?", user.ID).Order("RANDOM()").Limit(favSize).Find(&stars)

	var tidalIDs []int
	for _, s := range stars {
		tidalIDs = appendUnique(tidalIDs, s.TidalID)
	}

	// 2. Get some from Discovery (Tidal Top Tracks)
	discoverySize := size - len(tidalIDs)
	if discoverySize > 0 {
		top, err := c.proxy.GetTopTracks(r.Context(), discoverySize+10) // fetch extra to shuffle
		if err == nil {
			for _, t := range top {
				if len(tidalIDs) >= size { break }
				tidalIDs = appendUnique(tidalIDs, t.ID)
			}
		}
	}

	tracks := c.batchFetchTracks(r, tidalIDs)

	sub := spec.NewResponse()
	sub.RandomTracks = &spec.RandomTracks{List: tracks}
	return sub
}

func (c *Controller) ServeGetSimilarSongsTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	count := p.GetOrInt("count", 10)

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "provide an `id` parameter")
	}

	var trackID int
	switch id.Type {
	case specid.Track:
		trackID = id.Value
	case specid.Artist:
		// get first track from artist
		page, err := c.proxy.GetArtistAlbums(r.Context(), id.Value, false)
		if err != nil || len(page.Tracks) == 0 {
			return spec.NewResponse()
		}
		trackID = page.Tracks[0].ID
	default:
		return spec.NewError(10, "provide a track or artist `id`")
	}

	recs, err := c.proxy.GetRecommendations(r.Context(), trackID)
	if err != nil {
		return spec.NewResponse()
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
	count := p.GetOrInt("count", 20)
	if count > 20 {
		count = 20 // limit for speed
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
		// get a representative track for the artist to get recommendations
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		page, err := c.proxy.GetArtistAlbums(ctx, id.Value, true) // skip tracks for speed
		cancel()
		if err != nil || page == nil || len(page.Tracks) == 0 {
			return spec.NewResponse()
		}
		trackID = page.Tracks[0].ID
	default:
		return spec.NewResponse()
	}

	// fast timeout for recommendations
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	recs, err := c.proxy.GetRecommendations(ctx, trackID)
	cancel()

	if err != nil {
		log.Printf("[DISC] GetRecommendations error: %v", err)
		return spec.NewResponse()
	}

	if len(recs) > count {
		recs = recs[:count]
	}

	// quick return if no recs
	if len(recs) == 0 {
		return spec.NewResponse()
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
	candidates, err := c.proxy.SearchArtists(r.Context(), artistName, 10, 0)
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
