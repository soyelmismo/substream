package ctrlsubsonic

import (
	"log"
	"net/http"

	"go.senan.xyz/gonic/db"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetRandomSongs(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)
	size := p.GetOrInt("size", 10)

	// random tracks from user's starred tracks
	var stars []db.TrackStar
	c.dbc.Where("user_id=?", user.ID).
		Order("RANDOM()").
		Limit(size).
		Find(&stars)

	tidalIDs := make([]int, len(stars))
	for i, s := range stars {
		tidalIDs[i] = s.TidalID
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
		page, err := c.proxy.GetArtistAlbums(r.Context(), id.Value, false)
		if err != nil || len(page.Tracks) == 0 {
			return spec.NewResponse()
		}
		trackID = page.Tracks[0].ID
	default:
		return spec.NewResponse()
	}

	recs, err := c.proxy.GetRecommendations(r.Context(), trackID)
	if err != nil {
		log.Printf("[DISC] GetRecommendations error: %v", err)
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
	sub.SimilarSongs = &spec.SimilarSongs{Tracks: tracks}
	return sub
}


func (c *Controller) ServeGetTopSongs(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	count := p.GetOrInt("count", 10)

	artistName, err := p.Get("artist")
	if err != nil {
		return spec.NewError(10, "provide an `artist` parameter")
	}

	// search artist by name to get ID
	artists, err := c.proxy.SearchArtists(r.Context(), artistName, 1, 0)
	if err != nil || len(artists) == 0 {
		return spec.NewResponse()
	}

	// get artist page with tracks
	page, err := c.proxy.GetArtistAlbums(r.Context(), artists[0].ID, false)
	if err != nil {
		return spec.NewResponse()
	}

	topTracks := page.Tracks
	if len(topTracks) > count {
		topTracks = topTracks[:count]
	}

	tracks := make([]*spec.TrackChild, len(topTracks))
	for i := range topTracks {
		tracks[i] = spec.NewTrackFromTidal(&topTracks[i])
	}

	sub := spec.NewResponse()
	sub.TopSongs = &spec.TopSongs{Tracks: tracks}
	return sub
}
