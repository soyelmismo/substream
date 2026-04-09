package ctrlsubsonic

import (
	"log"
	"net/http"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeSearchThree(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	query, err := p.Get("query")
	if err != nil {
		return spec.NewError(10, "please provide a `query` parameter")
	}

	artistCount := p.GetOrInt("artistCount", 20)
	albumCount := p.GetOrInt("albumCount", 20)
	songCount := p.GetOrInt("songCount", 20)
	artistOffset := p.GetOrInt("artistOffset", 0)
	albumOffset := p.GetOrInt("albumOffset", 0)
	songOffset := p.GetOrInt("songOffset", 0)

	results := &spec.SearchResultThree{}

	type tracksResult struct {
		tracks []spec.TrackChild
		err    error
	}

	// parallel search: tracks, artists, albums
	tracksCh := make(chan []spec.TrackChild, 1)
	artistsCh := make(chan []spec.Artist, 1)
	albumsCh := make(chan []spec.Album, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[SUBS] SearchTracks panic: %v", r)
				tracksCh <- nil
			}
		}()
		if songCount <= 0 {
			tracksCh <- nil
			return
		}
		if songCount > 500 {
			songCount = 500
		}
		tracks, err := c.proxy.SearchTracks(r.Context(), query, songCount, songOffset)
		if err != nil {
			log.Printf("[SUBS] SearchTracks error: %v", err)
			tracksCh <- nil
			return
		}
		out := make([]spec.TrackChild, len(tracks))
		for i := range tracks {
			out[i] = *spec.NewTrackFromTidal(&tracks[i])
		}
		tracksCh <- out
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[SUBS] SearchArtists panic: %v", r)
				artistsCh <- nil
			}
		}()
		if artistCount <= 0 {
			artistsCh <- nil
			return
		}
		if artistCount > 500 {
			artistCount = 500
		}
		artists, err := c.proxy.SearchArtists(r.Context(), query, artistCount, artistOffset)
		if err != nil {
			log.Printf("[SUBS] SearchArtists error: %v", err)
			artistsCh <- nil
			return
		}
		out := make([]spec.Artist, len(artists))
		for i := range artists {
			out[i] = *spec.NewArtistFromTidal(&artists[i])
		}
		artistsCh <- out
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[SUBS] SearchAlbums panic: %v", r)
				albumsCh <- nil
			}
		}()
		if albumCount <= 0 {
			albumsCh <- nil
			return
		}
		if albumCount > 500 {
			albumCount = 500
		}
		albums, err := c.proxy.SearchAlbums(r.Context(), query, albumCount, albumOffset)
		if err != nil {
			log.Printf("[SUBS] SearchAlbums error: %v", err)
			albumsCh <- nil
			return
		}
		out := make([]spec.Album, len(albums))
		for i := range albums {
			out[i] = *spec.NewAlbumFromTidal(&albums[i])
		}
		albumsCh <- out
	}()

	log.Printf("[SUBS] Awaiting search results for query %q", query)
	tracks := <-tracksCh
	artists := <-artistsCh
	albums := <-albumsCh
	log.Printf("[SUBS] Search results ready for query %q: tracks=%d artists=%d albums=%d", query, len(tracks), len(artists), len(albums))


	// apply star info from local DB
	user := r.Context().Value(CtxUser).(*db.User)

	for i := range tracks {
		results.Tracks = append(results.Tracks, &tracks[i])
		c.applyTrackStar(user.ID, &tracks[i])
	}
	for i := range artists {
		results.Artists = append(results.Artists, &artists[i])
		c.applyArtistStar(user.ID, &artists[i])
	}
	for i := range albums {
		results.Albums = append(results.Albums, &albums[i])
		c.applyAlbumStar(user.ID, &albums[i])
	}

	sub := spec.NewResponse()
	sub.SearchResultThree = results
	return sub
}

// applyTrackStar checks if user has starred this track and decorates it
func (c *Controller) applyTrackStar(userID int, tc *spec.TrackChild) {
	if tc.ID == nil {
		return
	}
	var star db.TrackStar
	if c.dbc.Where("user_id=? AND tidal_id=?", userID, tc.ID.Value).First(&star).Error == nil {
		tc.Starred = &star.StarDate
	}
}

// applyAlbumStar checks if user has starred this album and decorates it
func (c *Controller) applyAlbumStar(userID int, a *spec.Album) {
	if a.ID == nil {
		return
	}
	var star db.AlbumStar
	if c.dbc.Where("user_id=? AND tidal_id=?", userID, a.ID.Value).First(&star).Error == nil {
		a.Starred = &star.StarDate
	}
}

// applyArtistStar checks if user has starred this artist and decorates it
func (c *Controller) applyArtistStar(userID int, a *spec.Artist) {
	if a.ID == nil {
		return
	}
	var star db.ArtistStar
	if c.dbc.Where("user_id=? AND tidal_id=?", userID, a.ID.Value).First(&star).Error == nil {
		a.Starred = &star.StarDate
	}
}

// star helpers for rating
func (c *Controller) getTrackRating(userID, tidalID int) int {
	var rating db.TrackRating
	if c.dbc.Where("user_id=? AND tidal_id=?", userID, tidalID).First(&rating).Error == nil {
		return rating.Rating
	}
	return 0
}

func (c *Controller) getAlbumRating(userID, tidalID int) int {
	var rating db.AlbumRating
	if c.dbc.Where("user_id=? AND tidal_id=?", userID, tidalID).First(&rating).Error == nil {
		return rating.Rating
	}
	return 0
}

// =====================================================================
// Star / Unstar / Rating
// =====================================================================

func (c *Controller) ServeStar(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	// star supports id, albumId, artistId
	if id, err := p.GetID("id"); err == nil {
		switch id.Type {
		case specid.Track:
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).
				FirstOrCreate(&db.TrackStar{UserID: user.ID, TidalID: id.Value})
		case specid.Album:
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).
				FirstOrCreate(&db.AlbumStar{UserID: user.ID, TidalID: id.Value})
		case specid.Artist:
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).
				FirstOrCreate(&db.ArtistStar{UserID: user.ID, TidalID: id.Value})
		}
	}
	if id, err := p.GetID("albumId"); err == nil {
		c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).
			FirstOrCreate(&db.AlbumStar{UserID: user.ID, TidalID: id.Value})
	}
	if id, err := p.GetID("artistId"); err == nil {
		c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).
			FirstOrCreate(&db.ArtistStar{UserID: user.ID, TidalID: id.Value})
	}

	return spec.NewResponse()
}

func (c *Controller) ServeUnstar(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	if id, err := p.GetID("id"); err == nil {
		switch id.Type {
		case specid.Track:
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).Delete(&db.TrackStar{})
		case specid.Album:
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).Delete(&db.AlbumStar{})
		case specid.Artist:
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).Delete(&db.ArtistStar{})
		}
	}
	if id, err := p.GetID("albumId"); err == nil {
		c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).Delete(&db.AlbumStar{})
	}
	if id, err := p.GetID("artistId"); err == nil {
		c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).Delete(&db.ArtistStar{})
	}

	return spec.NewResponse()
}

func (c *Controller) ServeSetRating(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "provide an `id` parameter")
	}
	rating, err := p.GetInt("rating")
	if err != nil {
		return spec.NewError(10, "provide a `rating` parameter")
	}

	switch id.Type {
	case specid.Track:
		if rating == 0 {
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).Delete(&db.TrackRating{})
		} else {
			var existing db.TrackRating
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).First(&existing)
			existing.UserID = user.ID
			existing.TidalID = id.Value
			existing.Rating = rating
			c.dbc.Save(&existing)
		}
	case specid.Album:
		if rating == 0 {
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).Delete(&db.AlbumRating{})
		} else {
			var existing db.AlbumRating
			c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).First(&existing)
			existing.UserID = user.ID
			existing.TidalID = id.Value
			existing.Rating = rating
			c.dbc.Save(&existing)
		}
	}

	return spec.NewResponse()
}

func (c *Controller) ServeGetStarredTwo(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	results := &spec.StarredTwo{}

	// starred tracks
	var trackStars []db.TrackStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Find(&trackStars)
	trackIDs := make([]int, len(trackStars))
	for i, s := range trackStars {
		trackIDs[i] = s.TidalID
	}
	tracks := c.batchFetchTracks(r, trackIDs)
	for _, tc := range tracks {
		results.Tracks = append(results.Tracks, tc)
	}

	// starred albums
	var albumStars []db.AlbumStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Find(&albumStars)
	for _, s := range albumStars {
		album, err := c.proxy.GetAlbumInfo(r.Context(), s.TidalID)
		if err != nil {
			continue
		}
		a := spec.NewAlbumFromTidal(album)
		a.Starred = &s.StarDate
		results.Albums = append(results.Albums, a)
	}

	// starred artists
	var artistStars []db.ArtistStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Find(&artistStars)
	for _, s := range artistStars {
		info, err := c.proxy.GetArtistInfo(r.Context(), s.TidalID)
		if err != nil {
			continue
		}
		a := spec.NewArtistFromTidal(&info.Artist)
		a.Starred = &s.StarDate
		results.Artists = append(results.Artists, a)
	}

	sub := spec.NewResponse()
	sub.StarredTwo = results
	return sub
}
