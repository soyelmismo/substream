package ctrlsubsonic

import (
	"log"
	"net/http"
	"strings"
	"time"


	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

type cachedSearch struct {
	tracks  []spec.TrackChild
	artists []spec.Artist
	albums  []spec.Album
	expiresAt time.Time
}

func (c *Controller) ServeSearchThree(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	query, err := p.Get("query")
	if err != nil {
		return spec.NewError(10, "please provide a `query` parameter")
	}

	var tracks []spec.TrackChild
	var artists []spec.Artist
	var albums []spec.Album
	var fromCache bool

	// Check cache
	if val, ok := c.searchCache.Load(query); ok {
		cached := val.(cachedSearch)
		if time.Now().Before(cached.expiresAt) {
			tracks = cached.tracks
			artists = cached.artists
			albums = cached.albums
			fromCache = true
		}
	}



	// Respect counts from client. If 0 is sent, don't search that type.
	// We use -1 as a marker for "not provided"
	artistCount := p.GetOrInt("artistCount", -1)
	albumCount := p.GetOrInt("albumCount", -1)
	songCount := p.GetOrInt("songCount", -1)

	// Set defaults only if not provided (-1)
	if artistCount == -1 { artistCount = 3 }
	if albumCount == -1 { albumCount = 20 }
	if songCount == -1 { songCount = 20 }

	// Still apply caps to prevent performance issues
	if artistCount > 10 { artistCount = 10 }
	if albumCount > 50 { albumCount = 50 }
	if songCount > 50 { songCount = 50 }

	artistOffset := p.GetOrInt("artistOffset", 0)
	albumOffset := p.GetOrInt("albumOffset", 0)
	songOffset := p.GetOrInt("songOffset", 0)

	results := &spec.SearchResultThree{
		Artists: []*spec.Artist{},
		Albums:  []*spec.Album{},
		Tracks:  []*spec.TrackChild{},
	}

	type tracksResult struct {
		tracks []spec.TrackChild
		err    error
	}

	// parallel search: tracks, artists, albums
	tracksCh := make(chan []spec.TrackChild, 1)
	artistsCh := make(chan []spec.Artist, 1)
	albumsCh := make(chan []spec.Album, 1)

	if fromCache {
		tracksCh <- tracks
		artistsCh <- artists
		albumsCh <- albums
	} else {
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
			tData, err := c.proxy.SearchTracks(r.Context(), query, songCount, songOffset)
			if err != nil {
				log.Printf("[SUBS] SearchTracks error: %v", err)
				tracksCh <- nil
				return
			}
			if len(tData) > songCount {
				tData = tData[:songCount]
			}
			out := make([]spec.TrackChild, len(tData))
			for i := range tData {
				out[i] = *spec.NewTrackFromTidal(&tData[i])
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
			aData, err := c.proxy.SearchArtists(r.Context(), query, artistCount, artistOffset)
			if err != nil {
				log.Printf("[SUBS] SearchArtists error: %v", err)
				artistsCh <- nil
				return
			}
			if len(aData) > artistCount {
				aData = aData[:artistCount]
			}
			out := make([]spec.Artist, len(aData))
			for i := range aData {
				out[i] = *spec.NewArtistFromTidal(&aData[i])
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
			alData, err := c.proxy.SearchAlbums(r.Context(), query, albumCount, albumOffset)
			if err != nil {
				log.Printf("[SUBS] SearchAlbums error: %v", err)
				albumsCh <- nil
				return
			}
			if len(alData) > albumCount {
				alData = alData[:albumCount]
			}
			out := make([]spec.Album, len(alData))
			for i := range alData {
				out[i] = *spec.NewAlbumFromTidal(&alData[i])
			}
			albumsCh <- out
		}()
	}

	// 2. Parallel Favorites Search
	favTracksCh := make(chan []spec.TrackChild, 1)
	favArtistsCh := make(chan []spec.Artist, 1)
	favAlbumsCh := make(chan []spec.Album, 1)

	user := r.Context().Value(CtxUser).(*db.User)
	if !fromCache && len(query) >= 3 && (songOffset == 0 || albumOffset == 0 || artistOffset == 0) {
		queryLower := strings.ToLower(query)

		go func() {
			if albumOffset != 0 {
				favAlbumsCh <- nil
				return
			}
			var stars []db.AlbumStar
			c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(20).Find(&stars)
			starIDs := make([]int, len(stars))
			for i, s := range stars {
				starIDs[i] = s.TidalID
			}

			starredMeta := c.batchFetchAlbums(r, starIDs)
			var matches []spec.Album
			for _, a := range starredMeta {
				if strings.Contains(strings.ToLower(a.Name), queryLower) || strings.Contains(strings.ToLower(a.Artist), queryLower) {
					matches = append(matches, *a)
				}
			}
			favAlbumsCh <- matches
		}()

		go func() {
			if artistOffset != 0 {
				favArtistsCh <- nil
				return
			}
			var stars []db.ArtistStar
			c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(20).Find(&stars)
			starIDs := make([]int, len(stars))
			for i, s := range stars {
				starIDs[i] = s.TidalID
			}

			starredMeta := c.batchFetchArtists(r, starIDs)
			var matches []spec.Artist
			for _, a := range starredMeta {
				if strings.Contains(strings.ToLower(a.Name), queryLower) {
					matches = append(matches, *a)
				}
			}
			favArtistsCh <- matches
		}()

		go func() {
			if songOffset != 0 {
				favTracksCh <- nil
				return
			}
			var stars []db.TrackStar
			c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(20).Find(&stars)
			starIDs := make([]int, len(stars))
			for i, s := range stars {
				starIDs[i] = s.TidalID
			}

			starredMeta := c.batchFetchTracks(r, starIDs)
			var matches []spec.TrackChild
			for _, t := range starredMeta {
				if strings.Contains(strings.ToLower(t.Title), queryLower) || strings.Contains(strings.ToLower(t.Artist), queryLower) || strings.Contains(strings.ToLower(t.Album), queryLower) {
					matches = append(matches, *t)
				}
			}
			favTracksCh <- matches
		}()
	} else {
		favTracksCh <- nil
		favArtistsCh <- nil
		favAlbumsCh <- nil
	}

	log.Printf("[SUBS] Awaiting search results for query %q (cache=%v)", query, fromCache)
	tracks = <-tracksCh
	artists = <-artistsCh
	albums = <-albumsCh

	// implement "Favorites First" logic merge with timeout
	var favTracks []spec.TrackChild
	var favArtists []spec.Artist
	var favAlbums []spec.Album

	waitForFavs := time.NewTimer(3 * time.Second)
	defer waitForFavs.Stop()

favLoop:
	for i := 0; i < 3; i++ {
		select {
		case favTracks = <-favTracksCh:
		case favArtists = <-favArtistsCh:
		case favAlbums = <-favAlbumsCh:
		case <-waitForFavs.C:
			log.Printf("[SUBS] Favorites search for %q timed out", query)
			break favLoop
		case <-r.Context().Done():
			return nil
		}
	}

	if len(favAlbums) > 0 {
		newAlbums := favAlbums
		seenIDs := make(map[int]bool)
		for _, m := range favAlbums {
			seenIDs[m.ID.Value] = true
		}
		for _, a := range albums {
			if !seenIDs[a.ID.Value] {
				newAlbums = append(newAlbums, a)
				seenIDs[a.ID.Value] = true
			}
		}
		albums = newAlbums
	}

	if len(favArtists) > 0 {
		newArtists := favArtists
		seenIDs := make(map[int]bool)
		for _, m := range favArtists {
			seenIDs[m.ID.Value] = true
		}
		for _, a := range artists {
			if !seenIDs[a.ID.Value] {
				newArtists = append(newArtists, a)
				seenIDs[a.ID.Value] = true
			}
		}
		artists = newArtists
	}

	if len(favTracks) > 0 {
		newTracks := favTracks
		seenIDs := make(map[int]bool)
		for _, m := range favTracks {
			seenIDs[m.ID.Value] = true
		}
		for _, t := range tracks {
			if !seenIDs[t.ID.Value] {
				newTracks = append(newTracks, t)
				seenIDs[t.ID.Value] = true
			}
		}
		tracks = newTracks
	}

	if !fromCache && (len(tracks) > 0 || len(artists) > 0 || len(albums) > 0) {
		c.searchCache.Store(query, cachedSearch{
			tracks:    tracks,
			artists:   artists,
			albums:    albums,
			expiresAt: time.Now().Add(1 * time.Minute),
		})
	}

	log.Printf("[SUBS] Search results ready for query %q: tracks=%d artists=%d albums=%d", query, len(tracks), len(artists), len(albums))

	if r.Context().Err() != nil {
		log.Printf("[SUBS] Search query %q cancelled", query)
		return nil
	}




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
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(50).Find(&trackStars)
	
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
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(50).Find(&albumStars)
	albumIDs := make([]int, len(albumStars))
	for i, s := range albumStars {
		albumIDs[i] = s.TidalID
	}
	results.Albums = c.batchFetchAlbums(r, albumIDs)

	// starred artists
	var artistStars []db.ArtistStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(50).Find(&artistStars)
	artistIDs := make([]int, len(artistStars))
	for i, s := range artistStars {
		artistIDs[i] = s.TidalID
	}
	results.Artists = c.batchFetchArtists(r, artistIDs)

	sub := spec.NewResponse()
	sub.StarredTwo = results
	return sub
}
