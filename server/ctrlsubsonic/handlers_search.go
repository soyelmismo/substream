package ctrlsubsonic

import (
	"log"
	"net/http"
	"strings"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

type cachedSearch struct {
	tracks  []spec.TrackChild
	artists []spec.Artist
	albums  []spec.Album
}

func (c *Controller) ServeSearchThree(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	query, err := p.Get("query")
	if err != nil {
		return spec.NewError(10, "please provide a `query` parameter")
	}

	// SYMFONIUM SYNC DETECTION: Empty query means "enumerate my library"
	// Return virtual library content only (NOT full Tidal catalog) to prevent infinite loops
	trimmedQuery := strings.TrimSpace(query)
	if trimmedQuery == "" || trimmedQuery == `""` || trimmedQuery == `''` {
		log.Printf("[SUBS] Empty query detected - returning virtual library only (Symfonium sync)")
		return c.searchVirtualLibrary(r)
	}

	var tracks []spec.TrackChild
	var artists []spec.Artist
	var albums []spec.Album
	var fromCache bool

	var favTracks []*spec.TrackChild
	var favArtists []*spec.Artist
	var favAlbums []*spec.Album

	// Check cache
	if cached := c.searchCache.Get(query); len(cached.tracks) > 0 || len(cached.artists) > 0 || len(cached.albums) > 0 {
		tracks = cached.tracks
		artists = cached.artists
		albums = cached.albums
		fromCache = true
	}

	// Respect counts from client. If 0 is sent, don't search that type.
	// We use -1 as a marker for "not provided"
	artistCount := p.GetOrInt("artistCount", -1)
	albumCount := p.GetOrInt("albumCount", -1)
	songCount := p.GetOrInt("songCount", -1)

	// Set defaults only if not provided (-1) - optimize for Tidal
	if artistCount == -1 {
		artistCount = 5
	}
	if albumCount == -1 {
		albumCount = 30
	}
	if songCount == -1 {
		songCount = 30
	}

	if artistCount > 50 {
		artistCount = 50
	}
	if albumCount > 100 {
		albumCount = 100
	}
	if songCount > 100 {
		songCount = 100
	}

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

	// 2. Optimized Favorites Search via SQL (Favorites-First)
	user := r.Context().Value(CtxUser).(*db.User)
	if !fromCache && len(query) >= 3 && (songOffset == 0 || albumOffset == 0 || artistOffset == 0) {
		q := "%" + query + "%"

		if songOffset == 0 {
			var stars []db.TrackStar
			c.dbc.Where("user_id = ? AND (fallback_title LIKE ? OR fallback_artist LIKE ?)", user.ID, q, q).Limit(20).Find(&stars)
			if len(stars) > 0 {
				ids := make([]int, len(stars))
				for i, s := range stars {
					ids[i] = extractIDFromURI(s.URI)
				}
				favTracks = c.batchFetchTracks(r, ids)
			}
		}

		if albumOffset == 0 {
			var stars []db.AlbumStar
			c.dbc.Where("user_id = ? AND (fallback_title LIKE ? OR fallback_artist LIKE ?)", user.ID, q, q).Limit(20).Find(&stars)
			if len(stars) > 0 {
				ids := make([]int, len(stars))
				for i, s := range stars {
					ids[i] = extractIDFromURI(s.URI)
				}
				favAlbums = c.batchFetchAlbums(r, ids)
			}
		}

		if artistOffset == 0 {
			var stars []db.ArtistStar
			c.dbc.Where("user_id = ? AND fallback_name LIKE ?", user.ID, q).Limit(20).Find(&stars)
			if len(stars) > 0 {
				ids := make([]int, len(stars))
				for i, s := range stars {
					ids[i] = extractIDFromURI(s.URI)
				}
				favArtists = c.batchFetchArtists(r, ids)
			}
		}
	}

	if len(favAlbums) > 0 {
		seenIDs := make(map[int]bool)
		var combined []*spec.Album
		for _, v := range favAlbums {
			combined = append(combined, v)
			seenIDs[v.ID.Value()] = true
		}
		for i := range albums {
			if !seenIDs[albums[i].ID.Value()] {
				combined = append(combined, &albums[i])
			}
		}
		results.Albums = combined
	} else {
		for i := range albums {
			results.Albums = append(results.Albums, &albums[i])
		}
	}

	if len(favArtists) > 0 {
		seenIDs := make(map[int]bool)
		var combined []*spec.Artist
		for _, v := range favArtists {
			combined = append(combined, v)
			seenIDs[v.ID.Value()] = true
		}
		for i := range artists {
			if !seenIDs[artists[i].ID.Value()] {
				combined = append(combined, &artists[i])
			}
		}
		results.Artists = combined
	} else {
		for i := range artists { results.Artists = append(results.Artists, &artists[i]) }
	}

	if len(favTracks) > 0 {
		seenIDs := make(map[int]bool)
		var combined []*spec.TrackChild
		for _, v := range favTracks {
			combined = append(combined, v)
			seenIDs[v.ID.Value()] = true
		}
		for i := range tracks {
			if !seenIDs[tracks[i].ID.Value()] {
				combined = append(combined, &tracks[i])
			}
		}
		results.Tracks = combined
	} else {
		for i := range tracks { results.Tracks = append(results.Tracks, &tracks[i]) }
	}

	if !fromCache && (len(tracks) > 0 || len(artists) > 0 || len(albums) > 0) {
		c.searchCache.Set(query, cachedSearch{
			tracks:  tracks,
			artists: artists,
			albums:  albums,
		}, 0) // Use default TTL
	}

	log.Printf("[SUBS] Search results ready for query %q: tracks=%d artists=%d albums=%d", query, len(tracks), len(artists), len(albums))

	if r.Context().Err() != nil {
		log.Printf("[SUBS] Search query %q cancelled", query)
		return nil
	}

	for _, t := range results.Tracks {
		c.applyTrackStar(user.ID, t)
	}
	for _, a := range results.Artists {
		c.applyArtistStar(user.ID, a)
	}
	for _, a := range results.Albums {
		c.applyAlbumStar(user.ID, a)
	}

	sub := spec.NewResponse()
	sub.SearchResultThree = results
	return sub
}

// searchVirtualLibrary returns only the user's local library content (stars, plays, playlists)
// This is used for empty query searches where Symfonium is trying to enumerate the library
func (c *Controller) searchVirtualLibrary(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	// Get pagination params
	artistCount := p.GetOrInt("artistCount", 0)
	albumCount := p.GetOrInt("albumCount", 0)
	songCount := p.GetOrInt("songCount", 0)
	artistOffset := p.GetOrInt("artistOffset", 0)
	albumOffset := p.GetOrInt("albumOffset", 0)
	songOffset := p.GetOrInt("songOffset", 0)

	results := &spec.SearchResultThree{
		Artists: []*spec.Artist{},
		Albums:  []*spec.Album{},
		Tracks:  []*spec.TrackChild{},
	}

	// Get virtual library IDs
	artistURIs := c.dbc.GetVirtualLibraryArtistIDs(user.ID)
	albumURIs := c.dbc.GetVirtualLibraryAlbumIDs(user.ID)
	trackURIs := c.dbc.GetVirtualLibraryTrackIDs(user.ID)

	// Apply pagination and fetch artists
	if artistCount > 0 && artistOffset < len(artistURIs) {
		end := artistOffset + artistCount
		if end > len(artistURIs) {
			end = len(artistURIs)
		}
		artistIDs := extractIDsFromURIs(artistURIs[artistOffset:end])
		artists := c.batchFetchArtists(r, artistIDs)
		for _, a := range artists {
			results.Artists = append(results.Artists, a)
		}
	}

	// Apply pagination and fetch albums
	if albumCount > 0 && albumOffset < len(albumURIs) {
		end := albumOffset + albumCount
		if end > len(albumURIs) {
			end = len(albumURIs)
		}
		albumIDs := extractIDsFromURIs(albumURIs[albumOffset:end])
		albums := c.batchFetchAlbums(r, albumIDs)
		for _, a := range albums {
			results.Albums = append(results.Albums, a)
		}
	}

	// Apply pagination and fetch tracks
	if songCount > 0 && songOffset < len(trackURIs) {
		end := songOffset + songCount
		if end > len(trackURIs) {
			end = len(trackURIs)
		}
		trackIDs := extractIDsFromURIs(trackURIs[songOffset:end])
		tracks := c.batchFetchTracks(r, trackIDs)
		for _, t := range tracks {
			results.Tracks = append(results.Tracks, t)
		}
	}

	log.Printf("[SUBS] Virtual library search: artists=%d/%d, albums=%d/%d, tracks=%d/%d",
		len(results.Artists), len(artistURIs), len(results.Albums), len(albumURIs), len(results.Tracks), len(trackURIs))

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
	if c.dbc.Where("user_id=? AND uri=?", userID, tc.ID.String()).First(&star).Error == nil {
		tc.Starred = &star.StarDate
	}
}

// applyTrackPlayCount applies the local play count from DB to track
func (c *Controller) applyTrackPlayCount(userID int, tc *spec.TrackChild) {
	if tc.ID == nil {
		return
	}
	var play db.Play
	if c.dbc.Where("user_id=? AND uri=?", userID, tc.ID.String()).First(&play).Error == nil {
		tc.PlayCount = play.Count
	}
}

// applyAlbumStar checks if user has starred this album and decorates it
func (c *Controller) applyAlbumStar(userID int, a *spec.Album) {
	if a.ID == nil {
		return
	}
	var star db.AlbumStar
	if c.dbc.Where("user_id=? AND uri=?", userID, a.ID.String()).First(&star).Error == nil {
		a.Starred = &star.StarDate
	}
}

// applyArtistStar checks if user has starred this artist and decorates it
func (c *Controller) applyArtistStar(userID int, a *spec.Artist) {
	if a.ID == nil {
		return
	}
	var star db.ArtistStar
	if c.dbc.Where("user_id=? AND uri=?", userID, a.ID.String()).First(&star).Error == nil {
		a.Starred = &star.StarDate
	}
}

// star helpers for rating
func (c *Controller) getTrackRating(userID int, uri string) int {
	var rating db.TrackRating
	if c.dbc.Where("user_id=? AND uri=?", userID, uri).First(&rating).Error == nil {
		return rating.Rating
	}
	return 0
}

func (c *Controller) getAlbumRating(userID int, uri string) int {
	var rating db.AlbumRating
	if c.dbc.Where("user_id=? AND uri=?", userID, uri).First(&rating).Error == nil {
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

	starFn := func(id specid.ID) {
		uri := id.String()
		switch id.Type() {
		case specid.Track:
			var s db.TrackStar
			if c.dbc.Where("user_id=? AND uri=?", user.ID, uri).First(&s).RecordNotFound() {
				if t, err := c.proxy.GetTrackInfo(r.Context(), id.Value()); err == nil {
					s = db.TrackStar{UserID: user.ID, URI: uri, Provider: "tidal", FallbackArtist: t.Artist.Name, FallbackTitle: t.Title}
				} else {
					s = db.TrackStar{UserID: user.ID, URI: uri, Provider: "tidal"}
				}
				c.dbc.Create(&s)
			}
		case specid.Album:
			var s db.AlbumStar
			if c.dbc.Where("user_id=? AND uri=?", user.ID, uri).First(&s).RecordNotFound() {
				if t, err := c.proxy.GetAlbumInfo(r.Context(), id.Value()); err == nil {
					artistName := ""
					if len(t.Artists) > 0 { artistName = t.Artists[0].Name }
					s = db.AlbumStar{UserID: user.ID, URI: uri, FallbackArtist: artistName, FallbackTitle: t.Title}
				} else {
					s = db.AlbumStar{UserID: user.ID, URI: uri}
				}
				c.dbc.Create(&s)
			}
		case specid.Artist:
			var s db.ArtistStar
			if c.dbc.Where("user_id=? AND uri=?", user.ID, uri).First(&s).RecordNotFound() {
				if t, err := c.proxy.GetArtistInfo(r.Context(), id.Value()); err == nil {
					s = db.ArtistStar{UserID: user.ID, URI: uri, FallbackName: t.Artist.Name}
				} else {
					s = db.ArtistStar{UserID: user.ID, URI: uri}
				}
				c.dbc.Create(&s)
			}
		}
	}

	if id, err := p.GetID("id"); err == nil {
		starFn(id)
	}
	if id, err := p.GetID("albumId"); err == nil {
		starFn(id)
	}
	if id, err := p.GetID("artistId"); err == nil {
		starFn(id)
		go func() {
			c.proxy.GetArtistInfo(r.Context(), id.Value())
			c.proxy.GetArtistAlbums(r.Context(), id.Value(), true)
		}()
	}

	return spec.NewResponse()
}

func (c *Controller) ServeUnstar(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	if id, err := p.GetID("id"); err == nil {
		uri := id.String()
		switch id.Type() {
		case specid.Track:
			c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.TrackStar{})
		case specid.Album:
			c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.AlbumStar{})
		case specid.Artist:
			c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.ArtistStar{})
		}
	}
	if id, err := p.GetID("albumId"); err == nil {
		uri := id.String()
		c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.AlbumStar{})
	}
	if id, err := p.GetID("artistId"); err == nil {
		uri := id.String()
		c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.ArtistStar{})
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
	uri := id.String()

	switch id.Type() {
	case specid.Track:
		if rating == 0 {
			c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.TrackRating{})
		} else {
			var existing db.TrackRating
			c.dbc.Where("user_id=? AND uri=?", user.ID, uri).First(&existing)
			existing.UserID = user.ID
			existing.URI = uri
			existing.Rating = rating
			c.dbc.Save(&existing)
		}
	case specid.Album:
		if rating == 0 {
			c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.AlbumRating{})
		} else {
			var existing db.AlbumRating
			c.dbc.Where("user_id=? AND uri=?", user.ID, uri).First(&existing)
			existing.UserID = user.ID
			existing.URI = uri
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
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(100).Find(&trackStars)

	trackIDs := make([]int, len(trackStars))
	for i, s := range trackStars {
		trackIDs[i] = extractIDFromURI(s.URI)
	}
	tracks := c.batchFetchTracks(r, trackIDs)

	for _, tc := range tracks {
		results.Tracks = append(results.Tracks, tc)
	}

	// starred albums
	var albumStars []db.AlbumStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(100).Find(&albumStars)
	albumIDs := make([]int, len(albumStars))
	for i, s := range albumStars {
		albumIDs[i] = extractIDFromURI(s.URI)
	}
	results.Albums = c.batchFetchAlbums(r, albumIDs)

	// starred artists
	var artistStars []db.ArtistStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Limit(100).Find(&artistStars)
	artistIDs := make([]int, len(artistStars))
	for i, s := range artistStars {
		artistIDs[i] = extractIDFromURI(s.URI)
	}
	results.Artists = c.batchFetchArtists(r, artistIDs)

	sub := spec.NewResponse()
	sub.StarredTwo = results
	return sub
}
