package ctrlsubsonic

import (
	"net/http"
	"strings"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetArtists(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	// return starred artists only (we don't have a local index)
	var stars []db.ArtistStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Find(&stars)

	indexMap := make(map[string]*spec.Index)
	var indexes []*spec.Index

	for _, s := range stars {
		info, err := c.proxy.GetArtistInfo(r.Context(), s.TidalID)
		if err != nil {
			continue
		}
		a := spec.NewArtistFromTidal(&info.Artist)
		a.Starred = &s.StarDate

		key := "#"
		if len(a.Name) > 0 {
			ch := []rune(a.Name)[0]
			if ch >= 'a' && ch <= 'z' {
				key = string(ch)
			} else if ch >= 'A' && ch <= 'Z' {
				key = string(ch + 32)
			}
		}

		if _, ok := indexMap[key]; !ok {
			idx := &spec.Index{Name: key, Artists: []*spec.Artist{}}
			indexMap[key] = idx
			indexes = append(indexes, idx)
		}
		indexMap[key].Artists = append(indexMap[key].Artists, a)
	}

	sub := spec.NewResponse()
	sub.Artists = &spec.Artists{List: indexes}
	return sub
}

func (c *Controller) ServeGetArtist(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Artist {
		return spec.NewError(10, "please provide an artist `id` parameter")
	}

	// fetch artist info + albums
	info, err := c.proxy.GetArtistInfo(r.Context(), id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching artist: %v", err)
	}

	artistPage, err := c.proxy.GetArtistAlbums(r.Context(), id.Value, true)
	if err != nil {
		return spec.NewError(0, "error fetching artist albums: %v", err)
	}

	artist := spec.NewArtistFromTidal(&info.Artist)
	artist.AlbumCount = len(artistPage.Albums.Items)
	artist.Albums = make([]*spec.Album, len(artistPage.Albums.Items))
	for i := range artistPage.Albums.Items {
		artist.Albums[i] = spec.NewAlbumFromTidal(&artistPage.Albums.Items[i])
	}

	sub := spec.NewResponse()
	sub.Artist = artist
	return sub
}

func (c *Controller) ServeGetAlbum(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Album {
		return spec.NewError(10, "please provide an album `id` parameter")
	}

	album, err := c.proxy.GetAlbumInfo(r.Context(), id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching album: %v", err)
	}

	a := spec.NewAlbumFromTidal(album)
	a.TrackCount = len(album.Items)
	a.Tracks = make([]*spec.TrackChild, len(album.Items))

	totalDuration := 0
	for i := range album.Items {
		tc := spec.NewTrackFromTidal(&album.Items[i])
		// fill in album context that track might be missing
		if tc.Album == "" {
			tc.Album = album.Title
		}
		if tc.AlbumID == nil {
			tc.AlbumID = a.ID
		}
		c.applyTrackStar(user.ID, tc)
		tc.UserRating = c.getTrackRating(user.ID, album.Items[i].ID)
		a.Tracks[i] = tc
		totalDuration += tc.Duration
	}
	if a.Duration == 0 {
		a.Duration = totalDuration
	}

	c.applyAlbumStar(user.ID, a)
	a.UserRating = c.getAlbumRating(user.ID, id.Value)

	sub := spec.NewResponse()
	sub.Album = a
	return sub
}

func (c *Controller) ServeGetAlbumListTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	listType, err := p.Get("type")
	if err != nil {
		return spec.NewError(10, "please provide a `type` parameter")
	}

	size := p.GetOrInt("size", 10)
	offset := p.GetOrInt("offset", 0)

	var albumIDs []int

	switch listType {
	case "starred":
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, s.TidalID)
		}

	case "recent":
		var plays []db.Play
		c.dbc.Select("DISTINCT tidal_id").
			Where("user_id=?", user.ID).
			Order("played_at DESC").
			Offset(offset).Limit(size).
			Find(&plays)
		// plays table stores track IDs, need to get album IDs from tracks
		for _, p := range plays {
			track, err := c.proxy.GetTrackInfo(r.Context(), p.TidalID)
			if err != nil {
				continue
			}
			albumIDs = appendUnique(albumIDs, track.Album.ID)
		}

	case "frequent":
		var plays []db.Play
		c.dbc.Select("DISTINCT tidal_id").
			Where("user_id=?", user.ID).
			Order("count DESC").
			Offset(offset).Limit(size*2).
			Find(&plays)
		for _, p := range plays {
			track, err := c.proxy.GetTrackInfo(r.Context(), p.TidalID)
			if err != nil {
				continue
			}
			albumIDs = appendUnique(albumIDs, track.Album.ID)
			if len(albumIDs) >= size {
				break
			}
		}

	case "newest":
		// newest starred albums
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, s.TidalID)
		}

	case "random":
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("RANDOM()").
			Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, s.TidalID)
		}

	case "highest":
		var ratings []db.AlbumRating
		c.dbc.Where("user_id=?", user.ID).
			Order("rating DESC").
			Offset(offset).Limit(size).
			Find(&ratings)
		for _, r := range ratings {
			albumIDs = append(albumIDs, r.TidalID)
		}

	default:
		// unsupported types return empty
	}

	// batch fetch album metadata
	albums := c.batchFetchAlbums(r, albumIDs)

	sub := spec.NewResponse()
	sub.AlbumsTwo = &spec.Albums{List: albums}
	return sub
}

func (c *Controller) ServeGetSong(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Track {
		return spec.NewError(10, "please provide a track `id` parameter")
	}

	track, err := c.proxy.GetTrackInfo(r.Context(), id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching track: %v", err)
	}

	tc := spec.NewTrackFromTidal(track)
	c.applyTrackStar(user.ID, tc)
	tc.UserRating = c.getTrackRating(user.ID, id.Value)

	sub := spec.NewResponse()
	sub.Track = tc
	return sub
}

func appendUnique(slice []int, val int) []int {
	for _, v := range slice {
		if v == val {
			return slice
		}
	}
	return append(slice, val)
}

func (c *Controller) ServeGetArtistInfoTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Artist {
		return spec.NewError(10, "please provide an artist `id` parameter")
	}

	info, err := c.proxy.GetArtistInfo(r.Context(), id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching artist info")
	}

	similar, _ := c.proxy.GetSimilarArtists(r.Context(), id.Value)

	artistInfo := &spec.ArtistInfo{
		Biography:      "",
		SmallImageURL:  c.proxy.GetCoverURL(info.Artist.Picture, 320),
		MediumImageURL: c.proxy.GetCoverURL(info.Artist.Picture, 640),
		LargeImageURL:  c.proxy.GetCoverURL(info.Artist.Picture, 1280),
		ArtistImageURL: c.proxy.GetCoverURL(info.Artist.Picture, 1280),
	}

	user := r.Context().Value(CtxUser).(*db.User)
	for _, a := range similar {
		sa := spec.NewArtistFromTidal(&a)
		c.applyArtistStar(user.ID, sa)
		artistInfo.Similar = append(artistInfo.Similar, sa)
	}

	sub := spec.NewResponse()
	if strings.Contains(r.URL.Path, "getArtistInfo2") {
		sub.ArtistInfoTwo = artistInfo
	} else {
		sub.ArtistInfo = artistInfo
	}
	return sub
}

func (c *Controller) ServeGetAlbumInfoTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Album {
		return spec.NewError(10, "please provide an album `id` parameter")
	}

	albumInfo := &spec.AlbumInfo{
		Notes:         "",
		MusicBrainzID: "",
		LastFMURL:     "",
	}

	sub := spec.NewResponse()
	sub.AlbumInfo = albumInfo
	return sub
}
