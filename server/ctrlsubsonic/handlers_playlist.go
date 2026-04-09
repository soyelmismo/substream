package ctrlsubsonic

import (
	"net/http"
	"strconv"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetPlaylists(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	var playlists []db.Playlist
	c.dbc.Where("user_id=? OR is_public=?", user.ID, true).
		Order("updated_at DESC").
		Find(&playlists)

	sub := spec.NewResponse()
	sub.Playlists = &spec.Playlists{
		List: make([]*spec.Playlist, len(playlists)),
	}

	for i, pl := range playlists {
		var count int
		c.dbc.Model(&db.PlaylistTrack{}).Where("playlist_id=?", pl.ID).Count(&count)

		// get owner name
		var owner db.User
		c.dbc.Where("id=?", pl.UserID).First(&owner)

		sub.Playlists.List[i] = &spec.Playlist{
			ID:        specid.ID{Type: specid.Playlist, StringValue: strconv.Itoa(pl.ID)},
			Name:      pl.Name,
			Comment:   pl.Comment,
			Owner:     owner.Name,
			SongCount: count,
			Created:   pl.CreatedAt,
			Changed:   pl.UpdatedAt,
			Public:    pl.IsPublic,
		}
	}

	return sub
}

func (c *Controller) ServeGetPlaylist(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "provide an `id` parameter")
	}

	playlistID, _ := strconv.Atoi(id.StringValue)
	if playlistID == 0 {
		playlistID = id.Value
	}

	var pl db.Playlist
	if err := c.dbc.Where("id=?", playlistID).First(&pl).Error; err != nil {
		return spec.NewError(70, "playlist not found")
	}

	// check access
	if pl.UserID != user.ID && !pl.IsPublic {
		return spec.NewError(50, "not authorized")
	}

	var tracks []db.PlaylistTrack
	c.dbc.Where("playlist_id=?", pl.ID).Order("position ASC").Find(&tracks)

	tidalIDs := make([]int, len(tracks))
	for i, t := range tracks {
		tidalIDs[i] = t.TidalID
	}

	// get owner name
	var owner db.User
	c.dbc.Where("id=?", pl.UserID).First(&owner)

	trackList := c.batchFetchTracks(r, tidalIDs)

	totalDuration := 0
	for _, tc := range trackList {
		totalDuration += tc.Duration
	}

	sub := spec.NewResponse()
	sub.Playlist = &spec.Playlist{
		ID:        specid.ID{Type: specid.Playlist, StringValue: strconv.Itoa(pl.ID)},
		Name:      pl.Name,
		Comment:   pl.Comment,
		Owner:     owner.Name,
		SongCount: len(trackList),
		Duration:  totalDuration,
		Created:   pl.CreatedAt,
		Changed:   pl.UpdatedAt,
		Public:    pl.IsPublic,
		List:      trackList,
	}

	return sub
}

func (c *Controller) ServeCreatePlaylist(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	name := p.GetOr("name", "New Playlist")

	// if playlistId is provided, this is an update
	if plID, err := p.GetID("playlistId"); err == nil {
		playlistID, _ := strconv.Atoi(plID.StringValue)
		if playlistID == 0 {
			playlistID = plID.Value
		}

		var pl db.Playlist
		if c.dbc.Where("id=? AND user_id=?", playlistID, user.ID).First(&pl).Error == nil {
			pl.Name = name
			pl.UpdatedAt = time.Now()
			c.dbc.Save(&pl)

			// replace tracks if songId provided
			if songIDs, err := p.GetIDList("songId"); err == nil {
				c.dbc.Where("playlist_id=?", pl.ID).Delete(&db.PlaylistTrack{})
				for i, sid := range songIDs {
					if sid.Type == specid.Track {
						c.dbc.Create(&db.PlaylistTrack{
							PlaylistID: pl.ID,
							TidalID:    sid.Value,
							Position:   i,
						})
					}
				}
			}

			return spec.NewResponse()
		}
	}

	// create new
	pl := db.Playlist{
		UserID:    user.ID,
		Name:      name,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	c.dbc.Create(&pl)

	// add tracks if songId provided
	if songIDs, err := p.GetIDList("songId"); err == nil {
		for i, sid := range songIDs {
			if sid.Type == specid.Track {
				c.dbc.Create(&db.PlaylistTrack{
					PlaylistID: pl.ID,
					TidalID:    sid.Value,
					Position:   i,
				})
			}
		}
	}

	sub := spec.NewResponse()
	sub.Playlist = &spec.Playlist{
		ID:      specid.ID{Type: specid.Playlist, StringValue: strconv.Itoa(pl.ID)},
		Name:    pl.Name,
		Owner:   user.Name,
		Created: pl.CreatedAt,
		Changed: pl.UpdatedAt,
	}
	return sub
}

func (c *Controller) ServeUpdatePlaylist(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	plID, err := p.GetID("playlistId")
	if err != nil {
		return spec.NewError(10, "provide a `playlistId` parameter")
	}

	playlistID, _ := strconv.Atoi(plID.StringValue)
	if playlistID == 0 {
		playlistID = plID.Value
	}

	var pl db.Playlist
	if err := c.dbc.Where("id=? AND user_id=?", playlistID, user.ID).First(&pl).Error; err != nil {
		return spec.NewError(70, "playlist not found")
	}

	if name := p.GetOr("name", ""); name != "" {
		pl.Name = name
	}
	if comment := p.GetOr("comment", ""); comment != "" {
		pl.Comment = comment
	}
	if pub, err := p.GetBool("public"); err == nil {
		pl.IsPublic = pub
	}

	// add tracks
	if songIDsToAdd, err := p.GetIDList("songIdToAdd"); err == nil {
		var maxPos int
		c.dbc.Model(&db.PlaylistTrack{}).Where("playlist_id=?", pl.ID).
			Select("COALESCE(MAX(position), -1)").Row().Scan(&maxPos)
		for i, sid := range songIDsToAdd {
			if sid.Type == specid.Track {
				c.dbc.Create(&db.PlaylistTrack{
					PlaylistID: pl.ID,
					TidalID:    sid.Value,
					Position:   maxPos + 1 + i,
				})
			}
		}
	}

	// remove tracks by index
	if idxToRemove, err := p.GetIntList("songIndexToRemove"); err == nil {
		var tracks []db.PlaylistTrack
		c.dbc.Where("playlist_id=?", pl.ID).Order("position ASC").Find(&tracks)
		for _, idx := range idxToRemove {
			if idx >= 0 && idx < len(tracks) {
				c.dbc.Delete(&tracks[idx])
			}
		}
		// reindex positions
		var remaining []db.PlaylistTrack
		c.dbc.Where("playlist_id=?", pl.ID).Order("position ASC").Find(&remaining)
		for i := range remaining {
			remaining[i].Position = i
			c.dbc.Save(&remaining[i])
		}
	}

	pl.UpdatedAt = time.Now()
	c.dbc.Save(&pl)

	return spec.NewResponse()
}

func (c *Controller) ServeDeletePlaylist(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "provide an `id` parameter")
	}

	playlistID, _ := strconv.Atoi(id.StringValue)
	if playlistID == 0 {
		playlistID = id.Value
	}

	var pl db.Playlist
	if err := c.dbc.Where("id=? AND user_id=?", playlistID, user.ID).First(&pl).Error; err != nil {
		return spec.NewError(70, "playlist not found")
	}

	c.dbc.Where("playlist_id=?", pl.ID).Delete(&db.PlaylistTrack{})
	c.dbc.Delete(&pl)

	return spec.NewResponse()
}
