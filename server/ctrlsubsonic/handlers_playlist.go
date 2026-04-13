package ctrlsubsonic

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/internal/importer"
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

		playlist := &spec.Playlist{
			ID:        specid.ID{URI: fmt.Sprintf("td:pl:%d", pl.ID)},
			Name:      pl.Name,
			Comment:   pl.Comment,
			Owner:     owner.Name,
			SongCount: count,
			Created:   pl.CreatedAt,
			Changed:   pl.UpdatedAt,
			Public:    pl.IsPublic,
		}
		// Include cover art if playlist has an image (external URL or local path)
		if pl.CoverURL != "" || pl.CoverPath != "" {
			playlist.CoverID = &specid.ID{URI: fmt.Sprintf("pl:%d", pl.ID)}
		}
		sub.Playlists.List[i] = playlist
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

	playlistID, _ := strconv.Atoi(id.String())
	if playlistID == 0 {
		playlistID = id.Value()
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
		// Extract numeric ID from URI (td:tr:12345 -> 12345)
		tidalIDs[i] = extractIDFromURI(t.URI)
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
	playlist := &spec.Playlist{
		ID:        specid.ID{URI: fmt.Sprintf("td:pl:%d", pl.ID)},
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
	// Include cover art if playlist has an image
	if pl.CoverURL != "" || pl.CoverPath != "" {
		playlist.CoverID = &specid.ID{URI: fmt.Sprintf("pl:%d", pl.ID)}
	}
	sub.Playlist = playlist

	return sub
}

func (c *Controller) ServeCreatePlaylist(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	name := p.GetOr("name", "New Playlist")

	// Check if name is an import URL (Spotify, Apple Music, etc.)
	if c.importer.Registry().IsImportURL(name) {
		return c.handleImportPlaylist(r.Context(), user, name)
	}

	// if playlistId is provided, this is an update
	if plID, err := p.GetID("playlistId"); err == nil {
		playlistID, _ := strconv.Atoi(plID.String())
		if playlistID == 0 {
			playlistID = plID.Value()
		}

		var pl db.Playlist
		if c.dbc.Where("id=? AND user_id=?", playlistID, user.ID).First(&pl).Error == nil {
			pl.Name = name
			pl.UpdatedAt = time.Now()
			c.dbc.Save(&pl)

			// append tracks if songId provided (compatible with psysonic/navydrome behavior)
			songIDs, err := p.GetIDList("songId")
			if err == nil {
				log.Printf("[DEBUG] createPlaylist: adding %d tracks to playlist %d", len(songIDs), pl.ID)
				var maxPos int
				c.dbc.Model(&db.PlaylistTrack{}).Where("playlist_id=?", pl.ID).
					Select("COALESCE(MAX(position), -1)").Row().Scan(&maxPos)
				log.Printf("[DEBUG] createPlaylist: current max position is %d", maxPos)
				for i, sid := range songIDs {
					if sid.Type() == specid.Track {
						trackURI := sid.String()
						log.Printf("[DEBUG] createPlaylist: adding track %s at position %d", trackURI, maxPos+1+i)
						result := c.dbc.Create(&db.PlaylistTrack{
							PlaylistID: pl.ID,
							URI:        trackURI,
							Position:   maxPos + 1 + i,
						})
						if result.Error != nil {
							log.Printf("[DEBUG] createPlaylist: ERROR adding track: %v", result.Error)
						}
					}
				}
			} else {
				log.Printf("[DEBUG] createPlaylist: no songId param found or error: %v", err)
			}

			// Return updated playlist with songs (OpenSubsonic v1.14.0+ compatible)
			return c.buildPlaylistResponse(r, &pl, user)
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
			if sid.Type() == specid.Track {
				c.dbc.Create(&db.PlaylistTrack{
					PlaylistID: pl.ID,
					URI:        sid.String(),
					Position:   i,
				})
			}
		}
	}

	sub := spec.NewResponse()
	sub.Playlist = &spec.Playlist{
		ID:      specid.ID{URI: fmt.Sprintf("td:pl:%d", pl.ID)},
		Name:    pl.Name,
		Owner:   user.Name,
		Created: pl.CreatedAt,
		Changed: pl.UpdatedAt,
	}
	return sub
}

// handleImportPlaylist initiates a background import and returns immediately
func (c *Controller) handleImportPlaylist(ctx context.Context, user *db.User, sourceURL string) *spec.Response {
	// Start the import job (returns immediately with placeholder)
	pl, err := c.importer.StartImport(ctx, user.ID, sourceURL)
	if err != nil {
		return spec.NewError(0, "Failed to start import: %v", err)
	}

	// Return the placeholder playlist immediately
	sub := spec.NewResponse()
	sub.Playlist = &spec.Playlist{
		ID:      specid.ID{URI: fmt.Sprintf("td:pl:%d", pl.ID)},
		Name:    pl.Name,
		Comment: pl.Comment,
		Owner:   user.Name,
		Created: pl.CreatedAt,
		Changed: pl.UpdatedAt,
	}
	return sub
}

// Helper function to satisfy the unused import linter
var _ = importer.ImportedTrack{}
var _ = context.Background

// buildPlaylistResponse builds a full playlist response with tracks
func (c *Controller) buildPlaylistResponse(r *http.Request, pl *db.Playlist, user *db.User) *spec.Response {
	var tracks []db.PlaylistTrack
	c.dbc.Where("playlist_id=?", pl.ID).Order("position ASC").Find(&tracks)

	tidalIDs := make([]int, len(tracks))
	for i, t := range tracks {
		tidalIDs[i] = extractIDFromURI(t.URI)
	}

	trackList := c.batchFetchTracks(r, tidalIDs)

	totalDuration := 0
	for _, tc := range trackList {
		totalDuration += tc.Duration
	}

	sub := spec.NewResponse()
	playlist := &spec.Playlist{
		ID:        specid.ID{URI: fmt.Sprintf("td:pl:%d", pl.ID)},
		Name:      pl.Name,
		Comment:   pl.Comment,
		Owner:     user.Name,
		SongCount: len(trackList),
		Duration:  totalDuration,
		Created:   pl.CreatedAt,
		Changed:   pl.UpdatedAt,
		Public:    pl.IsPublic,
		List:      trackList,
	}
	// Include cover art if playlist has an image
	if pl.CoverURL != "" || pl.CoverPath != "" {
		playlist.CoverID = &specid.ID{URI: fmt.Sprintf("pl:%d", pl.ID)}
	}
	sub.Playlist = playlist
	return sub
}

func (c *Controller) ServeUpdatePlaylist(r *http.Request) *spec.Response {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[PANIC] updatePlaylist: %v", rec)
		}
	}()

	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	log.Printf("[DEBUG] updatePlaylist: method=%s params=%v", r.Method, p)

	plIDStr, _ := p.Get("playlistId")
	log.Printf("[DEBUG] updatePlaylist: raw playlistId=%s", plIDStr)

	log.Printf("[DEBUG] updatePlaylist: about to call p.GetID")
	plID, err := p.GetID("playlistId")
	log.Printf("[DEBUG] updatePlaylist: p.GetID returned, plID=%+v err=%v", plID, err)
	if err != nil {
		log.Printf("[DEBUG] updatePlaylist: playlistId error: %v", err)
		return spec.NewError(10, "provide a `playlistId` parameter")
	}

	// Extract numeric ID from URI (td:pl:123 -> 123)
	playlistID := extractIDFromURI(plID.String())
	log.Printf("[DEBUG] updatePlaylist: extracted playlistID=%d from %s", playlistID, plID.String())

	var pl db.Playlist
	if err := c.dbc.Where("id=? AND user_id=?", playlistID, user.ID).First(&pl).Error; err != nil {
		log.Printf("[DEBUG] updatePlaylist: playlist not found: %v", err)
		return spec.NewError(70, "playlist not found")
	}
	log.Printf("[DEBUG] updatePlaylist: found playlist %d (name=%s)", pl.ID, pl.Name)

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
	songIDsToAdd, err := p.GetIDList("songIdToAdd")
	log.Printf("[DEBUG] updatePlaylist: songIdToAdd=%v err=%v", songIDsToAdd, err)
	if err == nil {
		var maxPos int
		c.dbc.Model(&db.PlaylistTrack{}).Where("playlist_id=?", pl.ID).
			Select("COALESCE(MAX(position), -1)").Row().Scan(&maxPos)
		log.Printf("[DEBUG] updatePlaylist: adding %d tracks, maxPos=%d", len(songIDsToAdd), maxPos)
		for i, sid := range songIDsToAdd {
			if sid.Type() == specid.Track {
				log.Printf("[DEBUG] updatePlaylist: adding track %s at position %d", sid.String(), maxPos+1+i)
				result := c.dbc.Create(&db.PlaylistTrack{
					PlaylistID: pl.ID,
					URI:        sid.String(),
					Position:   maxPos + 1 + i,
				})
				if result.Error != nil {
					log.Printf("[DEBUG] updatePlaylist: ERROR adding track: %v", result.Error)
				}
			}
		}
	}

	// remove tracks by index
	idxToRemove, err := p.GetIntList("songIndexToRemove")
	log.Printf("[DEBUG] updatePlaylist: songIndexToRemove=%v err=%v", idxToRemove, err)
	if err == nil && len(idxToRemove) > 0 {
		var tracks []db.PlaylistTrack
		c.dbc.Where("playlist_id=?", pl.ID).Order("position ASC").Find(&tracks)
		log.Printf("[DEBUG] updatePlaylist: removing %d tracks from %d total", len(idxToRemove), len(tracks))
		for _, idx := range idxToRemove {
			if idx >= 0 && idx < len(tracks) {
				log.Printf("[DEBUG] updatePlaylist: deleting track at index %d (id=%d)", idx, tracks[idx].ID)
				c.dbc.Delete(&tracks[idx])
			} else {
				log.Printf("[DEBUG] updatePlaylist: invalid index %d, only %d tracks", idx, len(tracks))
			}
		}
		// reindex positions
		var remaining []db.PlaylistTrack
		c.dbc.Where("playlist_id=?", pl.ID).Order("position ASC").Find(&remaining)
		log.Printf("[DEBUG] updatePlaylist: reindexing %d remaining tracks", len(remaining))
		for i := range remaining {
			remaining[i].Position = i
			c.dbc.Save(&remaining[i])
		}
	}

	pl.UpdatedAt = time.Now()
	c.dbc.Save(&pl)

	log.Printf("[DEBUG] updatePlaylist: saved playlist %d, returning response", pl.ID)
	return c.buildPlaylistResponse(r, &pl, user)
}

func (c *Controller) ServeDeletePlaylist(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "provide an `id` parameter")
	}

	playlistID, _ := strconv.Atoi(id.String())
	if playlistID == 0 {
		playlistID = id.Value()
	}

	var pl db.Playlist
	if err := c.dbc.Where("id=? AND user_id=?", playlistID, user.ID).First(&pl).Error; err != nil {
		return spec.NewError(70, "playlist not found")
	}

	c.dbc.Where("playlist_id=?", pl.ID).Delete(&db.PlaylistTrack{})
	c.dbc.Delete(&pl)

	return spec.NewResponse()
}
