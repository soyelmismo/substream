package ctrlsubsonic

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServePing(_ *http.Request) *spec.Response {
	return spec.NewResponse()
}

func (c *Controller) ServeGetLicence(_ *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.Licence = &spec.Licence{Valid: true}
	return sub
}

func (c *Controller) ServeGetMusicFolders(_ *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.MusicFolders = &spec.MusicFolders{
		List: []*spec.MusicFolder{
			{ID: 1, Name: "My Library"},
		},
	}
	return sub
}

func (c *Controller) ServeGetScanStatus(_ *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.ScanStatus = &spec.ScanStatus{Scanning: false, Count: 0}
	return sub
}

func (c *Controller) ServeGetOpenSubsonicExtensions(_ *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.OpenSubsonicExtensions = &spec.OpenSubsonicExtensions{
		{Name: "formPost", Versions: []int{1}},
	}
	return sub
}

func (c *Controller) ServeGetUser(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	sub := spec.NewResponse()
	sub.User = &spec.User{
		Username:     user.Name,
		AdminRole:    user.IsAdmin,
		DownloadRole: true,
		StreamRole:   true,
		PlaylistRole: true,
		CoverArtRole: true,
		SettingsRole: true,
		Folder:       []int{0},
	}
	return sub
}

// User management stubs to satisfy clients
func (c *Controller) ServeGetUsers(r *http.Request) *spec.Response {
	return spec.NewResponse()
}

func (c *Controller) ServeCreateUser(r *http.Request) *spec.Response { return spec.NewResponse() }
func (c *Controller) ServeDeleteUser(r *http.Request) *spec.Response { return spec.NewResponse() }
func (c *Controller) ServeChangePassword(r *http.Request) *spec.Response { return spec.NewResponse() }

func (c *Controller) ServeNotFound(_ *http.Request) *spec.Response {
	return spec.NewError(70, "view not found")
}

func (c *Controller) ServeScrobble(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}
	uri := id.String()
	trackID := id.Value()

	submissionStr, _ := p.Get("submission")
	isSubmission := true
	if submissionStr == "false" || submissionStr == "0" {
		isSubmission = false
	}

	log.Printf("[SCROBBLE] track=%s user=%s submission=%s isSubmission=%v", uri, user.Name, submissionStr, isSubmission)

	if isSubmission {
		// Auto-ingest: Record track metadata for virtual library (same as ServeStream)
		track, _ := c.proxy.GetTrackInfo(r.Context(), trackID)
		if track != nil {
			albumURI := fmt.Sprintf("td:al:%d", track.Album.ID)
			artistURI := fmt.Sprintf("td:ar:%d", track.Artist.ID)

			// Insert or update track_metadata
			c.dbc.Exec(`
				INSERT OR REPLACE INTO track_metadata (uri, album_uri, artist_uri, updated_at)
				VALUES (?, ?, ?, ?)
			`, uri, albumURI, artistURI, time.Now())
			log.Printf("[SCROBBLE] Recorded track_metadata for %s -> album=%s artist=%s", uri, albumURI, artistURI)
		}

		// record play in local DB
		var play db.Play
		c.dbc.Where("user_id=? AND uri=?", user.ID, uri).First(&play)
		oldCount := play.Count
		if play.ID == 0 {
			play.UserID = user.ID
			play.URI = uri
			play.Provider = "tidal"
			play.PlayedAt = time.Now()
			play.Count = 1
			log.Printf("[SCROBBLE] NEW play record for track=%s user=%s count=1", uri, user.Name)
		} else {
			play.Count++
			play.PlayedAt = time.Now()
			log.Printf("[SCROBBLE] UPDATED play record for track=%s user=%s count=%d (was %d)", uri, user.Name, play.Count, oldCount)
		}
		if err := c.dbc.Save(&play).Error; err != nil {
			log.Printf("[SCROBBLE] ERROR saving play record: %v", err)
		} else {
			log.Printf("[SCROBBLE] SAVED play record for track=%s count=%d", uri, play.Count)
		}

		// Update album play stats if album is favorited
		if track != nil && track.Album.ID != 0 {
			var albumStar db.AlbumStar
			albumURI := fmt.Sprintf("td:al:%d", track.Album.ID)
			if c.dbc.Where("user_id=? AND uri=?", user.ID, albumURI).First(&albumStar).Error == nil {
				albumStar.LastPlayed = time.Now()
				albumStar.PlayCount++
				c.dbc.Save(&albumStar)
				log.Printf("[SCROBBLE] Updated album %d stats: play_count=%d", track.Album.ID, albumStar.PlayCount)
			}
		}
	}

	// fetch track info for scrobbling (fire and forget if fails)
	track, err := c.proxy.GetTrackInfo(r.Context(), trackID)
	if err == nil && track != nil {
		for _, s := range c.scrobblers {
			if s.IsUserAuthenticated(*user) {
				_ = s.Scrobble(*user, scrobbleTrackFromTidal(track), time.Now(), isSubmission)
			}
		}
	}

	return spec.NewResponse()
}

func (c *Controller) ServeGetPlayQueue(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	var queue db.PlayQueue
	if err := c.dbc.Where("user_id=?", user.ID).First(&queue).Error; err != nil {
		return spec.NewResponse()
	}

	sub := spec.NewResponse()
	sub.PlayQueue = &spec.PlayQueue{
		Username:  user.Name,
		Position:  queue.Position,
		Changed:   queue.UpdatedAt,
		ChangedBy: queue.ChangedBy,
	}

	if queue.CurrentURI != "" {
		sub.PlayQueue.Current = &specid.ID{URI: queue.CurrentURI}
	}

	// parse items JSON (now URIs) and batch-fetch metadata
	itemURIs := parseURIList(queue.Items)
	sub.PlayQueue.List = c.batchFetchTracksByURIs(r, itemURIs)

	return sub
}

func (c *Controller) ServeSavePlayQueue(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	ids, _ := p.GetIDList("id")
	var itemURIs []string
	for _, id := range ids {
		if id.Type() == specid.Track {
			itemURIs = append(itemURIs, id.String())
		}
	}

	current, err := p.GetID("current")
	var currentURI string
	if err == nil && current.Type() == specid.Track {
		currentURI = current.String()
	}

	position := p.GetOrInt("position", 0)
	client := p.GetOr("c", "")

	var queue db.PlayQueue
	c.dbc.Where("user_id=?", user.ID).First(&queue)
	queue.UserID = user.ID
	queue.CurrentURI = currentURI
	queue.Position = position
	queue.ChangedBy = client
	queue.Items = encodeURIs(itemURIs)
	queue.UpdatedAt = time.Now()
	c.dbc.Save(&queue)

	return spec.NewResponse()
}
