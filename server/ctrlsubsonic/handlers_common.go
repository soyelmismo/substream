package ctrlsubsonic

import (
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
	sub.MusicFolders = &spec.MusicFolders{}
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
	if err != nil || id.Type != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	submissionStr, _ := p.Get("submission")
	isSubmission := true
	if submissionStr == "false" || submissionStr == "0" {
		isSubmission = false
	}

	if isSubmission {
		// record play in local DB
		var play db.Play
		c.dbc.Where("user_id=? AND tidal_id=?", user.ID, id.Value).First(&play)
		if play.ID == 0 {
			play.UserID = user.ID
			play.TidalID = id.Value
			play.PlayedAt = time.Now()
			play.Count = 1
		} else {
			play.Count++
			play.PlayedAt = time.Now()
		}
		c.dbc.Save(&play)
	}

	// fetch track info for scrobbling (fire and forget if fails)
	track, err := c.proxy.GetTrackInfo(r.Context(), id.Value)
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

	if queue.Current > 0 {
		currentID := &specid.ID{Type: specid.Track, Value: queue.Current}
		sub.PlayQueue.Current = currentID
	}

	// parse items JSON and batch-fetch metadata
	tidalIDs := parseTidalIDs(queue.Items)
	sub.PlayQueue.List = c.batchFetchTracks(r, tidalIDs)

	return sub
}

func (c *Controller) ServeSavePlayQueue(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	ids, _ := p.GetIDList("id")
	var tidalIDs []int
	for _, id := range ids {
		if id.Type == specid.Track {
			tidalIDs = append(tidalIDs, id.Value)
		}
	}

	current, err := p.GetID("current")
	var currentIDVal int
	if err == nil && current.Type == specid.Track {
		currentIDVal = current.Value
	}

	position := p.GetOrInt("position", 0)
	client := p.GetOr("c", "")

	var queue db.PlayQueue
	c.dbc.Where("user_id=?", user.ID).First(&queue)
	queue.UserID = user.ID
	queue.Current = currentIDVal
	queue.Position = position
	queue.ChangedBy = client
	queue.Items = encodeTidalIDs(tidalIDs)
	queue.UpdatedAt = time.Now()
	c.dbc.Save(&queue)

	return spec.NewResponse()
}
