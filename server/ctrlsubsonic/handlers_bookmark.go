package ctrlsubsonic

import (
	"net/http"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetBookmarks(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	var bookmarks []db.Bookmark
	c.dbc.Where("user_id=?", user.ID).Order("updated_at DESC").Find(&bookmarks)

	// Extract IDs from URIs for fetching track metadata
	tidalIDs := make([]int, len(bookmarks))
	for i, bm := range bookmarks {
		tidalIDs[i] = extractIDFromURI(bm.URI)
	}
	tracks := c.batchFetchTracks(r, tidalIDs)

	sub := spec.NewResponse()
	sub.Bookmarks = &spec.Bookmarks{
		List: make([]*spec.Bookmark, 0, len(tracks)),
	}

	for i, tc := range tracks {
		if tc == nil {
			continue
		}
		bm := bookmarks[i]
		sub.Bookmarks.List = append(sub.Bookmarks.List, &spec.Bookmark{
			Entry:    tc,
			Username: user.Name,
			Position: bm.Position,
			Comment:  bm.Comment,
			Created:  bm.CreatedAt,
			Changed:  bm.UpdatedAt,
		})
	}
	return sub
}

func (c *Controller) ServeCreateBookmark(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	position := p.GetOrInt("position", 0)
	comment := p.GetOr("comment", "")
	uri := id.String()

	c.dbc.Where("user_id=? AND uri=?", user.ID, uri).
		Assign(db.Bookmark{Position: position, Comment: comment, UpdatedAt: time.Now()}).
		FirstOrCreate(&db.Bookmark{UserID: user.ID, URI: uri, Provider: "tidal"})

	return spec.NewResponse()
}

func (c *Controller) ServeDeleteBookmark(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	uri := id.String()
	c.dbc.Where("user_id=? AND uri=?", user.ID, uri).Delete(&db.Bookmark{})

	return spec.NewResponse()
}
