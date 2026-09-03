package ctrlsubsonic

import (
	"net/http"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetLyrics(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	artist, err := p.Get("artist")
	if err != nil {
		return spec.NewError(10, "provide an `artist` parameter")
	}
	title, err := p.Get("title")
	if err != nil {
		return spec.NewError(10, "provide a `title` parameter")
	}

	prov := c.providers.Default()
	if prov == nil {
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	// Search for tracks matching artist + title
	query := artist + " " + title
	tracks, err := prov.SearchTracks(r.Context(), query, 5, 0)
	if err != nil || len(tracks) == 0 {
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	// Use first matching track
	structured, err := prov.GetLyrics(r.Context(), tracks[0].ID.RawID())
	if err != nil || structured == nil {
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	sub := spec.NewResponse()
	sub.LyricsList = &spec.LyricsList{
		StructuredLyrics: []*spec.StructuredLyrics{structured},
	}
	return sub
}

func (c *Controller) ServeGetLyricsBySongID(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	prov := c.getProvider(id.Provider())
	if prov == nil {
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	structured, err := prov.GetLyrics(r.Context(), id.RawID())
	if err != nil || structured == nil {
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	sub := spec.NewResponse()
	sub.LyricsList = &spec.LyricsList{
		StructuredLyrics: []*spec.StructuredLyrics{structured},
	}
	return sub
}
