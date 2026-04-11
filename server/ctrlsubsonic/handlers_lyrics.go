package ctrlsubsonic

import (
	"net/http"
	"strings"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeGetLyricsBySongID(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	lyrics, err := c.proxy.GetLyrics(r.Context(), id.Value())
	if err != nil {
		// return empty lyrics list if not found
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	lines := strings.Split(lyrics.Lyrics, "\n")
	var specLines []spec.Lyric
	for _, l := range lines {
		specLines = append(specLines, spec.Lyric{Value: l})
	}

	sub := spec.NewResponse()
	sub.LyricsList = &spec.LyricsList{
		StructuredLyrics: []*spec.StructuredLyrics{
			{
				Lang:   "und",
				Synced: lyrics.Subtitles != "",
				Lines:  specLines,
			},
		},
	}
	return sub
}
