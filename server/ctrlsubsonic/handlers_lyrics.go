package ctrlsubsonic

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
	"go.senan.xyz/gonic/tidalproxy"
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

	// Search for tracks matching artist + title
	query := artist + " " + title
	tracks, err := c.proxy.SearchTracks(r.Context(), query, 5, 0)
	if err != nil || len(tracks) == 0 {
		// return empty lyrics if no track found
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	// Use first matching track
	trackID := tracks[0].ID
	lyrics, err := c.proxy.GetLyrics(r.Context(), trackID)
	if err != nil {
		// return empty lyrics list if not found
		sub := spec.NewResponse()
		sub.LyricsList = &spec.LyricsList{}
		return sub
	}

	specLines := parseLyricsLines(lyrics)

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

// lrcRegex matches LRC format timestamps like [mm:ss.xx] or [mm:ss.xxx]
var lrcRegex = regexp.MustCompile(`^\[(\d{2}):(\d{2}\.\d{2,3})\]\s*(.*)$`)

// parseLyricsLines parses Tidal lyrics, using synced subtitles (LRC format) if available
func parseLyricsLines(lyrics *tidalproxy.TidalLyrics) []spec.Lyric {
	var specLines []spec.Lyric

	// Use synced subtitles if available (LRC format)
	if lyrics.Subtitles != "" {
		lines := strings.Split(lyrics.Subtitles, "\n")
		for _, line := range lines {
			matches := lrcRegex.FindStringSubmatch(line)
			if len(matches) == 4 {
				// Parse minutes and seconds
				min, _ := strconv.Atoi(matches[1])
				sec, _ := strconv.ParseFloat(matches[2], 64)
				// Convert to milliseconds
				startMs := int64(min*60*1000 + int(sec*1000))
				specLines = append(specLines, spec.Lyric{
					Start: &startMs,
					Value: matches[3],
				})
			} else if line != "" {
				// Line without timestamp, add without Start
				specLines = append(specLines, spec.Lyric{Value: line})
			}
		}
	} else {
		// Fallback to plain lyrics without timestamps
		lines := strings.Split(lyrics.Lyrics, "\n")
		for _, l := range lines {
			specLines = append(specLines, spec.Lyric{Value: l})
		}
	}

	return specLines
}

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

	specLines := parseLyricsLines(lyrics)

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
