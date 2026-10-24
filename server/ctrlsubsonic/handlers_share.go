package ctrlsubsonic

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// ServeCreateShare handles /createShare and returns canonical original web URLs for tracks and albums
func (c *Controller) ServeCreateShare(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "please provide a valid `id` parameter")
	}

	description := p.GetOr("description", "")
	targetURL := c.resolveCanonicalShareURL(r.Context(), id)

	var entry []*spec.TrackChild
	if id.Type() == specid.Track {
		if prov := c.getProvider(id.Provider()); prov != nil {
			if tc, err := prov.GetTrack(r.Context(), id.RawID()); err == nil && tc != nil {
				entry = append(entry, tc)
			}
		}
	}

	share := &spec.Share{
		ID:          id.String(),
		Url:         targetURL,
		Description: description,
		Username:    user.Name,
		Created:     time.Now(),
		Expires:     time.Now().Add(365 * 24 * time.Hour),
		Entry:       entry,
	}

	sub := spec.NewResponse()
	sub.Shares = &spec.Shares{
		Share: []*spec.Share{share},
	}
	return sub
}

// ServeGetShares handles /getShares
func (c *Controller) ServeGetShares(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.Shares = &spec.Shares{
		Share: []*spec.Share{},
	}
	return sub
}

func (c *Controller) resolveCanonicalShareURL(ctx context.Context, id specid.ID) string {
	provID := id.Provider()
	rawID := id.RawID()
	idType := id.Type()

	switch provID {
	case "td": // Tidal
		if idType == specid.Album {
			return fmt.Sprintf("https://tidal.com/album/%s", rawID)
		}
		if idType == specid.Artist {
			return fmt.Sprintf("https://tidal.com/artist/%s", rawID)
		}
		return fmt.Sprintf("https://tidal.com/track/%s", rawID)

	case "yt": // YouTube Music
		if idType == specid.Album {
			return fmt.Sprintf("https://music.youtube.com/playlist?list=%s", rawID)
		}
		if idType == specid.Artist {
			return fmt.Sprintf("https://music.youtube.com/channel/%s", rawID)
		}
		return fmt.Sprintf("https://music.youtube.com/watch?v=%s", rawID)

	case "sp": // Spotify
		if idType == specid.Album {
			return fmt.Sprintf("https://open.spotify.com/album/%s", rawID)
		}
		if idType == specid.Artist {
			return fmt.Sprintf("https://open.spotify.com/artist/%s", rawID)
		}
		return fmt.Sprintf("https://open.spotify.com/track/%s", rawID)

	case "dz": // Deezer
		if idType == specid.Album {
			return fmt.Sprintf("https://www.deezer.com/album/%s", rawID)
		}
		if idType == specid.Artist {
			return fmt.Sprintf("https://www.deezer.com/artist/%s", rawID)
		}
		return fmt.Sprintf("https://www.deezer.com/track/%s", rawID)

	case "qb": // Qobuz
		if idType == specid.Album {
			return fmt.Sprintf("https://play.qobuz.com/album/%s", rawID)
		}
		if idType == specid.Artist {
			return fmt.Sprintf("https://play.qobuz.com/artist/%s", rawID)
		}
		return fmt.Sprintf("https://play.qobuz.com/track/%s", rawID)

	case "sc": // SoundCloud
		if prov := c.getProvider("sc"); prov != nil {
			if tc, err := prov.GetTrack(ctx, rawID); err == nil && tc != nil && tc.Path != "" {
				// if permalink or url was stored
			}
		}
		return fmt.Sprintf("https://soundcloud.com/tracks/%s", rawID)

	case "mc": // Monochrome
		if prov := c.getProvider("mc"); prov != nil {
			if tc, err := prov.GetTrack(ctx, rawID); err == nil && tc != nil {
				// Si Monochrome tiene ISRC, intentamos enlazar canónicamente
				if tc.MusicBrainzID != "" {
					return fmt.Sprintf("https://musicbrainz.org/recording/%s", tc.MusicBrainzID)
				}
			}
		}
		if idType == specid.Album {
			return fmt.Sprintf("https://tracks.monochrome.st/release/%s", rawID)
		}
		return fmt.Sprintf("https://tracks.monochrome.st/track/%s", rawID)

	case "am": // Amazon Music
		if idType == specid.Album {
			return fmt.Sprintf("https://music.amazon.com/albums/%s", rawID)
		}
		return fmt.Sprintf("https://music.amazon.com/tracks/%s", rawID)

	default:
		// Fallback
		if idType == specid.Album {
			return fmt.Sprintf("https://sound.listo.click/app/#/album/%s", id.String())
		}
		return fmt.Sprintf("https://sound.listo.click/app/#/track/%s", id.String())
	}
}
