package ctrlsubsonic

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

func (c *Controller) ServeStream(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Track {
		return spec.NewError(10, "provide a track `id` parameter")
	}

	quality := p.GetOr("maxBitRate", "")
	tidalQuality := ""
	switch {
	case quality == "" || quality == "0":
		tidalQuality = "" // use pool default (HI_RES_LOSSLESS)
	case quality <= "128":
		tidalQuality = "LOW"
	case quality <= "320":
		tidalQuality = "HIGH"
	default:
		tidalQuality = "LOSSLESS"
	}

	streamURL, err := c.proxy.GetStreamURL(r.Context(), id.Value, tidalQuality)
	if err != nil {
		log.Printf("stream error for track %d: %v", id.Value, err)
		return spec.NewError(0, "error getting stream URL: %v", err)
	}

	http.Redirect(w, r, streamURL, http.StatusTemporaryRedirect)
	return nil
}

func (c *Controller) ServeGetCoverArt(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	id, err := p.GetID("id")
	if err != nil {
		log.Printf("[SUBS] getCoverArt: invalid id %q", p.GetOr("id", ""))
		w.WriteHeader(http.StatusNotFound)
		return nil
	}


	size := p.GetOrInt("size", 600)
	// clamp size to tidal-supported values
	switch {
	case size <= 80:
		size = 80
	case size <= 160:
		size = 160
	case size <= 320:
		size = 320
	case size <= 640:
		size = 640
	default:
		size = 1280
	}

	// check disk cache first
	cacheKey := fmt.Sprintf("%s-%d-%d", id.Type, id.Value, size)
	cachePath := filepath.Join(c.cachePath, cacheKey+".jpg")
	if data, err := os.ReadFile(cachePath); err == nil {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(data)
		return nil
	}

	// resolve cover UUID
	var coverUUID string
	switch id.Type {
	case specid.Album:
		album, err := c.proxy.GetAlbumInfo(r.Context(), id.Value)
		if err == nil {
			coverUUID = album.Cover
		}
	case specid.Artist:
		info, err := c.proxy.GetArtistInfo(r.Context(), id.Value)
		if err == nil {
			coverUUID = info.Artist.Picture
		}
	case specid.Track:
		track, err := c.proxy.GetTrackInfo(r.Context(), id.Value)
		if err == nil {
			coverUUID = track.Album.Cover
		}
	}

	if coverUUID == "" {
		return spec.NewError(70, "cover art not found")
	}

	// build CDN URL and proxy
	coverURL := c.proxy.GetCoverURL(coverUUID, size)
	if coverURL == "" {
		return spec.NewError(70, "cover art not found")
	}

	resp, err := http.Get(coverURL)
	if err != nil || resp.StatusCode != http.StatusOK {
		return spec.NewError(0, "error fetching cover art")
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return spec.NewError(0, "error reading cover art")
	}

	// cache to disk
	_ = os.MkdirAll(c.cachePath, 0o755)
	_ = os.WriteFile(cachePath, data, 0o644)

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "image/jpeg"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Write(data)
	return nil
}

// coverSlug converts a Tidal cover UUID to a URL path slug
func coverSlug(uuid string) string {
	return strings.ReplaceAll(uuid, "-", "/")
}
