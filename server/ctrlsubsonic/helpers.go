package ctrlsubsonic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/tidalproxy"
)

// batchFetchTracks fetches metadata for multiple tidal track IDs concurrently
// with a semaphore to limit parallelism. Failed fetches are silently skipped.
func (c *Controller) batchFetchTracks(r *http.Request, tidalIDs []int) []*spec.TrackChild {
	user := r.Context().Value(CtxUser).(*db.User)

	if len(tidalIDs) == 0 {
		return nil
	}

	type result struct {
		idx   int
		track *spec.TrackChild
	}

	results := make(chan result, len(tidalIDs))
	var wg sync.WaitGroup

	for i, id := range tidalIDs {
		wg.Add(1)
		go func(idx, tid int) {
			defer wg.Done()
			c.proxySem <- struct{}{}
			defer func() { <-c.proxySem }()

			track, err := c.proxy.GetTrackInfo(r.Context(), tid)
			if err != nil {
				return
			}
			results <- result{idx: idx, track: spec.NewTrackFromTidal(track)}
		}(i, id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// collect results preserving order
	ordered := make([]*spec.TrackChild, len(tidalIDs))
	for r := range results {
		ordered[r.idx] = r.track
	}

	// filter out nils (failed fetches)
	var tracks []*spec.TrackChild
	for i, t := range ordered {
		if t != nil {
			c.applyTrackStar(user.ID, t)
			c.applyTrackPlayCount(user.ID, t)
			uri := fmt.Sprintf("td:tr:%d", tidalIDs[i])
			t.UserRating = c.getTrackRating(user.ID, uri)
			tracks = append(tracks, t)
		}
	}
	return tracks
}

// batchFetchAlbums fetches metadata for multiple tidal album IDs concurrently
func (c *Controller) batchFetchAlbums(r *http.Request, tidalIDs []int) []*spec.Album {
	user := r.Context().Value(CtxUser).(*db.User)
	if len(tidalIDs) == 0 {
		return nil
	}

	type result struct {
		idx   int
		album *spec.Album
	}

	results := make(chan result, len(tidalIDs))
	var wg sync.WaitGroup

	for i, id := range tidalIDs {
		wg.Add(1)
		go func(idx, tid int) {
			defer wg.Done()
			c.proxySem <- struct{}{}
			defer func() { <-c.proxySem }()

			info, err := c.proxy.GetAlbumInfo(r.Context(), tid)
			if err != nil {
				return
			}
			a := spec.NewAlbumFromTidal(info)
			results <- result{idx: idx, album: a}
		}(i, id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	ordered := make([]*spec.Album, len(tidalIDs))
	for r := range results {
		ordered[r.idx] = r.album
	}

	var albums []*spec.Album
	for i, a := range ordered {
		if a != nil {
			c.applyAlbumStar(user.ID, a)
			uri := fmt.Sprintf("td:al:%d", tidalIDs[i])
			a.UserRating = c.getAlbumRating(user.ID, uri)
			albums = append(albums, a)
		}
	}
	return albums
}

// batchFetchAlbumsWithContext fetches metadata with custom context (for timeout control)
func (c *Controller) batchFetchAlbumsWithContext(ctx context.Context, tidalIDs []int) []*spec.Album {
	if len(tidalIDs) == 0 {
		return nil
	}

	type result struct {
		idx   int
		album *spec.Album
	}

	results := make(chan result, len(tidalIDs))
	var wg sync.WaitGroup

	for i, id := range tidalIDs {
		wg.Add(1)
		go func(idx, tid int) {
			defer wg.Done()
			
			info, err := c.proxy.GetAlbumInfo(ctx, tid)
			if err != nil {
				return
			}
			a := spec.NewAlbumFromTidal(info)
			results <- result{idx: idx, album: a}
		}(i, id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	ordered := make([]*spec.Album, len(tidalIDs))
	for r := range results {
		ordered[r.idx] = r.album
	}

	var albums []*spec.Album
	for _, a := range ordered {
		if a != nil {
			albums = append(albums, a)
		}
	}
	return albums
}

// getTrackPlayCount returns the play count for a track from local DB
// Note: Now accepts URI string instead of numeric tidalID
func (c *Controller) getTrackPlayCount(userID int, uri string) int {
	var play db.Play
	if c.dbc.Where("user_id=? AND uri=?", userID, uri).First(&play).Error == nil {
		return play.Count
	}
	return 0
}

// batchFetchArtists fetches metadata for multiple tidal artist IDs concurrently
// includes album count for each artist
func (c *Controller) batchFetchArtists(r *http.Request, tidalIDs []int) []*spec.Artist {
	user := r.Context().Value(CtxUser).(*db.User)
	if len(tidalIDs) == 0 {
		return nil
	}

	type result struct {
		idx    int
		artist *spec.Artist
	}

	results := make(chan result, len(tidalIDs))
	var wg sync.WaitGroup

	for i, id := range tidalIDs {
		wg.Add(1)
		go func(idx, tid int) {
			defer wg.Done()
			c.proxySem <- struct{}{}
			defer func() { <-c.proxySem }()

			// Get artist info
			info, err := c.proxy.GetArtistInfo(r.Context(), tid)
			if err != nil {
				return
			}
			a := spec.NewArtistFromTidal(&info.Artist)

			// Get album count from cache (avoids extra API call)
			a.AlbumCount = c.proxy.GetArtistAlbumCount(r.Context(), tid)

			results <- result{idx: idx, artist: a}
		}(i, id)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	ordered := make([]*spec.Artist, len(tidalIDs))
	for r := range results {
		ordered[r.idx] = r.artist
	}

	var artists []*spec.Artist
	for _, a := range ordered {
		if a != nil {
			c.applyArtistStar(user.ID, a)
			artists = append(artists, a)
		}
	}
	return artists
}

// parseTidalIDs parses a JSON array string of tidal IDs
func parseTidalIDs(jsonStr string) []int {
	var ids []int
	_ = json.Unmarshal([]byte(jsonStr), &ids)
	return ids
}

// encodeTidalIDs encodes a slice of tidal IDs to JSON
func encodeTidalIDs(ids []int) string {
	if len(ids) == 0 {
		return "[]"
	}
	data, _ := json.Marshal(ids)
	return string(data)
}

// scrobbleTrackFromTidal converts a TidalTrack to a scrobble.Track
func scrobbleTrackFromTidal(t *tidalproxy.TidalTrack) scrobble.Track {
	artist := t.Artist.Name
	if len(t.Artists) > 0 {
		artist = t.Artists[0].Name
	}
	return scrobble.Track{
		Track:       t.Title,
		Artist:      artist,
		Album:       t.Album.Title,
		AlbumArtist: artist,
		TrackNumber: uint(t.TrackNumber),
		Duration:    time.Duration(t.Duration) * time.Second,
	}
}

// parseURIList parses a JSON array string of URIs
func parseURIList(jsonStr string) []string {
	var uris []string
	_ = json.Unmarshal([]byte(jsonStr), &uris)
	return uris
}

// encodeURIs encodes a slice of URIs to JSON
func encodeURIs(uris []string) string {
	if len(uris) == 0 {
		return "[]"
	}
	data, _ := json.Marshal(uris)
	return string(data)
}

// extractIDsFromURIs extracts numeric IDs from a slice of URIs (e.g., ["td:tr:123"] -> [123])
func extractIDsFromURIs(uris []string) []int {
	ids := make([]int, 0, len(uris))
	for _, uri := range uris {
		id := extractIDFromURI(uri)
		if id > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

// extractIDFromURI extracts the numeric ID from a URI string (e.g., "td:tr:12345" -> 12345)
func extractIDFromURI(uri string) int {
	if uri == "" {
		return 0
	}
	parts := strings.Split(uri, ":")
	if len(parts) >= 3 {
		id, _ := strconv.Atoi(parts[2])
		return id
	}
	return 0
}
