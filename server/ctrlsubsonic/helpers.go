package ctrlsubsonic

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/tidalproxy"
)

// batchFetchTracks fetches metadata for multiple tidal track IDs concurrently
// with a semaphore to limit parallelism. Failed fetches are silently skipped.
func (c *Controller) batchFetchTracks(r *http.Request, tidalIDs []int) []*spec.TrackChild {
	if len(tidalIDs) == 0 {
		return nil
	}

	type result struct {
		idx   int
		track *spec.TrackChild
	}

	results := make(chan result, len(tidalIDs))
	semaphore := make(chan struct{}, 20) // max 20 concurrent
	var wg sync.WaitGroup

	for i, id := range tidalIDs {
		wg.Add(1)
		go func(idx, tid int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

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
	for _, t := range ordered {
		if t != nil {
			tracks = append(tracks, t)
		}
	}
	return tracks
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
