package spec

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
	"go.senan.xyz/gonic/tidalproxy"
)

// NewTrackFromTidal converts a Tidal track to Subsonic TrackChild
func NewTrackFromTidal(t *tidalproxy.TidalTrack) *TrackChild {
	artistName := t.Artist.Name
	if len(t.Artists) > 0 {
		artistName = t.Artists[0].Name
	}

	trackID := &specid.ID{Type: specid.Track, Value: t.ID}
	albumID := &specid.ID{Type: specid.Album, Value: t.Album.ID}
	artistID := &specid.ID{Type: specid.Artist, Value: t.Artist.ID}
	coverID := &specid.ID{Type: specid.Album, Value: t.Album.ID}

	tc := &TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       t.Album.Title,
		AlbumID:     albumID,
		Artist:      artistName,
		ArtistID:    artistID,
		CoverID:     coverID,
		TrackNumber: t.TrackNumber,
		DiscNumber:  t.VolumeNumber,
		Duration:    t.Duration,
		Bitrate:     1411,
		ContentType: "audio/flac",
		Suffix:      "flac",
		Size:        t.Duration * 176400, // approximate FLAC size
		IsDir:       false,
		Type:        "music",
		Path:        fmt.Sprintf("tidal/%d/%s.flac", t.Album.ID, t.Title),
		Year:        parseYear(t.Album.ReleaseDate),
	}

	// multi-artist
	if len(t.Artists) > 0 {
		tc.Artists = make([]*ArtistRef, len(t.Artists))
		names := make([]string, len(t.Artists))
		for i, a := range t.Artists {
			id := &specid.ID{Type: specid.Artist, Value: a.ID}
			tc.Artists[i] = &ArtistRef{ID: id, Name: a.Name}
			names[i] = a.Name
		}
		tc.DisplayArtist = strings.Join(names, ", ")
	}

	return tc
}

// NewAlbumFromTidal converts a Tidal album to Subsonic Album
func NewAlbumFromTidal(a *tidalproxy.TidalAlbum) *Album {
	albumID := &specid.ID{Type: specid.Album, Value: a.ID}
	coverID := &specid.ID{Type: specid.Album, Value: a.ID}

	artistName := ""
	var artistID *specid.ID
	var artistRefs []*ArtistRef
	if len(a.Artists) > 0 {
		artistName = a.Artists[0].Name
		artistID = &specid.ID{Type: specid.Artist, Value: a.Artists[0].ID}
		artistRefs = make([]*ArtistRef, len(a.Artists))
		for i, ar := range a.Artists {
			id := &specid.ID{Type: specid.Artist, Value: ar.ID}
			artistRefs[i] = &ArtistRef{ID: id, Name: ar.Name}
		}
	}

	names := make([]string, len(a.Artists))
	for i, ar := range a.Artists {
		names[i] = ar.Name
	}

	return &Album{
		ID:            albumID,
		Name:          a.Title,
		Artist:        artistName,
		ArtistID:      artistID,
		CoverID:       coverID,
		TrackCount:    a.NumberOfTracks,
		Duration:      a.Duration,
		Year:          parseYear(a.ReleaseDate),
		Artists:       artistRefs,
		DisplayArtist: strings.Join(names, ", "),
	}
}

// NewArtistFromTidal converts a Tidal artist to Subsonic Artist
func NewArtistFromTidal(a *tidalproxy.TidalArtist) *Artist {
	id := &specid.ID{Type: specid.Artist, Value: a.ID}
	coverID := &specid.ID{Type: specid.Artist, Value: a.ID}

	return &Artist{
		ID:      id,
		Name:    a.Name,
		CoverID: coverID,
	}
}

// ApplyTrackStar decorates a TrackChild with star info
func ApplyTrackStar(tc *TrackChild, starDate *time.Time) {
	tc.Starred = starDate
}

// ApplyAlbumStar decorates an Album with star info
func ApplyAlbumStar(a *Album, starDate *time.Time) {
	a.Starred = starDate
}

// ApplyArtistStar decorates an Artist with star info
func ApplyArtistStar(a *Artist, starDate *time.Time) {
	a.Starred = starDate
}

// parseYear extracts year from "2024-01-15" or "2024"
func parseYear(date string) int {
	if date == "" {
		return 0
	}
	parts := strings.Split(date, "-")
	if len(parts) > 0 {
		y, _ := strconv.Atoi(parts[0])
		return y
	}
	return 0
}
