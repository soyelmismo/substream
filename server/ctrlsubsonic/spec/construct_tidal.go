package spec

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
	"go.senan.xyz/gonic/tidalproxy"
)

// nonAsciiUnsafe matches any character that is NOT a safe filename char.
// Allows: Unicode letters (incluye tildes, ñ, ü, etc.), numbers, space, period, hyphen, underscore
var nonAsciiUnsafe = regexp.MustCompile(`[^\p{L}\p{N} ._-]+`)

// sanitizeFilename sanitizes a string for safe use in filenames.
// Removes all non-ASCII and unsafe filesystem characters.
func sanitizeFilename(s string) string {
	// First replace common unsafe chars with hyphen
	replacer := strings.NewReplacer(
		"/", "-",
		"\\", "-",
		":", "-",
		"*", "",
		"?", "",
		"\"", "'",
		"<", "",
		">", "",
		"|", "-",
	)
	s = replacer.Replace(s)
	// Then remove all non-ASCII/unsafe unicode characters
	s = nonAsciiUnsafe.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

// NewTrackFromTidal converts a Tidal track to Subsonic TrackChild
func NewTrackFromTidal(t *tidalproxy.TidalTrack) *TrackChild {
	artistName := t.Artist.Name
	if len(t.Artists) > 0 {
		artistName = t.Artists[0].Name
	}

	trackID := &specid.ID{URI: fmt.Sprintf("td:tr:%d", t.ID)}
	albumID := &specid.ID{URI: fmt.Sprintf("td:al:%d", t.Album.ID)}
	artistID := &specid.ID{URI: fmt.Sprintf("td:ar:%d", t.Artist.ID)}

	// Use track ID as fallback for cover if album ID is invalid
	coverIDValue := t.Album.ID
	if coverIDValue == 0 {
		coverIDValue = t.ID
	}
	coverID := &specid.ID{URI: fmt.Sprintf("td:al:%d", coverIDValue)}

	// Determine quality and suffix first
	// [NOTE] For LOSSLESS/HI_RES_LOSSLESS, we use .m4a extension in path because
	// we don't know if the stream will be BTS (native FLAC) or HLS (fMP4 container).
	// The actual audio quality is lossless in both cases.
	suffix := "m4a"
	bitrate := 320
	contentType := "audio/mp4"
	if t.AudioQuality == "LOSSLESS" || t.AudioQuality == "HI_RES_LOSSLESS" {
		bitrate = 1411
	}

	// Build user-friendly path: audio/artista/NN. artista - titulo.formato
	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(t.Title)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.%s", safeArtist, t.TrackNumber, safeArtist, safeTitle, suffix)

	tc := &TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       t.Album.Title,
		AlbumID:     albumID,
		Artist:      artistName,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: t.TrackNumber,
		DiscNumber:  t.VolumeNumber,
		Duration:    t.Duration,
		Bitrate:     bitrate,
		ContentType: contentType,
		Suffix:      suffix,
		Size:        t.Duration * 176400, // approximate FLAC size
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        parseYear(t.Album.ReleaseDate),
	}

	// multi-artist
	if len(t.Artists) > 0 {
		tc.Artists = make([]*ArtistRef, len(t.Artists))
		names := make([]string, len(t.Artists))
		for i, a := range t.Artists {
			id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}
			tc.Artists[i] = &ArtistRef{ID: id, Name: a.Name}
			names[i] = a.Name
		}
		tc.DisplayArtist = strings.Join(names, ", ")
	}

	return tc
}

// NewAlbumFromTidal converts a Tidal album to Subsonic Album
func NewAlbumFromTidal(a *tidalproxy.TidalAlbum) *Album {
	albumID := &specid.ID{URI: fmt.Sprintf("td:al:%d", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("td:al:%d", a.ID)}

	artistName := ""
	var artistID *specid.ID
	var artistRefs []*ArtistRef
	if len(a.Artists) > 0 {
		artistName = a.Artists[0].Name
		artistID = &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.Artists[0].ID)}
		artistRefs = make([]*ArtistRef, len(a.Artists))
		for i, ar := range a.Artists {
			id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", ar.ID)}
			artistRefs[i] = &ArtistRef{ID: id, Name: ar.Name}
		}
	}

	names := make([]string, len(a.Artists))
	for i, ar := range a.Artists {
		names[i] = ar.Name
	}

	return &Album{
		ID:            albumID,
		ParentID:      artistID,
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
	id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}

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
