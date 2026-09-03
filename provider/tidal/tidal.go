package tidal

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
	"go.senan.xyz/gonic/tidalproxy"
)

var nonAsciiUnsafe = regexp.MustCompile(`[^\p{L}\p{N} ._-]+`)
var lrcRegex = regexp.MustCompile(`^\[(\d{2}):(\d{2}\.\d{2,3})\]\s*(.*)$`)

func sanitizeFilename(s string) string {
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
	s = nonAsciiUnsafe.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

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

// Provider implements provider.MusicProvider using tidalproxy backend.
type Provider struct {
	proxy tidalproxy.TidalProxy
}

// New creates a new Tidal music provider wrapping tidalproxy.
func New(proxy tidalproxy.TidalProxy) *Provider {
	return &Provider{proxy: proxy}
}

func (p *Provider) ID() string {
	return "td"
}

func (p *Provider) Name() string {
	return "Tidal"
}

// Proxy returns the underlying TidalProxy if direct access is needed
func (p *Provider) Proxy() tidalproxy.TidalProxy {
	return p.proxy
}

func (p *Provider) parseNumericID(raw string) (int, error) {
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid tidal ID %q: must be integer", raw)
	}
	return val, nil
}

func (p *Provider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	t, err := p.proxy.GetTrackInfo(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}

func (p *Provider) GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild {
	intIDs := make([]int, 0, len(rawIDs))
	indexMap := make(map[int]int) // intID -> original index

	for i, raw := range rawIDs {
		if id, err := strconv.Atoi(raw); err == nil {
			intIDs = append(intIDs, id)
			indexMap[id] = i
		}
	}

	results := make([]*spec.TrackChild, len(rawIDs))
	if len(intIDs) == 0 {
		return results
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)

	for _, tid := range intIDs {
		wg.Add(1)
		go func(trackID int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if t, err := p.proxy.GetTrackInfo(ctx, trackID); err == nil && t != nil {
				idx := indexMap[trackID]
				results[idx] = p.trackToSpec(t)
			}
		}(tid)
	}

	wg.Wait()
	return results
}

func (p *Provider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	album, err := p.proxy.GetAlbumInfo(ctx, id)
	if err != nil {
		return nil, err
	}

	a := p.albumToSpec(album)
	a.TrackCount = len(album.Items)
	a.Tracks = make([]*spec.TrackChild, len(album.Items))

	totalDuration := 0
	for i := range album.Items {
		tc := p.trackToSpec(&album.Items[i])
		if tc.Album == "" {
			tc.Album = album.Title
		}
		if tc.AlbumID == nil {
			tc.AlbumID = a.ID
		}
		a.Tracks[i] = tc
		totalDuration += tc.Duration
	}
	if a.Duration == 0 {
		a.Duration = totalDuration
	}

	return a, nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	info, err := p.proxy.GetArtistInfo(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.artistToSpec(&info.Artist), nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, nil, err
	}
	page, err := p.proxy.GetArtistAlbums(ctx, id, skipTracks)
	if err != nil {
		return nil, nil, err
	}

	albums := make([]*spec.Album, len(page.Albums.Items))
	for i := range page.Albums.Items {
		albums[i] = p.albumToSpec(&page.Albums.Items[i])
	}

	tracks := make([]*spec.TrackChild, len(page.Tracks))
	for i := range page.Tracks {
		tracks[i] = p.trackToSpec(&page.Tracks[i])
	}

	return albums, tracks, nil
}

func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	rawTracks, err := p.proxy.SearchTracks(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(rawTracks))
	for i := range rawTracks {
		out[i] = p.trackToSpec(&rawTracks[i])
	}
	return out, nil
}

func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	rawAlbums, err := p.proxy.SearchAlbums(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Album, len(rawAlbums))
	for i := range rawAlbums {
		out[i] = p.albumToSpec(&rawAlbums[i])
	}
	return out, nil
}

func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	rawArtists, err := p.proxy.SearchArtists(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Artist, len(rawArtists))
	for i := range rawArtists {
		out[i] = p.artistToSpec(&rawArtists[i])
	}
	return out, nil
}

func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	var wg sync.WaitGroup
	wg.Add(3)

	var tracks []*spec.TrackChild
	var albums []*spec.Album
	var artists []*spec.Artist

	go func() {
		defer wg.Done()
		tracks, _ = p.SearchTracks(ctx, query, limit, offset)
	}()
	go func() {
		defer wg.Done()
		albums, _ = p.SearchAlbums(ctx, query, limit, offset)
	}()
	go func() {
		defer wg.Done()
		artists, _ = p.SearchArtists(ctx, query, limit, offset)
	}()

	wg.Wait()

	return &provider.SearchResults{
		Tracks:  tracks,
		Albums:  albums,
		Artists: artists,
	}, nil
}

func (p *Provider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return "", err
	}
	return p.proxy.GetStreamURL(ctx, id, quality, clientIP)
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	return p.proxy.GetCoverURL(coverID, size)
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	rawLyrics, err := p.proxy.GetLyrics(ctx, id)
	if err != nil {
		return nil, err
	}

	var specLines []spec.Lyric
	if rawLyrics.Subtitles != "" {
		for _, line := range strings.Split(rawLyrics.Subtitles, "\n") {
			matches := lrcRegex.FindStringSubmatch(line)
			if len(matches) == 4 {
				min, _ := strconv.Atoi(matches[1])
				sec, _ := strconv.ParseFloat(matches[2], 64)
				startMs := int64(min*60*1000 + int(sec*1000))
				specLines = append(specLines, spec.Lyric{
					Start: &startMs,
					Value: matches[3],
				})
			} else if line != "" {
				specLines = append(specLines, spec.Lyric{Value: line})
			}
		}
	} else {
		for _, l := range strings.Split(rawLyrics.Lyrics, "\n") {
			specLines = append(specLines, spec.Lyric{Value: l})
		}
	}

	return &spec.StructuredLyrics{
		Lang:   "und",
		Synced: rawLyrics.Subtitles != "",
		Lines:  specLines,
	}, nil
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	tracks, err := p.proxy.GetTopTracks(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(tracks))
	for i := range tracks {
		out[i] = p.trackToSpec(&tracks[i])
	}
	return out, nil
}

func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	id, err := p.parseNumericID(artistRawID)
	if err != nil {
		return nil, err
	}
	tracks, err := p.proxy.GetArtistTopTracks(ctx, id, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(tracks))
	for i := range tracks {
		out[i] = p.trackToSpec(&tracks[i])
	}
	return out, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	id, err := p.parseNumericID(artistRawID)
	if err != nil {
		return nil, err
	}
	artists, err := p.proxy.GetSimilarArtists(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Artist, len(artists))
	for i := range artists {
		out[i] = p.artistToSpec(&artists[i])
	}
	return out, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	id, err := p.parseNumericID(trackRawID)
	if err != nil {
		return nil, err
	}
	tracks, err := p.proxy.GetRecommendations(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(tracks))
	for i := range tracks {
		out[i] = p.trackToSpec(&tracks[i])
	}
	return out, nil
}

// --- Spec Conversion Helpers ---

func (p *Provider) trackToSpec(t *tidalproxy.TidalTrack) *spec.TrackChild {
	artistName := t.Artist.Name
	if len(t.Artists) > 0 {
		artistName = t.Artists[0].Name
	}

	trackID := &specid.ID{URI: fmt.Sprintf("td:tr:%d", t.ID)}
	albumID := &specid.ID{URI: fmt.Sprintf("td:al:%d", t.Album.ID)}
	artistID := &specid.ID{URI: fmt.Sprintf("td:ar:%d", t.Artist.ID)}

	coverIDValue := t.Album.ID
	if coverIDValue == 0 {
		coverIDValue = t.ID
	}
	coverID := &specid.ID{URI: fmt.Sprintf("td:al:%d", coverIDValue)}

	suffix := "m4a"
	bitrate := 320
	contentType := "audio/mp4"
	if t.AudioQuality == "LOSSLESS" || t.AudioQuality == "HI_RES_LOSSLESS" {
		bitrate = 1411
	}

	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(t.Title)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.%s", safeArtist, t.TrackNumber, safeArtist, safeTitle, suffix)

	tc := &spec.TrackChild{
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
		Size:        t.Duration * 176400,
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        parseYear(t.Album.ReleaseDate),
	}

	if len(t.Artists) > 0 {
		tc.Artists = make([]*spec.ArtistRef, len(t.Artists))
		names := make([]string, len(t.Artists))
		for i, a := range t.Artists {
			id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}
			tc.Artists[i] = &spec.ArtistRef{ID: id, Name: a.Name}
			names[i] = a.Name
		}
		tc.DisplayArtist = strings.Join(names, ", ")
	}

	return tc
}

func (p *Provider) albumToSpec(a *tidalproxy.TidalAlbum) *spec.Album {
	albumID := &specid.ID{URI: fmt.Sprintf("td:al:%d", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("td:al:%d", a.ID)}

	artistName := ""
	var artistID *specid.ID
	var artistRefs []*spec.ArtistRef
	if len(a.Artists) > 0 {
		artistName = a.Artists[0].Name
		artistID = &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.Artists[0].ID)}
		artistRefs = make([]*spec.ArtistRef, len(a.Artists))
		for i, ar := range a.Artists {
			id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", ar.ID)}
			artistRefs[i] = &spec.ArtistRef{ID: id, Name: ar.Name}
		}
	}

	names := make([]string, len(a.Artists))
	for i, ar := range a.Artists {
		names[i] = ar.Name
	}

	return &spec.Album{
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

func (p *Provider) artistToSpec(a *tidalproxy.TidalArtist) *spec.Artist {
	id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}

	return &spec.Artist{
		ID:      id,
		Name:    a.Name,
		CoverID: coverID,
	}
}
