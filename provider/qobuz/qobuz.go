package qobuz

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider for Qobuz
type Provider struct {
	client *Client
}

// Ensure Provider implements provider.MusicProvider
var _ provider.MusicProvider = (*Provider)(nil)

// New creates a new Qobuz Provider
func New(opts ClientOptions) *Provider {
	return &Provider{
		client: NewClient(opts),
	}
}

func (p *Provider) ID() string {
	return "qb"
}

func (p *Provider) Name() string {
	return "Qobuz"
}

func (p *Provider) parseNumericID(raw string) (int64, error) {
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Qobuz numeric ID %q: %w", raw, err)
	}
	return val, nil
}

func sanitizeFilename(s string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "\"", "_", "<", "_", ">", "_", "|", "_")
	return replacer.Replace(s)
}

func parseYear(dateStr string) int {
	parts := strings.Split(dateStr, "-")
	if len(parts) > 0 && len(parts[0]) == 4 {
		y, _ := strconv.Atoi(parts[0])
		return y
	}
	return 0
}

func (p *Provider) trackToSpec(t *QobuzTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	rawID := strconv.FormatInt(t.ID, 10)
	albumRawID := t.Album.ID
	if albumRawID == "" {
		albumRawID = "0"
	}
	artistRawID := strconv.FormatInt(t.Performer.ID, 10)
	if artistRawID == "0" {
		artistRawID = strconv.FormatInt(t.Album.Artist.ID, 10)
	}

	artistName := t.Performer.Name
	if artistName == "" {
		artistName = t.Album.Artist.Name
	}

	contentType := "audio/flac"
	suffix := "flac"
	bitrate := 1411
	if t.Hires {
		bitrate = 4608 // 24/96 avg
	}

	trackID := &specid.ID{URI: fmt.Sprintf("qb:tr:%s", rawID)}
	albumID := &specid.ID{URI: fmt.Sprintf("qb:al:%s", albumRawID)}
	artistID := &specid.ID{URI: fmt.Sprintf("qb:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("qb:al:%s", albumRawID)}

	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(t.Title)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.%s", safeArtist, t.TrackNumber, safeArtist, safeTitle, suffix)

	return &spec.TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       t.Album.Title,
		AlbumID:     albumID,
		Artist:      artistName,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: t.TrackNumber,
		DiscNumber:  t.MediaNumber,
		Duration:    t.Duration,
		Bitrate:     bitrate,
		ContentType: contentType,
		Suffix:      suffix,
		Size:        t.Duration * 176400,
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        parseYear(t.ReleaseDateOriginal),
	}
}

func (p *Provider) albumToSpec(a *QobuzAlbum) *spec.Album {
	if a == nil {
		return nil
	}
	albumID := &specid.ID{URI: fmt.Sprintf("qb:al:%s", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("qb:al:%s", a.ID)}
	artistRawID := strconv.FormatInt(a.Artist.ID, 10)
	artistID := &specid.ID{URI: fmt.Sprintf("qb:ar:%s", artistRawID)}

	return &spec.Album{
		ID:         albumID,
		ParentID:   artistID,
		Name:       a.Title,
		Artist:     a.Artist.Name,
		ArtistID:   artistID,
		CoverID:    coverID,
		TrackCount: a.TracksCount,
		Duration:   a.Duration,
		Year:       parseYear(a.ReleaseDateOriginal),
	}
}

func (p *Provider) artistToSpec(a *QobuzArtist) *spec.Artist {
	if a == nil {
		return nil
	}
	rawID := strconv.FormatInt(a.ID, 10)
	id := &specid.ID{URI: fmt.Sprintf("qb:ar:%s", rawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("qb:ar:%s", rawID)}
	return &spec.Artist{
		ID:      id,
		Name:    a.Name,
		CoverID: coverID,
	}
}

func (p *Provider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	t, err := p.client.GetTrack(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}

func (p *Provider) GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild {
	res := make([]*spec.TrackChild, len(rawIDs))
	for i, raw := range rawIDs {
		t, err := p.GetTrack(ctx, raw)
		if err == nil {
			res[i] = t
		}
	}
	return res
}

func (p *Provider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	a, err := p.client.GetAlbum(ctx, rawID)
	if err != nil {
		return nil, err
	}
	albumSpec := p.albumToSpec(a)
	return albumSpec, nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	a, err := p.client.GetArtist(ctx, id, "albums")
	if err != nil {
		return nil, err
	}
	return p.artistToSpec(a), nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, nil, err
	}
	a, err := p.client.GetArtist(ctx, id, "albums")
	if err != nil {
		return nil, nil, err
	}

	albums := make([]*spec.Album, 0, len(a.Albums.Items))
	for _, alb := range a.Albums.Items {
		albums = append(albums, p.albumToSpec(&alb))
	}

	var tracks []*spec.TrackChild
	if !skipTracks {
		tracks = make([]*spec.TrackChild, 0, len(a.Tracks.Items))
		for _, trk := range a.Tracks.Items {
			tracks = append(tracks, p.trackToSpec(&trk))
		}
	}

	return albums, tracks, nil
}

func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	items, err := p.client.SearchTracks(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.TrackChild, 0, len(items))
	for _, t := range items {
		res = append(res, p.trackToSpec(&t))
	}
	return res, nil
}

func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	items, err := p.client.SearchAlbums(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.Album, 0, len(items))
	for _, a := range items {
		res = append(res, p.albumToSpec(&a))
	}
	return res, nil
}

func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	items, err := p.client.SearchArtists(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.Artist, 0, len(items))
	for _, a := range items {
		res = append(res, p.artistToSpec(&a))
	}
	return res, nil
}

func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	tracks, _ := p.SearchTracks(ctx, query, limit, offset)
	albums, _ := p.SearchAlbums(ctx, query, limit, offset)
	artists, _ := p.SearchArtists(ctx, query, limit, offset)

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
	return p.client.GetStreamURL(ctx, id, quality)
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	if strings.HasPrefix(coverID, "http") {
		return coverID
	}
	return fmt.Sprintf("https://static.qobuz.com/images/covers/%s", coverID)
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, fmt.Errorf("qobuz lyrics not supported")
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "top", limit, 0)
}

func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	id, err := p.parseNumericID(artistRawID)
	if err != nil {
		return nil, err
	}
	a, err := p.client.GetArtist(ctx, id, "tracks")
	if err != nil {
		return nil, err
	}
	count := len(a.Tracks.Items)
	if count > limit && limit > 0 {
		count = limit
	}
	res := make([]*spec.TrackChild, 0, count)
	for i := 0; i < count; i++ {
		res = append(res, p.trackToSpec(&a.Tracks.Items[i]))
	}
	return res, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	return nil, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	return nil, nil
}

// GetTrackByISRC looks up a Qobuz track by its international standard recording code
func (p *Provider) GetTrackByISRC(ctx context.Context, isrc string) (*spec.TrackChild, error) {
	t, err := p.client.SearchByISRC(ctx, isrc)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}
