package deezer

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider for Deezer
type Provider struct {
	client *Client
}

// Ensure Provider implements provider.MusicProvider
var _ provider.MusicProvider = (*Provider)(nil)

// New creates a new Deezer Provider
func New(opts ClientOptions) *Provider {
	return &Provider{
		client: NewClient(opts),
	}
}

func (p *Provider) ID() string {
	return "dz"
}

func (p *Provider) Name() string {
	return "Deezer"
}

func (p *Provider) parseNumericID(raw string) (int64, error) {
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Deezer numeric ID %q: %w", raw, err)
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

func (p *Provider) trackToSpec(t *DeezerTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	rawID := strconv.FormatInt(t.ID, 10)
	albumRawID := strconv.FormatInt(t.Album.ID, 10)
	artistRawID := strconv.FormatInt(t.Artist.ID, 10)

	trackID := &specid.ID{URI: fmt.Sprintf("dz:tr:%s", rawID)}
	albumID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", albumRawID)}
	artistID := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", albumRawID)}

	safeArtist := sanitizeFilename(t.Artist.Name)
	safeTitle := sanitizeFilename(t.Title)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.flac", safeArtist, t.TrackPosition, safeArtist, safeTitle)

	return &spec.TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       t.Album.Title,
		AlbumID:     albumID,
		Artist:      t.Artist.Name,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: t.TrackPosition,
		DiscNumber:  t.DiskNumber,
		Duration:    t.Duration,
		Bitrate:     1411,
		ContentType: "audio/flac",
		Suffix:      "flac",
		Size:        t.Duration * 176400,
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        parseYear(t.ReleaseDate),
	}
}

func (p *Provider) albumToSpec(a *DeezerAlbum) *spec.Album {
	if a == nil {
		return nil
	}
	rawID := strconv.FormatInt(a.ID, 10)
	artistRawID := strconv.FormatInt(a.Artist.ID, 10)
	albumID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", rawID)}
	artistID := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", rawID)}

	return &spec.Album{
		ID:         albumID,
		ParentID:   artistID,
		Name:       a.Title,
		Artist:     a.Artist.Name,
		ArtistID:   artistID,
		CoverID:    coverID,
		TrackCount: a.NbTracks,
		Duration:   a.Duration,
		Year:       parseYear(a.ReleaseDate),
	}
}

func (p *Provider) artistToSpec(a *DeezerArtist) *spec.Artist {
	if a == nil {
		return nil
	}
	rawID := strconv.FormatInt(a.ID, 10)
	id := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", rawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", rawID)}
	return &spec.Artist{
		ID:         id,
		Name:       a.Name,
		CoverID:    coverID,
		AlbumCount: a.NbAlbum,
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
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	a, err := p.client.GetAlbum(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.albumToSpec(a), nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	a, err := p.client.GetArtist(ctx, id)
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
	albums, err := p.client.GetArtistAlbums(ctx, id, 50)
	if err != nil {
		return nil, nil, err
	}

	resAlbums := make([]*spec.Album, 0, len(albums))
	for _, a := range albums {
		resAlbums = append(resAlbums, p.albumToSpec(&a))
	}

	var tracks []*spec.TrackChild
	if !skipTracks {
		top, err := p.client.GetArtistTopTracks(ctx, id, 50)
		if err == nil {
			tracks = make([]*spec.TrackChild, 0, len(top))
			for _, t := range top {
				tracks = append(tracks, p.trackToSpec(&t))
			}
		}
	}

	return resAlbums, tracks, nil
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
	t, err := p.client.GetTrack(ctx, id)
	if err != nil {
		return "", err
	}
	return p.client.GetStreamURL(ctx, t, quality)
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	if strings.HasPrefix(coverID, "http") {
		return coverID
	}
	return fmt.Sprintf("https://e-cdns-images.dzcdn.net/images/cover/%s/500x500.jpg", coverID)
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, fmt.Errorf("deezer lyrics not supported")
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "top", limit, 0)
}

func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	id, err := p.parseNumericID(artistRawID)
	if err != nil {
		return nil, err
	}
	tracks, err := p.client.GetArtistTopTracks(ctx, id, limit)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.TrackChild, 0, len(tracks))
	for _, t := range tracks {
		res = append(res, p.trackToSpec(&t))
	}
	return res, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	return nil, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	return nil, nil
}

// GetTrackByISRC looks up a Deezer track by its international standard recording code
func (p *Provider) GetTrackByISRC(ctx context.Context, isrc string) (*spec.TrackChild, error) {
	t, err := p.client.SearchByISRC(ctx, isrc)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}
