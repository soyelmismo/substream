package amazon

import (
	"context"
	"fmt"
	"strings"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider for Amazon Music
type Provider struct {
	client *Client
}

// Ensure Provider implements provider.MusicProvider
var _ provider.MusicProvider = (*Provider)(nil)

// New creates a new Amazon Music Provider
func New(opts ClientOptions) *Provider {
	return &Provider{
		client: NewClient(opts),
	}
}

func (p *Provider) ID() string {
	return "am"
}

func (p *Provider) Name() string {
	return "Amazon Music"
}

func (p *Provider) trackToSpec(t *AmazonTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	trackID := &specid.ID{URI: fmt.Sprintf("am:tr:%s", t.ASIN)}
	albumID := &specid.ID{URI: fmt.Sprintf("am:al:%s", t.AlbumASIN)}
	artistID := &specid.ID{URI: fmt.Sprintf("am:ar:%s", t.ArtistASIN)}
	coverID := &specid.ID{URI: fmt.Sprintf("am:al:%s", t.AlbumASIN)}

	return &spec.TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       t.Album,
		AlbumID:     albumID,
		Artist:      t.Artist,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: t.TrackNumber,
		Duration:    t.Duration,
		Bitrate:     1411,
		ContentType: "audio/flac",
		Suffix:      "flac",
		Size:        t.Duration * 176400,
		IsDir:       false,
		Type:        "music",
	}
}

func (p *Provider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	t, err := p.client.GetTrack(ctx, rawID)
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
	albumID := &specid.ID{URI: fmt.Sprintf("am:al:%s", rawID)}
	return &spec.Album{
		ID:   albumID,
		Name: "Amazon Album " + rawID,
	}, nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	artistID := &specid.ID{URI: fmt.Sprintf("am:ar:%s", rawID)}
	return &spec.Artist{
		ID:   artistID,
		Name: "Amazon Artist " + rawID,
	}, nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	return nil, nil, nil
}

func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	return nil, nil
}

func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	return nil, nil
}

func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	return nil, nil
}

func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	return &provider.SearchResults{}, nil
}

func (p *Provider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	return p.client.GetStreamURL(ctx, rawID, quality)
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	if strings.HasPrefix(coverID, "http") {
		return coverID
	}
	return fmt.Sprintf("https://m.media-amazon.com/images/I/%s.jpg", coverID)
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, fmt.Errorf("amazon lyrics not supported")
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return nil, nil
}

func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	return nil, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	return nil, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	return nil, nil
}
