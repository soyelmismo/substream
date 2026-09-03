package ytmusic

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider and provider.ISRCResolver for YouTube Music
type Provider struct {
	client *Client
}

// Ensure Provider implements provider.MusicProvider and provider.ISRCResolver
var _ provider.MusicProvider = (*Provider)(nil)
var _ provider.ISRCResolver = (*Provider)(nil)

// New creates a new YouTube Music Provider
func New() *Provider {
	return &Provider{
		client: NewClient(),
	}
}

func (p *Provider) ID() string {
	return "yt"
}

func (p *Provider) Name() string {
	return "YouTube Music"
}

func (p *Provider) trackToSpec(t *YTTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}

	trackID := &specid.ID{URI: fmt.Sprintf("yt:tr:%s", t.VideoID)}
	albumID := &specid.ID{URI: fmt.Sprintf("yt:al:%s", t.VideoID)}
	artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", t.ArtistID)}
	coverID := &specid.ID{URI: fmt.Sprintf("yt:tr:%s", t.VideoID)}

	artistName := t.Artist
	if artistName == "" {
		artistName = "Unknown Artist"
	}
	albumName := t.Album
	if albumName == "" {
		albumName = t.Title + " - Single"
	}

	// 160kbps Opus stream size estimation
	bitrate := 160
	estSize := t.Duration * (bitrate * 1000 / 8)

	return &spec.TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       albumName,
		AlbumID:     albumID,
		Artist:      artistName,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		Duration:    t.Duration,
		Bitrate:     bitrate,
		ContentType: "audio/webm",
		Suffix:      "webm",
		Size:        estSize,
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
	albumID := &specid.ID{URI: fmt.Sprintf("yt:al:%s", rawID)}
	return &spec.Album{
		ID:   albumID,
		Name: "YouTube Music Album " + rawID,
	}, nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", rawID)}
	return &spec.Artist{
		ID:   artistID,
		Name: "YouTube Music Artist " + rawID,
	}, nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	return nil, nil, nil
}

func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	tracks, err := p.client.SearchTracks(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	specTracks := make([]*spec.TrackChild, 0, len(tracks))
	for _, t := range tracks {
		if st := p.trackToSpec(t); st != nil {
			specTracks = append(specTracks, st)
		}
	}
	return specTracks, nil
}

func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	return nil, nil
}

func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	return nil, nil
}

func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	tracks, err := p.SearchTracks(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	return &provider.SearchResults{
		Tracks: tracks,
	}, nil
}

func (p *Provider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	return p.client.GetStreamURL(ctx, rawID)
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	if strings.HasPrefix(coverID, "http") {
		return coverID
	}
	// If it's a video ID, return YouTube max resolution or high resolution thumbnail
	rawID := coverID
	if strings.HasPrefix(rawID, "yt:tr:") {
		rawID = strings.TrimPrefix(rawID, "yt:tr:")
	}
	return fmt.Sprintf("https://i.ytimg.com/vi/%s/hqdefault.jpg", rawID)
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, fmt.Errorf("lyrics not supported for YouTube Music")
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "top hits", limit, 0)
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

// GetTrackByISRC attempts to find an official track on YouTube Music using ISRC or metadata
func (p *Provider) GetTrackByISRC(ctx context.Context, isrc string) (*spec.TrackChild, error) {
	if strings.TrimSpace(isrc) == "" {
		return nil, fmt.Errorf("empty isrc")
	}

	// Search by ISRC
	tracks, err := p.client.SearchTracks(ctx, isrc, 5)
	if err == nil && len(tracks) > 0 {
		return p.trackToSpec(tracks[0]), nil
	}

	return nil, fmt.Errorf("no track found on YouTube Music for ISRC %s", isrc)
}

// MatchTrackByMetadata searches YouTube Music for "{artist} {title}" and validates duration match
func (p *Provider) MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error) {
	query := fmt.Sprintf("%s %s", strings.TrimSpace(artist), strings.TrimSpace(title))
	tracks, err := p.client.SearchTracks(ctx, query, 5)
	if err != nil {
		return nil, err
	}

	if len(tracks) == 0 {
		return nil, fmt.Errorf("no tracks found on YouTube Music for %q", query)
	}

	// Select best candidate matching duration (within +- 4 seconds)
	for _, t := range tracks {
		if expectedDuration <= 0 {
			return p.trackToSpec(t), nil
		}
		diff := t.Duration - expectedDuration
		if diff < 0 {
			diff = -diff
		}
		if diff <= 4 {
			log.Printf("[YTMUSIC] ✅ Matched track %q by %q with YouTube Music video %s (duration diff: %ds)", title, artist, t.VideoID, diff)
			return p.trackToSpec(t), nil
		}
	}

	// If no exact duration match, return the top candidate
	log.Printf("[YTMUSIC] ⚠️ Fallback to top YouTube Music result for %q by %q: %s", title, artist, tracks[0].VideoID)
	return p.trackToSpec(tracks[0]), nil
}
