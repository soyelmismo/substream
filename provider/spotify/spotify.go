package spotify

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider and provider.ISRCResolver for Spotify
type Provider struct {
	client   *Client
	registry *provider.Registry
}

// Ensure Provider implements provider.MusicProvider and provider.ISRCResolver
var _ provider.MusicProvider = (*Provider)(nil)
var _ provider.ISRCResolver = (*Provider)(nil)

// Options configures Spotify credentials and Widevine DRM paths
type Options struct {
	SPDC     string
	WVDPath  string
	CacheDir string
}

// New creates a new Spotify Provider
func New(reg *provider.Registry, opts ...Options) *Provider {
	var clientOpts []ClientOptions
	if len(opts) > 0 {
		clientOpts = append(clientOpts, ClientOptions{
			SPDC:     opts[0].SPDC,
			WVDPath:  opts[0].WVDPath,
			CacheDir: opts[0].CacheDir,
		})
	}
	return &Provider{
		client:   NewClient(clientOpts...),
		registry: reg,
	}
}

func (p *Provider) ID() string {
	return "sp"
}

func (p *Provider) Name() string {
	return "Spotify"
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

func (p *Provider) trackToSpec(t *SpotifyTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	trackID := &specid.ID{URI: fmt.Sprintf("sp:tr:%s", t.ID)}
	albumID := &specid.ID{URI: fmt.Sprintf("sp:al:%s", t.Album.ID)}
	artistRawID := "unknown"
	artistName := "Unknown Artist"
	if len(t.Artists) > 0 {
		artistRawID = t.Artists[0].ID
		artistName = t.Artists[0].Name
	}
	artistID := &specid.ID{URI: fmt.Sprintf("sp:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("sp:al:%s", t.Album.ID)}

	duration := t.DurationMs / 1000
	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(t.Name)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.flac", safeArtist, t.TrackNumber, safeArtist, safeTitle)

	return &spec.TrackChild{
		ID:          trackID,
		Title:       t.Name,
		Album:       t.Album.Name,
		AlbumID:     albumID,
		Artist:      artistName,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: t.TrackNumber,
		DiscNumber:  t.DiscNumber,
		Duration:    duration,
		Bitrate:     1411,
		ContentType: "audio/flac",
		Suffix:      "flac",
		Size:        duration * 176400,
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        parseYear(t.Album.ReleaseDate),
	}
}

func (p *Provider) albumToSpec(a *SpotifyAlbum) *spec.Album {
	if a == nil {
		return nil
	}
	albumID := &specid.ID{URI: fmt.Sprintf("sp:al:%s", a.ID)}
	artistRawID := "unknown"
	artistName := "Unknown Artist"
	if len(a.Artists) > 0 {
		artistRawID = a.Artists[0].ID
		artistName = a.Artists[0].Name
	}
	artistID := &specid.ID{URI: fmt.Sprintf("sp:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("sp:al:%s", a.ID)}

	return &spec.Album{
		ID:         albumID,
		ParentID:   artistID,
		Name:       a.Name,
		Artist:     artistName,
		ArtistID:   artistID,
		CoverID:    coverID,
		TrackCount: a.TotalTracks,
		Year:       parseYear(a.ReleaseDate),
	}
}

func (p *Provider) artistToSpec(a *SpotifyArtist) *spec.Artist {
	if a == nil {
		return nil
	}
	id := &specid.ID{URI: fmt.Sprintf("sp:ar:%s", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("sp:ar:%s", a.ID)}
	return &spec.Artist{
		ID:      id,
		Name:    a.Name,
		CoverID: coverID,
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
	a, err := p.client.GetAlbum(ctx, rawID)
	if err != nil {
		return nil, err
	}
	return p.albumToSpec(a), nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	a, err := p.client.GetArtist(ctx, rawID)
	if err != nil {
		return nil, err
	}
	return p.artistToSpec(a), nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	return nil, nil, nil
}

func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	res, err := p.client.Search(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, 0, len(res.Tracks.Items))
	for _, t := range res.Tracks.Items {
		out = append(out, p.trackToSpec(&t))
	}
	return out, nil
}

func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	res, err := p.client.Search(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Album, 0, len(res.Albums.Items))
	for _, a := range res.Albums.Items {
		out = append(out, p.albumToSpec(&a))
	}
	return out, nil
}

func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	res, err := p.client.Search(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Artist, 0, len(res.Artists.Items))
	for _, a := range res.Artists.Items {
		out = append(out, p.artistToSpec(&a))
	}
	return out, nil
}

func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	res, err := p.client.Search(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	tracks := make([]*spec.TrackChild, 0, len(res.Tracks.Items))
	for _, t := range res.Tracks.Items {
		tracks = append(tracks, p.trackToSpec(&t))
	}
	albums := make([]*spec.Album, 0, len(res.Albums.Items))
	for _, a := range res.Albums.Items {
		albums = append(albums, p.albumToSpec(&a))
	}
	artists := make([]*spec.Artist, 0, len(res.Artists.Items))
	for _, a := range res.Artists.Items {
		artists = append(artists, p.artistToSpec(&a))
	}
	return &provider.SearchResults{
		Tracks:  tracks,
		Albums:  albums,
		Artists: artists,
	}, nil
}

// GetStreamURL resolves native Widevine decrypted audio if configured, or falls back to ISRC resolution
func (p *Provider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	// 1. Native Widevine DRM decryption (Free MP4_128 AAC or Premium MP4_256)
	if p.client.HasWidevine() {
		if streamPath, err := p.client.GetWidevineStream(ctx, rawID); err == nil && streamPath != "" {
			log.Printf("[SPOTIFY] 🚀 Resolved native Widevine audio stream for %s -> %s", rawID, streamPath)
			return streamPath, nil
		} else if err != nil {
			log.Printf("[SPOTIFY] ⚠️ Widevine resolution failed for %s: %v (falling back to ISRC)", rawID, err)
		}
	}

	// 2. Fallback via ISRC across registered lossless providers
	track, err := p.client.GetTrack(ctx, rawID)
	if err != nil {
		return "", err
	}
	isrc := track.ExternalIDs.ISRC
	if isrc == "" {
		return "", fmt.Errorf("spotify track %s has no ISRC", rawID)
	}

	if p.registry != nil {
		streamURL, _, _, err := p.registry.ResolveStreamByISRC(ctx, isrc, quality, clientIP, p.ID())
		if err == nil && streamURL != "" {
			return streamURL, nil
		}

		// Fallback to YouTube Music matching by Title + Artist
		if ytProv, err := p.registry.Get("yt"); err == nil && ytProv != nil && p.registry.IsAudioEnabled("yt") {
			artistName := ""
			if len(track.Artists) > 0 {
				artistName = track.Artists[0].Name
			}
			durationSec := track.DurationMs / 1000

			if matcher, ok := ytProv.(interface {
				MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error)
			}); ok {
				matchedTrack, err := matcher.MatchTrackByMetadata(ctx, artistName, track.Name, durationSec)
				if err == nil && matchedTrack != nil && matchedTrack.ID != nil {
					streamURL, err := ytProv.GetStreamURL(ctx, matchedTrack.ID.RawID(), quality, clientIP)
					if err == nil && streamURL != "" {
						log.Printf("[SPOTIFY] ✅ Resolved fallback stream for %q by %q via YouTube Music (%s)", track.Name, artistName, matchedTrack.ID.RawID())
						return streamURL, nil
					}
				}
			}
		}

		return "", fmt.Errorf("failed to resolve stream for ISRC %s across providers and YouTube Music: %w", isrc, err)
	}
	return "", fmt.Errorf("no provider registry available")
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	if strings.HasPrefix(coverID, "http") {
		return coverID
	}
	return coverID
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, fmt.Errorf("lyrics not supported")
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "top", limit, 0)
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

// GetTrackByISRC implements provider.ISRCResolver
func (p *Provider) GetTrackByISRC(ctx context.Context, isrc string) (*spec.TrackChild, error) {
	t, err := p.client.SearchByISRC(ctx, isrc)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}

// MatchTrackByMetadata matches a track by artist, title, and duration for cross-provider fallback
func (p *Provider) MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error) {
	query := fmt.Sprintf("%s %s", strings.TrimSpace(artist), strings.TrimSpace(title))
	tracks, err := p.SearchTracks(ctx, query, 5, 0)
	if err != nil {
		return nil, err
	}
	if len(tracks) == 0 {
		return nil, fmt.Errorf("no tracks found on Spotify for %q", query)
	}

	// Select best candidate matching duration (within +- 5 seconds)
	for _, t := range tracks {
		if expectedDuration <= 0 {
			return t, nil
		}
		diff := t.Duration - expectedDuration
		if diff < 0 {
			diff = -diff
		}
		if diff <= 5 {
			log.Printf("[SPOTIFY] ✅ Matched track %q by %q with Spotify track %s (duration diff: %ds)", title, artist, t.ID.RawID(), diff)
			return t, nil
		}
	}

	// If no exact duration match, return the top candidate
	log.Printf("[SPOTIFY] ⚠️ Fallback to top Spotify result for %q by %q: %s", title, artist, tracks[0].ID.RawID())
	return tracks[0], nil
}

