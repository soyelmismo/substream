package ytmusic

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

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

func sanitizeFilename(s string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "\"", "_", "<", "_", ">", "_", "|", "_")
	return replacer.Replace(s)
}

func (p *Provider) trackToSpec(t *YTTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}

	trackID := &specid.ID{URI: fmt.Sprintf("yt:tr:%s", t.VideoID)}
	albumRawID := t.AlbumID
	if albumRawID == "" {
		albumRawID = t.VideoID
	}
	albumID := &specid.ID{URI: fmt.Sprintf("yt:al:%s", albumRawID)}

	artistRawID := t.ArtistID
	if artistRawID == "" {
		artistRawID = t.Artist
	}
	artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", artistRawID)}
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

	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(t.Title)
	path := fmt.Sprintf("audio/%s/%s - %s.flac", safeArtist, safeArtist, safeTitle)

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
		Year:        t.Year,
		IsDir:       false,
		Type:        "music",
		Path:        path,
		TrackNumber: t.TrackNumber,
	}
}

func (p *Provider) albumToSpec(ytAlbum *YTAlbum, ytTracks []*YTTrack) *spec.Album {
	if ytAlbum == nil {
		return nil
	}
	albumID := &specid.ID{URI: fmt.Sprintf("yt:al:%s", ytAlbum.ID)}
	artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", ytAlbum.ArtistID)}
	coverID := albumID

	specTracks := make([]*spec.TrackChild, 0, len(ytTracks))
	totalDuration := 0
	for _, t := range ytTracks {
		st := p.trackToSpec(t)
		if st != nil {
			specTracks = append(specTracks, st)
			totalDuration += t.Duration
		}
	}

	return &spec.Album{
		ID:         albumID,
		Name:       ytAlbum.Title,
		Title:      ytAlbum.Title,
		Artist:     ytAlbum.Artist,
		ArtistID:   artistID,
		CoverID:    coverID,
		Year:       ytAlbum.Year,
		TrackCount: len(specTracks),
		Duration:   totalDuration,
		Tracks:     specTracks,
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
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)
	for i, raw := range rawIDs {
		wg.Add(1)
		go func(idx int, rID string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			t, err := p.GetTrack(ctx, rID)
			if err == nil {
				res[idx] = t
			}
		}(i, raw)
	}
	wg.Wait()
	return res
}

func (p *Provider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	if rawID == "" {
		return nil, fmt.Errorf("empty album ID")
	}

	// 1. If rawID is a browse ID (starts with MPRE, OLAK, VL, FEmusic)
	if strings.HasPrefix(rawID, "MPRE") || strings.HasPrefix(rawID, "OLAK") || strings.HasPrefix(rawID, "VL") || strings.HasPrefix(rawID, "FEmusic") {
		ytAlbum, ytTracks, err := p.client.GetAlbum(ctx, rawID)
		if err != nil {
			return nil, err
		}
		return p.albumToSpec(ytAlbum, ytTracks), nil
	}

	// 2. Otherwise, rawID might be a track's VideoID
	track, err := p.client.GetTrack(ctx, rawID)
	if err == nil && track != nil {
		if track.AlbumID != "" && (strings.HasPrefix(track.AlbumID, "MPRE") || strings.HasPrefix(track.AlbumID, "OLAK")) {
			ytAlbum, ytTracks, err := p.client.GetAlbum(ctx, track.AlbumID)
			if err == nil && ytAlbum != nil && len(ytTracks) > 0 {
				return p.albumToSpec(ytAlbum, ytTracks), nil
			}
		}

		// Single track album fallback
		specTrack := p.trackToSpec(track)
		specTrack.TrackNumber = 1
		albumID := &specid.ID{URI: fmt.Sprintf("yt:al:%s", rawID)}
		artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", track.ArtistID)}
		coverID := &specid.ID{URI: fmt.Sprintf("yt:tr:%s", track.VideoID)}
		albumName := track.Album
		if albumName == "" {
			albumName = track.Title + " - Single"
		}
		return &spec.Album{
			ID:         albumID,
			Name:       albumName,
			Title:      albumName,
			Artist:     track.Artist,
			ArtistID:   artistID,
			CoverID:    coverID,
			Year:       track.Year,
			TrackCount: 1,
			Duration:   track.Duration,
			Tracks:     []*spec.TrackChild{specTrack},
		}, nil
	}

	// 3. Last resort: try GetAlbum directly
	ytAlbum, ytTracks, err := p.client.GetAlbum(ctx, rawID)
	if err != nil {
		return nil, err
	}
	return p.albumToSpec(ytAlbum, ytTracks), nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	detail, err := p.client.GetArtist(ctx, rawID)
	if err != nil {
		return nil, err
	}
	artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", detail.Artist.ID)}
	coverID := artistID
	return &spec.Artist{
		ID:         artistID,
		Name:       detail.Artist.Name,
		CoverID:    coverID,
		AlbumCount: len(detail.Albums) + len(detail.Singles),
	}, nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	detail, err := p.client.GetArtist(ctx, rawID)
	if err != nil {
		return nil, nil, err
	}

	allAlbums := make([]*spec.Album, 0, len(detail.Albums)+len(detail.Singles))
	for _, alb := range detail.Albums {
		albumID := &specid.ID{URI: fmt.Sprintf("yt:al:%s", alb.ID)}
		artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", detail.Artist.ID)}
		allAlbums = append(allAlbums, &spec.Album{
			ID:       albumID,
			Name:     alb.Title,
			Title:    alb.Title,
			Artist:   detail.Artist.Name,
			ArtistID: artistID,
			CoverID:  albumID,
			Year:     alb.Year,
		})
	}
	for _, s := range detail.Singles {
		albumID := &specid.ID{URI: fmt.Sprintf("yt:al:%s", s.ID)}
		artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", detail.Artist.ID)}
		allAlbums = append(allAlbums, &spec.Album{
			ID:       albumID,
			Name:     s.Title,
			Title:    s.Title,
			Artist:   detail.Artist.Name,
			ArtistID: artistID,
			CoverID:  albumID,
			Year:     s.Year,
		})
	}

	var tracks []*spec.TrackChild
	if !skipTracks && len(detail.TopTracks) > 0 {
		tracks = make([]*spec.TrackChild, 0, len(detail.TopTracks))
		for _, t := range detail.TopTracks {
			st := p.trackToSpec(t)
			if st != nil {
				tracks = append(tracks, st)
			}
		}
	}

	return allAlbums, tracks, nil
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
	rawID := coverID
	if strings.HasPrefix(rawID, "yt:tr:") {
		rawID = strings.TrimPrefix(rawID, "yt:tr:")
	} else if strings.HasPrefix(rawID, "yt:al:") {
		rawID = strings.TrimPrefix(rawID, "yt:al:")
	} else if strings.HasPrefix(rawID, "yt:ar:") {
		rawID = strings.TrimPrefix(rawID, "yt:ar:")
	}

	if p.client != nil {
		if t, ok := p.client.GetCachedTrack(rawID); ok && t.Thumbnail != "" {
			return t.Thumbnail
		}
		if alb, ok := p.client.GetCachedAlbum(rawID); ok && alb.Thumbnail != "" {
			return alb.Thumbnail
		}
		if art, ok := p.client.GetCachedArtist(rawID); ok && art.Thumbnail != "" {
			return art.Thumbnail
		}
		if strings.HasPrefix(rawID, "MPRE") || strings.HasPrefix(rawID, "OLAK") {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			if alb, _, err := p.client.GetAlbum(ctx, rawID); err == nil && alb != nil && alb.Thumbnail != "" {
				return alb.Thumbnail
			}
		}
		if strings.HasPrefix(rawID, "UC") {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			if detail, err := p.client.GetArtist(ctx, rawID); err == nil && detail != nil && detail.Artist != nil && detail.Artist.Thumbnail != "" {
				return detail.Artist.Thumbnail
			}
		}
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
	detail, err := p.client.GetArtist(ctx, artistRawID)
	if err != nil {
		return nil, err
	}
	if len(detail.TopTracks) == 0 {
		return nil, nil
	}
	res := make([]*spec.TrackChild, 0, len(detail.TopTracks))
	for _, t := range detail.TopTracks {
		st := p.trackToSpec(t)
		if st != nil {
			res = append(res, st)
			if limit > 0 && len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	detail, err := p.client.GetArtist(ctx, artistRawID)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.Artist, 0, len(detail.SimilarArtists))
	for _, sa := range detail.SimilarArtists {
		artistID := &specid.ID{URI: fmt.Sprintf("yt:ar:%s", sa.ID)}
		res = append(res, &spec.Artist{
			ID:      artistID,
			Name:    sa.Name,
			CoverID: artistID,
		})
	}
	return res, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	if trackRawID == "" {
		return nil, fmt.Errorf("empty track ID")
	}
	tracks, err := p.client.GetRecommendations(ctx, trackRawID, 50)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.TrackChild, 0, len(tracks))
	for _, t := range tracks {
		st := p.trackToSpec(t)
		if st != nil {
			res = append(res, st)
		}
	}
	return res, nil
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
