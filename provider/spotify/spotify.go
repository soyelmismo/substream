package spotify

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider and provider.ISRCResolver for Spotify
type Provider struct {
	client       *Client
	registry     *provider.Registry
	albumCovers  sync.Map // albumID -> imageURL
	artistCovers sync.Map // artistID -> imageURL
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

	// Cache album cover if available
	if len(t.Album.Images) > 0 && t.Album.Images[0].URL != "" {
		p.albumCovers.Store(t.Album.ID, t.Album.Images[0].URL)
	}

	// Build artist refs & display string
	var artistRefs []*spec.ArtistRef
	var artistNames []string
	for _, a := range t.Artists {
		if a.ID != "" && a.Name != "" {
			artistRefs = append(artistRefs, &spec.ArtistRef{
				ID:   &specid.ID{URI: fmt.Sprintf("sp:ar:%s", a.ID)},
				Name: a.Name,
			})
			artistNames = append(artistNames, a.Name)
		}
	}
	displayArtist := artistName
	if len(artistNames) > 0 {
		displayArtist = strings.Join(artistNames, ", ")
	}

	// Build album artists
	var albumArtists []*spec.ArtistRef
	for _, a := range t.Album.Artists {
		if a.ID != "" && a.Name != "" {
			albumArtists = append(albumArtists, &spec.ArtistRef{
				ID:   &specid.ID{URI: fmt.Sprintf("sp:ar:%s", a.ID)},
				Name: a.Name,
			})
		}
	}

	duration := t.DurationMs / 1000
	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(t.Name)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.flac", safeArtist, t.TrackNumber, safeArtist, safeTitle)

	var isrcs []string
	if isrc := strings.TrimSpace(t.ExternalIDs.ISRC); isrc != "" {
		isrcs = []string{isrc}
	}

	return &spec.TrackChild{
		ID:            trackID,
		Title:         t.Name,
		Album:         t.Album.Name,
		AlbumID:       albumID,
		Artist:        artistName,
		ArtistID:      artistID,
		Artists:       artistRefs,
		DisplayArtist: displayArtist,
		AlbumArtists:  albumArtists,
		ParentID:      albumID,
		CoverID:       coverID,
		TrackNumber:   t.TrackNumber,
		DiscNumber:    t.DiscNumber,
		Duration:      duration,
		Bitrate:       1411,
		ContentType:   "audio/flac",
		Suffix:        "flac",
		Size:          duration * 176400,
		IsDir:         false,
		Type:          "music",
		MediaType:     "song",
		Path:          path,
		Year:          parseYear(t.Album.ReleaseDate),
		ISRC:          isrcs,
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

	if len(a.Images) > 0 && a.Images[0].URL != "" {
		p.albumCovers.Store(a.ID, a.Images[0].URL)
	}

	var artistRefs []*spec.ArtistRef
	var artistNames []string
	for _, art := range a.Artists {
		if art.ID != "" && art.Name != "" {
			artistRefs = append(artistRefs, &spec.ArtistRef{
				ID:   &specid.ID{URI: fmt.Sprintf("sp:ar:%s", art.ID)},
				Name: art.Name,
			})
			artistNames = append(artistNames, art.Name)
		}
	}
	displayArtist := artistName
	if len(artistNames) > 0 {
		displayArtist = strings.Join(artistNames, ", ")
	}

	totalDuration := 0
	var tracks []*spec.TrackChild
	if len(a.Tracks.Items) > 0 {
		tracks = make([]*spec.TrackChild, 0, len(a.Tracks.Items))
		for _, tr := range a.Tracks.Items {
			// Ensure album reference is attached so track has album metadata
			if tr.Album.ID == "" {
				tr.Album = SpotifyAlbumRef{
					ID:          a.ID,
					Name:        a.Name,
					ReleaseDate: a.ReleaseDate,
					Images:      a.Images,
					Artists:     a.Artists,
				}
			}
			tc := p.trackToSpec(&tr)
			if tc != nil {
				tracks = append(tracks, tc)
				totalDuration += tc.Duration
			}
		}
	}

	trackCount := a.TotalTracks
	if len(tracks) > trackCount {
		trackCount = len(tracks)
	}

	return &spec.Album{
		ID:            albumID,
		ParentID:      artistID,
		Name:          a.Name,
		Artist:        artistName,
		ArtistID:      artistID,
		Artists:       artistRefs,
		DisplayArtist: displayArtist,
		CoverID:       coverID,
		TrackCount:    trackCount,
		Duration:      totalDuration,
		Year:          parseYear(a.ReleaseDate),
		Tracks:        tracks,
	}
}

func (p *Provider) artistToSpec(a *SpotifyArtist) *spec.Artist {
	if a == nil {
		return nil
	}
	id := &specid.ID{URI: fmt.Sprintf("sp:ar:%s", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("sp:ar:%s", a.ID)}
	if len(a.Images) > 0 && a.Images[0].URL != "" {
		p.artistCovers.Store(a.ID, a.Images[0].URL)
	}
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
	albums, err := p.client.GetArtistAlbums(ctx, rawID, 50, 0)
	if err != nil || len(albums) == 0 {
		// Resilient fallback: lookup artist name, then search albums by artist
		if art, aerr := p.client.GetArtist(ctx, rawID); aerr == nil && art != nil && art.Name != "" {
			if sres, serr := p.client.Search(ctx, fmt.Sprintf("artist:\"%s\"", art.Name), 50, 0); serr == nil && sres != nil && len(sres.Albums.Items) > 0 {
				albums = sres.Albums.Items
				err = nil
			}
		}
	}
	if err != nil && len(albums) == 0 {
		return nil, nil, err
	}

	resAlbums := make([]*spec.Album, 0, len(albums))
	for i := range albums {
		resAlbums = append(resAlbums, p.albumToSpec(&albums[i]))
	}

	var tracks []*spec.TrackChild
	if !skipTracks {
		top, terr := p.client.GetArtistTopTracks(ctx, rawID)
		if terr != nil || len(top) == 0 {
			// Fallback: search top tracks for artist
			if art, aerr := p.client.GetArtist(ctx, rawID); aerr == nil && art != nil && art.Name != "" {
				if sres, serr := p.client.Search(ctx, art.Name, 10, 0); serr == nil && sres != nil {
					top = sres.Tracks.Items
				}
			}
		}
		if len(top) > 0 {
			tracks = make([]*spec.TrackChild, 0, len(top))
			for i := range top {
				tc := p.trackToSpec(&top[i])
				if tc != nil {
					tracks = append(tracks, tc)
				}
			}
		}
	}
	return resAlbums, tracks, nil
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
	if strings.HasPrefix(coverID, "http://") || strings.HasPrefix(coverID, "https://") {
		return coverID
	}
	if strings.HasPrefix(coverID, "ab67") {
		return fmt.Sprintf("https://i.scdn.co/image/%s", coverID)
	}

	// 1. Check in-memory album cover cache
	if img, ok := p.albumCovers.Load(coverID); ok {
		if urlStr, ok := img.(string); ok && urlStr != "" {
			return urlStr
		}
	}

	// 2. Check in-memory artist cover cache
	if img, ok := p.artistCovers.Load(coverID); ok {
		if urlStr, ok := img.(string); ok && urlStr != "" {
			return urlStr
		}
	}

	// 3. Lazy load on-demand if not cached yet
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	if album, err := p.client.GetAlbum(ctx, coverID); err == nil && album != nil && len(album.Images) > 0 {
		p.albumCovers.Store(coverID, album.Images[0].URL)
		return album.Images[0].URL
	}
	if artist, err := p.client.GetArtist(ctx, coverID); err == nil && artist != nil && len(artist.Images) > 0 {
		p.artistCovers.Store(coverID, artist.Images[0].URL)
		return artist.Images[0].URL
	}
	if track, err := p.client.GetTrack(ctx, coverID); err == nil && track != nil && len(track.Album.Images) > 0 {
		p.albumCovers.Store(coverID, track.Album.Images[0].URL)
		return track.Album.Images[0].URL
	}

	return ""
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	resp, err := p.client.GetLyrics(ctx, rawID)
	if err != nil {
		return nil, err
	}
	if len(resp.Lyrics.Lines) == 0 {
		return nil, fmt.Errorf("no lyrics available")
	}

	synced := resp.Lyrics.SyncType == "LINE_SYNCED"
	lines := make([]spec.Lyric, 0, len(resp.Lyrics.Lines))
	for _, l := range resp.Lyrics.Lines {
		var startMsPtr *int64
		if ms, err := strconv.ParseInt(l.StartTimeMs, 10, 64); err == nil {
			startMsPtr = &ms
		}
		lines = append(lines, spec.Lyric{
			Start: startMsPtr,
			Value: l.Words,
		})
	}

	lang := resp.Lyrics.Language
	if lang == "" {
		lang = "und"
	}

	return &spec.StructuredLyrics{
		Lang:   lang,
		Synced: synced,
		Lines:  lines,
	}, nil
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "top", limit, 0)
}

func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	tracks, err := p.client.GetArtistTopTracks(ctx, artistRawID)
	if err != nil || len(tracks) == 0 {
		if art, aerr := p.client.GetArtist(ctx, artistRawID); aerr == nil && art != nil && art.Name != "" {
			if sres, serr := p.client.Search(ctx, art.Name, limit, 0); serr == nil && sres != nil {
				tracks = sres.Tracks.Items
				err = nil
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(tracks) > limit {
		tracks = tracks[:limit]
	}
	out := make([]*spec.TrackChild, 0, len(tracks))
	for i := range tracks {
		tc := p.trackToSpec(&tracks[i])
		if tc != nil {
			out = append(out, tc)
		}
	}
	return out, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	artists, err := p.client.GetSimilarArtists(ctx, artistRawID)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Artist, 0, len(artists))
	for i := range artists {
		out = append(out, p.artistToSpec(&artists[i]))
	}
	return out, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	tracks, err := p.client.GetRecommendations(ctx, trackRawID, 20)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, 0, len(tracks))
	for i := range tracks {
		tc := p.trackToSpec(&tracks[i])
		if tc != nil {
			out = append(out, tc)
		}
	}
	return out, nil
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

