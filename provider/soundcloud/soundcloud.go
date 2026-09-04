package soundcloud

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider for SoundCloud
type Provider struct {
	client     *Client
	coverCache sync.Map // rawID -> imageURL
}

// Ensure Provider implements provider.MusicProvider
var _ provider.MusicProvider = (*Provider)(nil)

// New creates a new SoundCloud provider
func New(opts ...ClientOptions) *Provider {
	var opt ClientOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	return &Provider{
		client: NewClient(opt),
	}
}

func (p *Provider) ID() string {
	return "sc"
}

func (p *Provider) Name() string {
	return "SoundCloud"
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
	if len(dateStr) >= 4 {
		y, _ := strconv.Atoi(dateStr[:4])
		return y
	}
	return 0
}

func (p *Provider) trackToSpec(t *SoundCloudTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	rawID := strconv.FormatInt(t.ID, 10)
	artistRawID := strconv.FormatInt(t.User.ID, 10)

	trackID := &specid.ID{URI: fmt.Sprintf("sc:tr:%s", rawID)}
	artistID := &specid.ID{URI: fmt.Sprintf("sc:ar:%s", artistRawID)}
	albumID := &specid.ID{URI: fmt.Sprintf("sc:al:%s", rawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("sc:tr:%s", rawID)}

	artwork := t.ArtworkURL
	if artwork == "" {
		artwork = t.User.AvatarURL
	}
	if artwork != "" {
		p.coverCache.Store(rawID, artwork)
	}

	durationSec := t.Duration / 1000
	if durationSec <= 0 {
		durationSec = 1
	}

	artistName := strings.TrimSpace(t.User.Username)
	if artistName == "" {
		artistName = "Unknown Artist"
	}
	title := strings.TrimSpace(t.Title)
	if title == "" {
		title = "Untitled"
	}

	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(title)
	path := fmt.Sprintf("soundcloud/%s/%s.mp3", safeArtist, safeTitle)

	year := parseYear(t.ReleaseDate)
	if year == 0 {
		year = parseYear(t.CreatedAt)
	}

	albumName := "SoundCloud"
	if t.Genre != "" {
		albumName = fmt.Sprintf("SoundCloud [%s]", t.Genre)
	}

	return &spec.TrackChild{
		ID:          trackID,
		Title:       title,
		Album:       albumName,
		AlbumID:     albumID,
		Artist:      artistName,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: 1,
		DiscNumber:  1,
		Duration:    durationSec,
		Bitrate:     128,
		ContentType: "audio/mpeg",
		Suffix:      "mp3",
		Size:        durationSec * 16000, // ~128kbps = 16kB/s
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        year,
		Genre:       t.Genre,
	}
}

func (p *Provider) albumToSpec(a *SoundCloudPlaylist) *spec.Album {
	if a == nil {
		return nil
	}
	rawID := strconv.FormatInt(a.ID, 10)
	artistRawID := strconv.FormatInt(a.User.ID, 10)
	albumID := &specid.ID{URI: fmt.Sprintf("sc:al:%s", rawID)}
	artistID := &specid.ID{URI: fmt.Sprintf("sc:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("sc:al:%s", rawID)}

	artwork := a.ArtworkURL
	if artwork == "" {
		artwork = a.User.AvatarURL
	}
	if artwork != "" {
		p.coverCache.Store(rawID, artwork)
	}

	artistName := strings.TrimSpace(a.User.Username)
	if artistName == "" {
		artistName = "Unknown Artist"
	}
	title := strings.TrimSpace(a.Title)
	if title == "" {
		title = "Untitled Playlist"
	}

	year := parseYear(a.ReleaseDate)
	if year == 0 {
		year = parseYear(a.CreatedAt)
	}

	return &spec.Album{
		ID:         albumID,
		ParentID:   artistID,
		Name:       title,
		Artist:     artistName,
		ArtistID:   artistID,
		CoverID:    coverID,
		TrackCount: a.TrackCount,
		Duration:   a.Duration / 1000,
		Year:       year,
	}
}

func (p *Provider) userToSpec(u *SoundCloudUser) *spec.Artist {
	if u == nil {
		return nil
	}
	rawID := strconv.FormatInt(u.ID, 10)
	artistID := &specid.ID{URI: fmt.Sprintf("sc:ar:%s", rawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("sc:ar:%s", rawID)}

	if u.AvatarURL != "" {
		p.coverCache.Store(rawID, u.AvatarURL)
	}

	name := strings.TrimSpace(u.Username)
	if name == "" {
		name = "Unknown Artist"
	}

	return &spec.Artist{
		ID:         artistID,
		Name:       name,
		CoverID:    coverID,
		AlbumCount: u.TrackCount,
	}
}

// GetTrack fetches track metadata by ID
func (p *Provider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	track, err := p.client.GetTrack(ctx, rawID)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(track), nil
}

// GetTracksBatch fetches multiple tracks concurrently
func (p *Provider) GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild {
	var wg sync.WaitGroup
	results := make([]*spec.TrackChild, len(rawIDs))

	for i, rawID := range rawIDs {
		wg.Add(1)
		go func(idx int, id string) {
			defer wg.Done()
			if track, err := p.client.GetTrack(ctx, id); err == nil && track != nil {
				results[idx] = p.trackToSpec(track)
			}
		}(i, rawID)
	}

	wg.Wait()

	var clean []*spec.TrackChild
	for _, t := range results {
		if t != nil {
			clean = append(clean, t)
		}
	}
	return clean
}

// GetAlbum fetches playlist/album metadata and its tracks
func (p *Provider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	pl, err := p.client.GetPlaylist(ctx, rawID)
	if err != nil {
		return nil, err
	}
	specAlbum := p.albumToSpec(pl)
	if len(pl.Tracks) > 0 {
		for i, t := range pl.Tracks {
			st := p.trackToSpec(&t)
			if st != nil {
				st.TrackNumber = i + 1
				st.Album = specAlbum.Name
				st.AlbumID = specAlbum.ID
				specAlbum.Tracks = append(specAlbum.Tracks, st)
			}
		}
	}
	return specAlbum, nil
}

// GetArtist fetches artist profile
func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	u, err := p.client.GetUser(ctx, rawID)
	if err != nil {
		return nil, err
	}
	return p.userToSpec(u), nil
}

// GetArtistAlbums fetches albums/playlists and top tracks for an artist
func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	var albums []*spec.Album
	var tracks []*spec.TrackChild
	var wg sync.WaitGroup

	// Fetch playlists/albums
	wg.Add(1)
	go func() {
		defer wg.Done()
		if plList, err := p.client.GetUserAlbums(ctx, rawID, 20); err == nil {
			for _, pl := range plList {
				albums = append(albums, p.albumToSpec(pl))
			}
		}
	}()

	// Fetch top tracks if requested
	if !skipTracks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tList, err := p.client.GetUserTracks(ctx, rawID, 30); err == nil {
				for _, t := range tList {
					tracks = append(tracks, p.trackToSpec(t))
				}
			}
		}()
	}

	wg.Wait()
	return albums, tracks, nil
}

// SearchTracks searches SoundCloud for tracks
func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	tracks, err := p.client.SearchTracks(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}

	var results []*spec.TrackChild
	for _, t := range tracks {
		if st := p.trackToSpec(t); st != nil {
			results = append(results, st)
		}
	}
	return results, nil
}

// SearchAlbums searches SoundCloud for playlists/albums
func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	albums, err := p.client.SearchAlbums(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}

	var results []*spec.Album
	for _, a := range albums {
		if sa := p.albumToSpec(a); sa != nil {
			results = append(results, sa)
		}
	}
	return results, nil
}

// SearchArtists searches SoundCloud for users/artists
func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	users, err := p.client.SearchUsers(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}

	var results []*spec.Artist
	for _, u := range users {
		if su := p.userToSpec(u); su != nil {
			results = append(results, su)
		}
	}
	return results, nil
}

// Search executes concurrent search across tracks, albums, and artists
func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	var results provider.SearchResults
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if tracks, err := p.SearchTracks(ctx, query, limit, offset); err == nil {
			results.Tracks = tracks
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if albums, err := p.SearchAlbums(ctx, query, limit, offset); err == nil {
			results.Albums = albums
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if artists, err := p.SearchArtists(ctx, query, limit, offset); err == nil {
			results.Artists = artists
		}
	}()

	wg.Wait()
	return &results, nil
}

// GetStreamURL resolves playable audio stream URL for a track
func (p *Provider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	return p.client.GetStreamURL(ctx, rawID)
}

// GetCoverURL returns high-resolution cover image URL
func (p *Provider) GetCoverURL(coverID string, size int) string {
	rawID := coverID
	if strings.HasPrefix(rawID, "sc:tr:") {
		rawID = strings.TrimPrefix(rawID, "sc:tr:")
	} else if strings.HasPrefix(rawID, "sc:al:") {
		rawID = strings.TrimPrefix(rawID, "sc:al:")
	} else if strings.HasPrefix(rawID, "sc:ar:") {
		rawID = strings.TrimPrefix(rawID, "sc:ar:")
	}

	imgURL := ""
	if strings.HasPrefix(rawID, "http://") || strings.HasPrefix(rawID, "https://") {
		imgURL = rawID
	} else if val, ok := p.coverCache.Load(rawID); ok {
		imgURL = val.(string)
	}

	if imgURL == "" {
		return ""
	}

	// Upgrade SoundCloud artwork to 500x500 if larger image requested
	if strings.Contains(imgURL, "-large.") {
		if size > 100 {
			imgURL = strings.Replace(imgURL, "-large.", "-t500x500.", 1)
		}
	}

	return imgURL
}

// GetLyrics returns nil as SoundCloud doesn't have an official lyrics API
func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, fmt.Errorf("lyrics not supported on soundcloud")
}

// GetTopTracks searches popular tracks on SoundCloud
func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "electronic dance hip hop pop", limit, 0)
}

// GetArtistTopTracks fetches tracks for a given artist
func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	tracks, err := p.client.GetUserTracks(ctx, artistRawID, limit)
	if err != nil {
		return nil, err
	}
	var results []*spec.TrackChild
	for _, t := range tracks {
		results = append(results, p.trackToSpec(t))
	}
	return results, nil
}

// GetSimilarArtists finds users by query based on the artist name
func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	u, err := p.client.GetUser(ctx, artistRawID)
	if err != nil {
		return nil, err
	}
	return p.SearchArtists(ctx, u.Username, 5, 0)
}

// GetRecommendations finds related tracks for a track
func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	tracks, err := p.client.GetRelatedTracks(ctx, trackRawID, 15)
	if err != nil {
		return nil, err
	}
	var results []*spec.TrackChild
	for _, t := range tracks {
		results = append(results, p.trackToSpec(t))
	}
	return results, nil
}

// MatchTrackByMetadata matches a track by artist, title, and duration for cross-provider fallback
func (p *Provider) MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error) {
	query := fmt.Sprintf("%s %s", artist, title)
	tracks, err := p.client.SearchTracks(ctx, query, 5, 0)
	if err != nil {
		return nil, err
	}

	for _, t := range tracks {
		trackDur := t.Duration / 1000
		diff := math.Abs(float64(trackDur - expectedDuration))
		if expectedDuration <= 0 || diff <= 15 {
			return p.trackToSpec(t), nil
		}
	}

	if len(tracks) > 0 {
		return p.trackToSpec(tracks[0]), nil
	}

	return nil, fmt.Errorf("no matching track found on soundcloud")
}
