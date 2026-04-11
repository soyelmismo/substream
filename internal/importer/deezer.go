package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	deezerAPIURL     = "https://api.deezer.com"
	deezerPlaylistRe = `deezer\.com/.*/playlist/(\d+)`
	deezerUserAgent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
)

var deezerPlaylistRegex = regexp.MustCompile(deezerPlaylistRe)

// DeezerProvider implements playlist extraction from Deezer API
type DeezerProvider struct {
	client *http.Client
}

// Name returns the provider identifier
func (d *DeezerProvider) Name() string {
	return "deezer"
}

// Match checks if the URL is a Deezer playlist link
func (d *DeezerProvider) Match(url string) bool {
	return deezerPlaylistRegex.MatchString(url)
}

// getClient returns the HTTP client (lazy initialization)
func (d *DeezerProvider) getClient() *http.Client {
	if d.client == nil {
		d.client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	return d.client
}

// Deezer API response structures
type deezerPlaylistResponse struct {
	ID          int               `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Picture     string            `json:"picture"`
	PictureBig  string            `json:"picture_big"`
	Tracks      deezerTrackList   `json:"tracks"`
}

type deezerTrackList struct {
	Data []deezerTrack `json:"data"`
}

type deezerTrack struct {
	ID       int           `json:"id"`
	Title    string        `json:"title"`
	Duration int           `json:"duration"`
	Artist   deezerArtist  `json:"artist"`
	Album    deezerAlbum   `json:"album"`
	ISRC     string        `json:"isrc"`
}

type deezerArtist struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type deezerAlbum struct {
	ID     int    `json:"id"`
	Title  string `json:"title"`
	Cover  string `json:"cover"`
}

// Fetch retrieves playlist data from Deezer API
func (d *DeezerProvider) Fetch(ctx context.Context, playlistURL string) (*ImportedPlaylist, error) {
	// Extract playlist ID
	matches := deezerPlaylistRegex.FindStringSubmatch(playlistURL)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid Deezer playlist URL")
	}
	playlistID := matches[1]

	// Fetch playlist from Deezer API
	url := fmt.Sprintf("%s/playlist/%s", deezerAPIURL, playlistID)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	
	req.Header.Set("User-Agent", deezerUserAgent)
	
	resp, err := d.getClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch playlist: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("deezer API returned %d: %s", resp.StatusCode, string(body))
	}
	
	var playlist deezerPlaylistResponse
	if err := json.NewDecoder(resp.Body).Decode(&playlist); err != nil {
		return nil, fmt.Errorf("decode playlist: %w", err)
	}
	
	// Check for API error
	if playlist.ID == 0 && playlist.Title == "" {
		return nil, fmt.Errorf("playlist not found or private")
	}
	
	// Convert to ImportedPlaylist
	result := &ImportedPlaylist{
		Title:       playlist.Title,
		Description: playlist.Description,
		CoverURL:    playlist.PictureBig,
		Tracks:      make([]ImportedTrack, 0, len(playlist.Tracks.Data)),
	}
	
	// Convert tracks
	for _, track := range playlist.Tracks.Data {
		importedTrack := ImportedTrack{
			Title:  track.Title,
			Artist: track.Artist.Name,
			Album:  track.Album.Title,
			ISRC:   track.ISRC,
		}
		result.Tracks = append(result.Tracks, importedTrack)
	}
	
	return result, nil
}

// isDeezerURL helper for quick URL detection
func isDeezerURL(s string) bool {
	return strings.Contains(s, "deezer.com/playlist/")
}
