package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	spotifyEmbedURL  = "https://open.spotify.com/embed/playlist/%s"
	spotifyTrackURL  = "https://open.spotify.com/embed/track/%s"
	spotifySpclient  = "https://spclient.wg.spotify.com/playlist/v2/playlist/%s"
	spotifyUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

var (
	spotifyPlaylistRegex = regexp.MustCompile(`open\.spotify\.com/playlist/([a-zA-Z0-9]+)`)
	nextDataRegex        = regexp.MustCompile(`<script id="__NEXT_DATA__"[^>]*>([^<]+)</script>`)
)

// SpotifyProvider implements anonymous playlist extraction from Spotify
type SpotifyProvider struct {
	client      *http.Client
	cachedToken string
	tokenExpiry time.Time
}

// Name returns the provider identifier
func (s *SpotifyProvider) Name() string {
	return "spotify"
}

// Match checks if the URL is a Spotify playlist link
func (s *SpotifyProvider) Match(url string) bool {
	return spotifyPlaylistRegex.MatchString(url)
}

// getClient returns the HTTP client (lazy initialization)
func (s *SpotifyProvider) getClient() *http.Client {
	if s.client == nil {
		s.client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	return s.client
}

// Fetch retrieves playlist data using spclient as primary, embed as fallback
func (s *SpotifyProvider) Fetch(ctx context.Context, playlistURL string) (*ImportedPlaylist, error) {
	matches := spotifyPlaylistRegex.FindStringSubmatch(playlistURL)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid Spotify playlist URL")
	}
	playlistID := matches[1]

	// Step 1: Get token from embed page (always needed for spclient)
	if err := s.fetchTokenFromEmbed(ctx, playlistID); err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	// Step 2: Try spclient API first (has full description and all tracks)
	result, err := s.fetchFromSpclient(ctx, playlistID)
	if err == nil {
		log.Printf("[SPOTIFY] Successfully fetched from spclient: %s (%d tracks)", result.Title, len(result.Tracks))
		return result, nil
	}

	log.Printf("[SPOTIFY] Spclient failed (%v), falling back to embed", err)

	// Step 3: Fallback to embed page (limited data)
	return s.fetchFromEmbed(ctx, playlistID)
}

// fetchTokenFromEmbed extracts anonymous token from embed page
func (s *SpotifyProvider) fetchTokenFromEmbed(ctx context.Context, playlistID string) error {
	url := fmt.Sprintf(spotifyEmbedURL, playlistID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", spotifyUserAgent)
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := s.getClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("embed returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	match := nextDataRegex.FindSubmatch(body)
	if match == nil {
		return fmt.Errorf("no __NEXT_DATA__ found")
	}

	var data map[string]interface{}
	if err := json.Unmarshal(match[1], &data); err != nil {
		return err
	}

	// Extract token from multiple possible paths
	tokenPaths := [][]string{
		{"props", "pageProps", "state", "settings", "session"},
		{"props", "pageProps", "settings", "session"},
		{"props", "pageProps", "session"},
	}

	for _, path := range tokenPaths {
		if v := resolvePath(data, path); v != nil {
			if sess, ok := v.(map[string]interface{}); ok {
				if token, ok := sess["accessToken"].(string); ok && token != "" {
					s.cachedToken = token
					if ms, ok := sess["accessTokenExpirationTimestampMs"].(float64); ok {
						s.tokenExpiry = time.Now().Add(time.Duration(ms) * time.Millisecond)
					}
					return nil
				}
			}
		}
	}

	return fmt.Errorf("no access token found")
}

// fetchFromSpclient uses spclient API for full playlist data
func (s *SpotifyProvider) fetchFromSpclient(ctx context.Context, playlistID string) (*ImportedPlaylist, error) {
	if s.cachedToken == "" {
		return nil, fmt.Errorf("no token available")
	}

	url := fmt.Sprintf(spotifySpclient, playlistID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+s.cachedToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", spotifyUserAgent)

	resp, err := s.getClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("spclient returned %d", resp.StatusCode)
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	result := &ImportedPlaylist{
		Tracks: []ImportedTrack{},
	}

	// Extract metadata from spclient response
	// Name and description are in attributes object
	if attrs, ok := data["attributes"].(map[string]interface{}); ok {
		if name, ok := attrs["name"].(string); ok {
			result.Title = name
		}
		if desc, ok := attrs["description"].(string); ok && desc != "" {
			result.Description = desc
		}
		// Extract cover from attributes.picture
		if pic, ok := attrs["picture"].(string); ok && pic != "" {
			// Convert picture ID to full URL
			result.CoverURL = fmt.Sprintf("https://i.scdn.co/image/%s", pic)
		}
	}

	// Fallback: try top-level fields (if API changes)
	if result.Title == "" {
		if name, ok := data["name"].(string); ok {
			result.Title = name
		}
	}

	// Fallback description from ownerUsername
	if result.Description == "" {
		if owner, ok := data["ownerUsername"].(string); ok && owner != "" {
			result.Description = fmt.Sprintf("Playlist by %s", owner)
		}
	}

	// Extract tracks
	contents, _ := data["contents"].(map[string]interface{})
	items, _ := contents["items"].([]interface{})

	for _, item := range items {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		uri, _ := itemMap["uri"].(string)
		if !strings.HasPrefix(uri, "spotify:track:") {
			continue
		}

		trackID := strings.TrimPrefix(uri, "spotify:track:")
		track, err := s.fetchTrackMetadata(ctx, trackID)
		if err != nil {
			// Minimal fallback
			track = ImportedTrack{
				Title:  fmt.Sprintf("Track %s", trackID),
				Artist: "Unknown",
			}
		}
		result.Tracks = append(result.Tracks, track)
	}

	return result, nil
}

// fetchFromEmbed uses embed page as fallback (limited to ~100 tracks)
func (s *SpotifyProvider) fetchFromEmbed(ctx context.Context, playlistID string) (*ImportedPlaylist, error) {
	url := fmt.Sprintf(spotifyEmbedURL, playlistID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", spotifyUserAgent)
	req.Header.Set("Accept", "text/html")

	resp, err := s.getClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	match := nextDataRegex.FindSubmatch(body)
	if match == nil {
		return nil, fmt.Errorf("no __NEXT_DATA__")
	}

	var data map[string]interface{}
	if err := json.Unmarshal(match[1], &data); err != nil {
		return nil, err
	}

	entity, err := extractEntity(data)
	if err != nil {
		return nil, err
	}

	result := &ImportedPlaylist{
		Title:       getString(entity, "name", "title", "Unknown"),
		Description: getString(entity, "subtitle", "", ""),
		Tracks:      []ImportedTrack{},
	}

	// Get cover
	if coverArt, ok := entity["coverArt"].(map[string]interface{}); ok {
		if sources, ok := coverArt["sources"].([]interface{}); ok && len(sources) > 0 {
			if last, ok := sources[len(sources)-1].(map[string]interface{}); ok {
				if url, ok := last["url"].(string); ok {
					result.CoverURL = url
				}
			}
		}
	}

	// Get tracks (limited)
	trackList, _ := entity["trackList"].([]interface{})
	for _, item := range trackList {
		track, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		result.Tracks = append(result.Tracks, parseTrack(track))
	}

	return result, nil
}

// fetchTrackMetadata gets track details from embed page
func (s *SpotifyProvider) fetchTrackMetadata(ctx context.Context, trackID string) (ImportedTrack, error) {
	url := fmt.Sprintf(spotifyTrackURL, trackID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ImportedTrack{}, err
	}

	req.Header.Set("User-Agent", spotifyUserAgent)
	req.Header.Set("Accept", "text/html")

	resp, err := s.getClient().Do(req)
	if err != nil {
		return ImportedTrack{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ImportedTrack{}, err
	}

	match := nextDataRegex.FindSubmatch(body)
	if match == nil {
		return ImportedTrack{}, fmt.Errorf("no data")
	}

	var data map[string]interface{}
	if err := json.Unmarshal(match[1], &data); err != nil {
		return ImportedTrack{}, err
	}

	entity, err := extractEntity(data)
	if err != nil {
		return ImportedTrack{}, err
	}

	return parseTrack(entity), nil
}

// Helper functions
func resolvePath(data map[string]interface{}, path []string) interface{} {
	current := data
	for i, key := range path {
		if i == len(path)-1 {
			return current[key]
		}
		next, ok := current[key].(map[string]interface{})
		if !ok {
			return nil
		}
		current = next
	}
	return nil
}

func extractEntity(data map[string]interface{}) (map[string]interface{}, error) {
	paths := [][]string{
		{"props", "pageProps", "state", "data", "entity"},
		{"props", "pageProps", "data", "entity"},
		{"props", "pageProps", "entity"},
	}

	for _, path := range paths {
		if v := resolvePath(data, path); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				return m, nil
			}
		}
	}

	// Deep search fallback
	if v := deepFind(data, "trackList"); v != nil {
		if m, ok := v.(map[string]interface{}); ok {
			return m, nil
		}
	}

	return nil, fmt.Errorf("entity not found")
}

func deepFind(data interface{}, key string) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		if _, ok := v[key]; ok {
			return v
		}
		for _, val := range v {
			if r := deepFind(val, key); r != nil {
				return r
			}
		}
	case []interface{}:
		for _, item := range v {
			if r := deepFind(item, key); r != nil {
				return r
			}
		}
	}
	return nil
}

func parseTrack(track map[string]interface{}) ImportedTrack {
	result := ImportedTrack{
		Title:  getString(track, "title", "name", "Unknown"),
		Artist: getString(track, "subtitle", "", ""),
	}

	if artists, ok := track["artists"].([]interface{}); ok {
		var names []string
		for _, a := range artists {
			if m, ok := a.(map[string]interface{}); ok {
				if n, ok := m["name"].(string); ok {
					names = append(names, n)
				}
			}
		}
		if len(names) > 0 {
			result.Artist = strings.Join(names, ", ")
		}
	}

	// Debug: log available keys for album extraction
	if album, ok := track["album"].(map[string]interface{}); ok {
		result.Album = getString(album, "name", "title", "")
	} else if track["album"] != nil {
		// Album field exists but is not a map - log for debug
		log.Printf("[SPOTIFY DEBUG] Album field type: %T, value: %v", track["album"], track["album"])
	}

	// Also try alternative album fields
	if result.Album == "" {
		for _, key := range []string{"albumName", "album_title", "releaseName"} {
			if val, ok := track[key].(string); ok && val != "" {
				result.Album = val
				break
			}
		}
	}

	return result
}

func getString(data map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if key == "" {
			continue
		}
		if val, ok := data[key].(string); ok {
			return val
		}
	}
	return ""
}

func isSpotifyURL(s string) bool {
	return strings.Contains(s, "open.spotify.com/playlist/") ||
		strings.Contains(s, "spotify:playlist:")
}
