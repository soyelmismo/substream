package soundcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultAPIBaseURL   = "https://api-v2.soundcloud.com"
	defaultUserAgent    = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
	fallbackClientID    = "Pb72ranhoyt6gw7hM7TkzUItXlMWSNSo"
	clientIDCachePeriod = 12 * time.Hour
)

var (
	scriptSrcRegex = regexp.MustCompile(`src="(https://[^"]+\.js)"`)
	clientIDRegexes = []*regexp.Regexp{
		regexp.MustCompile(`client_id[:=]"([a-zA-Z0-9]{32})"`),
		regexp.MustCompile(`"clientId":"([a-zA-Z0-9]{32})"`),
		regexp.MustCompile(`client_id=([a-zA-Z0-9]{32})`),
	}
)

// Client interacts with the SoundCloud API v2
type Client struct {
	httpClient      *http.Client
	baseURL         string
	clientID        string
	clientIDExpires time.Time
	clientIDMu      sync.RWMutex
}

// ClientOptions configures the SoundCloud client
type ClientOptions struct {
	BaseURL  string
	ClientID string
	Timeout  time.Duration
}

// NewClient creates a new SoundCloud API client
func NewClient(opts ClientOptions) *Client {
	baseURL := opts.BaseURL
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	return &Client{
		httpClient: &http.Client{Timeout: timeout},
		baseURL:    strings.TrimRight(baseURL, "/"),
		clientID:   opts.ClientID,
	}
}

// getClientID retrieves a valid client_id, refreshing it dynamically if needed
func (c *Client) getClientID(ctx context.Context) (string, error) {
	c.clientIDMu.RLock()
	if c.clientID != "" && time.Now().Before(c.clientIDExpires) {
		id := c.clientID
		c.clientIDMu.RUnlock()
		return id, nil
	}
	c.clientIDMu.RUnlock()

	c.clientIDMu.Lock()
	defer c.clientIDMu.Unlock()

	// Double-check after acquiring write lock
	if c.clientID != "" && time.Now().Before(c.clientIDExpires) {
		return c.clientID, nil
	}

	// Scrape soundcloud.com for client_id
	id, err := c.scrapeClientID(ctx)
	if err != nil {
		log.Printf("[SOUNDCLOUD] ⚠️ Failed to scrape client_id, using fallback: %v", err)
		c.clientID = fallbackClientID
		c.clientIDExpires = time.Now().Add(1 * time.Hour)
		return fallbackClientID, nil
	}

	c.clientID = id
	c.clientIDExpires = time.Now().Add(clientIDCachePeriod)
	log.Printf("[SOUNDCLOUD] 🔑 Successfully discovered client_id: %s (valid for %v)", id, clientIDCachePeriod)
	return id, nil
}

// scrapeClientID extracts client_id from soundcloud.com Javascript bundles
func (c *Client) scrapeClientID(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://soundcloud.com", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch soundcloud.com: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read soundcloud.com: %w", err)
	}

	matches := scriptSrcRegex.FindAllStringSubmatch(string(body), -1)
	if len(matches) == 0 {
		return "", fmt.Errorf("no scripts found on soundcloud.com")
	}

	// Search scripts in reverse order (vendor and app chunks are near the end)
	for i := len(matches) - 1; i >= 0; i-- {
		scriptURL := matches[i][1]
		sReq, err := http.NewRequestWithContext(ctx, http.MethodGet, scriptURL, nil)
		if err != nil {
			continue
		}
		sReq.Header.Set("User-Agent", defaultUserAgent)

		sResp, err := c.httpClient.Do(sReq)
		if err != nil {
			continue
		}
		sBody, err := io.ReadAll(sResp.Body)
		sResp.Body.Close()
		if err != nil {
			continue
		}

		sText := string(sBody)
		for _, re := range clientIDRegexes {
			if sub := re.FindStringSubmatch(sText); len(sub) > 1 {
				return sub[1], nil
			}
		}
	}

	return "", fmt.Errorf("client_id not found in any scripts")
}

// get executes an API request with auto client_id injection and retry on 401/403
func (c *Client) get(ctx context.Context, endpoint string, query url.Values, target interface{}) error {
	for attempt := 0; attempt < 2; attempt++ {
		clientID, err := c.getClientID(ctx)
		if err != nil {
			return err
		}

		if query == nil {
			query = url.Values{}
		}
		query.Set("client_id", clientID)

		var reqURL string
		if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
			reqURL = endpoint
			if strings.Contains(reqURL, "?") {
				reqURL += "&" + query.Encode()
			} else {
				reqURL += "?" + query.Encode()
			}
		} else {
			reqURL = fmt.Sprintf("%s/%s?%s", c.baseURL, strings.TrimPrefix(endpoint, "/"), query.Encode())
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("User-Agent", defaultUserAgent)
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("execute request: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("read response: %w", err)
		}

		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			// Invalidate client_id and retry once
			log.Printf("[SOUNDCLOUD] ⚠️ Got HTTP %d with client_id %s, refreshing client_id...", resp.StatusCode, clientID)
			c.clientIDMu.Lock()
			c.clientID = ""
			c.clientIDMu.Unlock()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("soundcloud API %s returned HTTP %d: %s", endpoint, resp.StatusCode, string(body))
		}

		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("unmarshal soundcloud response: %w", err)
		}
		return nil
	}

	return fmt.Errorf("soundcloud request failed after retries")
}

// GetTrack fetches track metadata by ID
func (c *Client) GetTrack(ctx context.Context, trackID string) (*SoundCloudTrack, error) {
	var track SoundCloudTrack
	if err := c.get(ctx, fmt.Sprintf("/tracks/%s", trackID), nil, &track); err != nil {
		return nil, err
	}
	return &track, nil
}

// GetStreamURL resolves the playable audio URL for a track
func (c *Client) GetStreamURL(ctx context.Context, trackID string) (string, error) {
	track, err := c.GetTrack(ctx, trackID)
	if err != nil {
		return "", fmt.Errorf("get track %s: %w", trackID, err)
	}

	transcodings := track.Media.Transcodings
	if len(transcodings) == 0 {
		return "", fmt.Errorf("no transcodings available for track %s", trackID)
	}

	// 1. First choice: Progressive MP3 (fast, seekable, raw audio without HLS overhead)
	var selected *SoundCloudTranscoding
	for _, tc := range transcodings {
		if tc.Format.Protocol == "progressive" && strings.Contains(tc.Format.MimeType, "mpeg") {
			selected = &tc
			break
		}
	}

	// 2. Second choice: Any progressive stream
	if selected == nil {
		for _, tc := range transcodings {
			if tc.Format.Protocol == "progressive" {
				selected = &tc
				break
			}
		}
	}

	// 3. Third choice: HLS Opus or MP3
	if selected == nil {
		for _, tc := range transcodings {
			if tc.Format.Protocol == "hls" {
				selected = &tc
				break
			}
		}
	}

	if selected == nil {
		selected = &transcodings[0]
	}

	var streamResp SoundCloudStreamResponse
	if err := c.get(ctx, selected.URL, nil, &streamResp); err != nil {
		return "", fmt.Errorf("resolve stream URL for %s: %w", trackID, err)
	}

	if streamResp.URL == "" {
		return "", fmt.Errorf("empty stream URL returned for track %s", trackID)
	}

	return streamResp.URL, nil
}

// GetUser fetches a user profile by ID
func (c *Client) GetUser(ctx context.Context, userID string) (*SoundCloudUser, error) {
	var user SoundCloudUser
	if err := c.get(ctx, fmt.Sprintf("/users/%s", userID), nil, &user); err != nil {
		return nil, err
	}
	return &user, nil
}

// GetUserTracks fetches tracks by a user
func (c *Client) GetUserTracks(ctx context.Context, userID string, limit int) ([]*SoundCloudTrack, error) {
	if limit <= 0 {
		limit = 20
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))

	var resp struct {
		Collection []*SoundCloudTrack `json:"collection"`
	}
	if err := c.get(ctx, fmt.Sprintf("/users/%s/tracks", userID), q, &resp); err != nil {
		return nil, err
	}
	return resp.Collection, nil
}

// GetUserAlbums fetches playlists/albums by a user
func (c *Client) GetUserAlbums(ctx context.Context, userID string, limit int) ([]*SoundCloudPlaylist, error) {
	if limit <= 0 {
		limit = 20
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))

	var resp struct {
		Collection []*SoundCloudPlaylist `json:"collection"`
	}
	if err := c.get(ctx, fmt.Sprintf("/users/%s/albums", userID), q, &resp); err != nil {
		return nil, err
	}
	return resp.Collection, nil
}

// GetTracksBatch fetches full metadata for multiple track IDs in batch
func (c *Client) GetTracksBatch(ctx context.Context, trackIDs []int64) ([]SoundCloudTrack, error) {
	if len(trackIDs) == 0 {
		return nil, nil
	}

	var allTracks []SoundCloudTrack
	chunkSize := 50
	for i := 0; i < len(trackIDs); i += chunkSize {
		end := i + chunkSize
		if end > len(trackIDs) {
			end = len(trackIDs)
		}
		chunk := trackIDs[i:end]

		idStrs := make([]string, len(chunk))
		for j, id := range chunk {
			idStrs[j] = strconv.FormatInt(id, 10)
		}

		q := url.Values{}
		q.Set("ids", strings.Join(idStrs, ","))

		var batch []SoundCloudTrack
		if err := c.get(ctx, "/tracks", q, &batch); err != nil {
			return nil, fmt.Errorf("fetch batch tracks: %w", err)
		}
		allTracks = append(allTracks, batch...)
	}

	return allTracks, nil
}

// GetPlaylist fetches a playlist/album with its tracks, hydrating incomplete track stubs
func (c *Client) GetPlaylist(ctx context.Context, playlistID string) (*SoundCloudPlaylist, error) {
	var pl SoundCloudPlaylist
	if err := c.get(ctx, fmt.Sprintf("/playlists/%s", playlistID), nil, &pl); err != nil {
		return nil, err
	}

	// SoundCloud v2 only returns full metadata for the first 5 tracks in playlists.
	// Subsequent tracks are stubs ({id: ...}) missing title, user, and duration.
	var missingIDs []int64
	for _, t := range pl.Tracks {
		if t.Title == "" && t.ID > 0 {
			missingIDs = append(missingIDs, t.ID)
		}
	}

	if len(missingIDs) > 0 {
		hydrated, err := c.GetTracksBatch(ctx, missingIDs)
		if err == nil && len(hydrated) > 0 {
			trackMap := make(map[int64]SoundCloudTrack, len(hydrated))
			for _, t := range hydrated {
				trackMap[t.ID] = t
			}
			for i, t := range pl.Tracks {
				if full, ok := trackMap[t.ID]; ok {
					pl.Tracks[i] = full
				}
			}
		} else if err != nil {
			log.Printf("[SOUNDCLOUD] ⚠️ Failed to hydrate %d stub tracks in playlist %s: %v", len(missingIDs), playlistID, err)
		}
	}

	return &pl, nil
}

// GetRelatedTracks fetches similar/related tracks for recommendations
func (c *Client) GetRelatedTracks(ctx context.Context, trackID string, limit int) ([]*SoundCloudTrack, error) {
	if limit <= 0 {
		limit = 10
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))

	var resp struct {
		Collection []*SoundCloudTrack `json:"collection"`
	}
	if err := c.get(ctx, fmt.Sprintf("/tracks/%s/related", trackID), q, &resp); err != nil {
		return nil, err
	}
	return resp.Collection, nil
}

// SearchTracks searches for tracks by keyword
func (c *Client) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*SoundCloudTrack, error) {
	if limit <= 0 {
		limit = 20
	}
	q := url.Values{}
	q.Set("q", query)
	q.Set("limit", strconv.Itoa(limit))
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}

	var resp SoundCloudSearchTracksResponse
	if err := c.get(ctx, "/search/tracks", q, &resp); err != nil {
		return nil, err
	}
	return resp.Collection, nil
}

// SearchAlbums searches for albums/playlists by keyword
func (c *Client) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*SoundCloudPlaylist, error) {
	if limit <= 0 {
		limit = 20
	}
	q := url.Values{}
	q.Set("q", query)
	q.Set("limit", strconv.Itoa(limit))
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}

	var resp SoundCloudSearchAlbumsResponse
	if err := c.get(ctx, "/search/albums", q, &resp); err != nil {
		return nil, err
	}
	return resp.Collection, nil
}

// SearchUsers searches for artists/users by keyword
func (c *Client) SearchUsers(ctx context.Context, query string, limit, offset int) ([]*SoundCloudUser, error) {
	if limit <= 0 {
		limit = 20
	}
	q := url.Values{}
	q.Set("q", query)
	q.Set("limit", strconv.Itoa(limit))
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}

	var resp SoundCloudSearchUsersResponse
	if err := c.get(ctx, "/search/users", q, &resp); err != nil {
		return nil, err
	}
	return resp.Collection, nil
}

// BatchGetTracks fetches full metadata for multiple track IDs in chunks of up to 50
func (c *Client) BatchGetTracks(ctx context.Context, ids []string) ([]*SoundCloudTrack, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var allTracks []*SoundCloudTrack
	chunkSize := 50
	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]
		q := url.Values{}
		q.Set("ids", strings.Join(chunk, ","))
		var chunkTracks []*SoundCloudTrack
		if err := c.get(ctx, "/tracks", q, &chunkTracks); err != nil {
			return allTracks, err
		}
		allTracks = append(allTracks, chunkTracks...)
	}
	return allTracks, nil
}

// Resolve resolves a SoundCloud permalink URL (playlist, track, or user)
func (c *Client) Resolve(ctx context.Context, targetURL string) (json.RawMessage, error) {
	q := url.Values{}
	q.Set("url", targetURL)
	var raw json.RawMessage
	if err := c.get(ctx, "/resolve", q, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// GetUserFollowings fetches users followed by a given user
func (c *Client) GetUserFollowings(ctx context.Context, userID string, limit int) ([]*SoundCloudUser, error) {
	if limit <= 0 {
		limit = 10
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	var resp struct {
		Collection []*SoundCloudUser `json:"collection"`
	}
	if err := c.get(ctx, fmt.Sprintf("/users/%s/followings", userID), q, &resp); err != nil {
		return nil, err
	}
	return resp.Collection, nil
}
