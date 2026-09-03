package spotify

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	spotifyAPIBaseURL = "https://api.spotify.com/v1"
	defaultUserAgent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)

// Client interacts with Spotify API using automatic anonymous TOTP tokens & client tokens
type Client struct {
	httpClient    *http.Client
	mu            sync.RWMutex
	accessToken   string
	clientToken   string
	clientID      string
	deviceID      string
	clientVersion string
	cookies       map[string]string
	expiresAt     time.Time
}

// NewClient creates a new Spotify client
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 20 * time.Second},
		cookies:    make(map[string]string),
	}
}

func (c *Client) getSessionInfo(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://open.spotify.com", nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", defaultUserAgent)

	for name, value := range c.cookies {
		req.AddCookie(&http.Cookie{Name: name, Value: value})
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	re := regexp.MustCompile(`<script id="appServerConfig" type="text/plain">([^<]+)</script>`)
	matches := re.FindStringSubmatch(string(body))
	if len(matches) > 1 {
		decoded, err := base64.StdEncoding.DecodeString(matches[1])
		if err == nil {
			var cfg map[string]interface{}
			if json.Unmarshal(decoded, &cfg) == nil {
				if ver, ok := cfg["clientVersion"].(string); ok {
					c.clientVersion = ver
				}
			}
		}
	}
	if c.clientVersion == "" {
		c.clientVersion = "1.2.60.402.g81232873"
	}

	for _, cookie := range resp.Cookies() {
		if cookie.Name == "sp_t" {
			c.deviceID = cookie.Value
		}
		c.cookies[cookie.Name] = cookie.Value
	}
	return nil
}

func (c *Client) fetchClientToken(ctx context.Context) error {
	payload := map[string]interface{}{
		"client_data": map[string]interface{}{
			"client_version": c.clientVersion,
			"client_id":      c.clientID,
			"js_sdk_data": map[string]interface{}{
				"device_brand": "unknown",
				"device_model": "unknown",
				"os":           "windows",
				"os_version":   "NT 10.0",
				"device_id":    c.deviceID,
				"device_type":  "computer",
			},
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://clienttoken.spotify.com/v1/clienttoken", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Authority", "clienttoken.spotify.com")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var data struct {
			GrantedToken struct {
				Token string `json:"token"`
			} `json:"granted_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&data); err == nil && data.GrantedToken.Token != "" {
			c.clientToken = data.GrantedToken.Token
		}
	}
	return nil
}

// ensureToken ensures valid anonymous Spotify tokens are ready
func (c *Client) ensureToken(ctx context.Context) (string, string, error) {
	c.mu.RLock()
	if c.accessToken != "" && time.Now().Before(c.expiresAt.Add(-60*time.Second)) {
		t, ct := c.accessToken, c.clientToken
		c.mu.RUnlock()
		return t, ct, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.accessToken != "" && time.Now().Before(c.expiresAt.Add(-60*time.Second)) {
		return c.accessToken, c.clientToken, nil
	}

	_ = c.getSessionInfo(ctx)

	totpCode, version, err := generateSpotifyTOTP(time.Now())
	if err != nil {
		return "", "", fmt.Errorf("generate spotify totp: %w", err)
	}

	tokenURL := fmt.Sprintf("https://open.spotify.com/api/token?reason=init&productType=web-player&totp=%s&totpVer=%d&totpServer=%s",
		url.QueryEscape(totpCode), version, url.QueryEscape(totpCode))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("request anonymous spotify token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", "", fmt.Errorf("spotify token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		AccessToken              string  `json:"accessToken"`
		AccessTokenExpirationMs float64 `json:"accessTokenExpirationTimestampMs"`
		ClientID                 string  `json:"clientId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", "", fmt.Errorf("decode token response: %w", err)
	}

	if res.AccessToken == "" {
		return "", "", fmt.Errorf("empty access token in spotify response")
	}

	c.accessToken = res.AccessToken
	c.clientID = res.ClientID
	if res.AccessTokenExpirationMs > 0 {
		c.expiresAt = time.UnixMilli(int64(res.AccessTokenExpirationMs))
	} else {
		c.expiresAt = time.Now().Add(55 * time.Minute)
	}

	_ = c.fetchClientToken(ctx)

	return c.accessToken, c.clientToken, nil
}

// doRequest executes an authenticated request against the Spotify Web API
func (c *Client) doRequest(ctx context.Context, endpoint string, query url.Values, target interface{}) error {
	token, clientToken, err := c.ensureToken(ctx)
	if err != nil {
		return err
	}

	reqURL := fmt.Sprintf("%s/%s", spotifyAPIBaseURL, strings.TrimPrefix(endpoint, "/"))
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if clientToken != "" {
		req.Header.Set("Client-Token", clientToken)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("spotify API %s status %d: %s", endpoint, resp.StatusCode, string(body))
	}

	if target != nil {
		return json.NewDecoder(resp.Body).Decode(target)
	}
	return nil
}

// GetTrack retrieves track details including ISRC
func (c *Client) GetTrack(ctx context.Context, trackID string) (*SpotifyTrack, error) {
	var track SpotifyTrack
	if err := c.doRequest(ctx, "tracks/"+trackID, nil, &track); err != nil {
		return nil, err
	}
	return &track, nil
}

// GetAlbum retrieves album details
func (c *Client) GetAlbum(ctx context.Context, albumID string) (*SpotifyAlbum, error) {
	var album SpotifyAlbum
	if err := c.doRequest(ctx, "albums/"+albumID, nil, &album); err != nil {
		return nil, err
	}
	return &album, nil
}

// GetArtist retrieves artist details
func (c *Client) GetArtist(ctx context.Context, artistID string) (*SpotifyArtist, error) {
	var artist SpotifyArtist
	if err := c.doRequest(ctx, "artists/"+artistID, nil, &artist); err != nil {
		return nil, err
	}
	return &artist, nil
}

// Search searches across tracks, albums, and artists
func (c *Client) Search(ctx context.Context, query string, limit, offset int) (*SpotifySearchResponse, error) {
	params := url.Values{
		"q":      {query},
		"type":   {"track,album,artist"},
		"limit":  {strconv.Itoa(limit)},
		"offset": {strconv.Itoa(offset)},
	}
	var res SpotifySearchResponse
	if err := c.doRequest(ctx, "search", params, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// SearchByISRC looks up a Spotify track by ISRC
func (c *Client) SearchByISRC(ctx context.Context, isrc string) (*SpotifyTrack, error) {
	isrc = strings.TrimSpace(isrc)
	if isrc == "" {
		return nil, fmt.Errorf("empty isrc")
	}
	params := url.Values{
		"q":     {"isrc:" + isrc},
		"type":  {"track"},
		"limit": {"1"},
	}
	var res SpotifySearchResponse
	if err := c.doRequest(ctx, "search", params, &res); err != nil {
		return nil, err
	}
	if len(res.Tracks.Items) == 0 {
		return nil, fmt.Errorf("no spotify track for isrc %s", isrc)
	}
	return &res.Tracks.Items[0], nil
}
