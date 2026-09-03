package qobuz

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultQobuzAPIBaseURL   = "https://www.qobuz.com/api.json/0.2"
	defaultQobuzAppID        = "712109809"
	defaultQobuzAppSecret    = "589be88e4538daea11f509d29e4a23b1"
	defaultUserAgent         = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

// Client interacts with Qobuz API
type Client struct {
	httpClient    *http.Client
	baseURL       string
	appID         string
	appSecret     string
	userAuthToken string
	customURL     string
}

// ClientOptions configures a Qobuz Client
type ClientOptions struct {
	BaseURL       string
	AppID         string
	AppSecret     string
	UserAuthToken string
	CustomURL     string
	Timeout       time.Duration
}

// NewClient creates a new Qobuz API client
func NewClient(opts ClientOptions) *Client {
	baseURL := opts.BaseURL
	if baseURL == "" {
		baseURL = defaultQobuzAPIBaseURL
	}
	appID := opts.AppID
	if appID == "" {
		appID = defaultQobuzAppID
	}
	appSecret := opts.AppSecret
	if appSecret == "" {
		appSecret = defaultQobuzAppSecret
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &Client{
		httpClient:    &http.Client{Timeout: timeout},
		baseURL:       strings.TrimRight(baseURL, "/"),
		appID:         appID,
		appSecret:     appSecret,
		userAuthToken: strings.TrimSpace(opts.UserAuthToken),
		customURL:     strings.TrimRight(strings.TrimSpace(opts.CustomURL), "/"),
	}
}

// qobuzRequestSignature computes the MD5 request signature expected by Qobuz API
func (c *Client) qobuzRequestSignature(endpoint string, params url.Values, ts string) string {
	normalizedPath := strings.ReplaceAll(strings.TrimPrefix(endpoint, "/"), "/", "")
	var keys []string
	for k := range params {
		if k != "app_id" && k != "user_auth_token" && k != "request_ts" && k != "request_sig" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(normalizedPath)
	for _, k := range keys {
		for _, v := range params[k] {
			sb.WriteString(k)
			sb.WriteString(v)
		}
	}
	sb.WriteString(ts)
	sb.WriteString(c.appSecret)

	hash := md5.Sum([]byte(sb.String()))
	return hex.EncodeToString(hash[:])
}

// doSignedRequest executes a signed GET request to Qobuz API
func (c *Client) doSignedRequest(ctx context.Context, endpoint string, params url.Values, target interface{}) error {
	if params == nil {
		params = url.Values{}
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sig := c.qobuzRequestSignature(endpoint, params, ts)

	params.Set("app_id", c.appID)
	params.Set("request_ts", ts)
	params.Set("request_sig", sig)
	if c.userAuthToken != "" {
		params.Set("user_auth_token", c.userAuthToken)
	}

	reqURL := fmt.Sprintf("%s/%s?%s", c.baseURL, strings.TrimPrefix(endpoint, "/"), params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-App-Id", c.appID)
	if c.userAuthToken != "" {
		req.Header.Set("X-User-Auth-Token", c.userAuthToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("qobuz API %s status %d: %s", endpoint, resp.StatusCode, string(body))
	}

	if target != nil {
		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}

	return nil
}

// GetTrack retrieves track details by ID
func (c *Client) GetTrack(ctx context.Context, trackID int64) (*QobuzTrack, error) {
	var track QobuzTrack
	params := url.Values{"track_id": {strconv.FormatInt(trackID, 10)}}
	if err := c.doSignedRequest(ctx, "track/get", params, &track); err != nil {
		return nil, err
	}
	return &track, nil
}

// GetAlbum retrieves album details by ID
func (c *Client) GetAlbum(ctx context.Context, albumID string) (*QobuzAlbum, error) {
	var album QobuzAlbum
	params := url.Values{"album_id": {albumID}}
	if err := c.doSignedRequest(ctx, "album/get", params, &album); err != nil {
		return nil, err
	}
	return &album, nil
}

// GetArtist retrieves artist details by ID
func (c *Client) GetArtist(ctx context.Context, artistID int64, extra string) (*QobuzArtist, error) {
	var artist QobuzArtist
	params := url.Values{"artist_id": {strconv.FormatInt(artistID, 10)}}
	if extra != "" {
		params.Set("extra", extra) // e.g. "albums"
	}
	if err := c.doSignedRequest(ctx, "artist/get", params, &artist); err != nil {
		return nil, err
	}
	return &artist, nil
}

// SearchTracks searches for tracks by query
func (c *Client) SearchTracks(ctx context.Context, query string, limit, offset int) ([]QobuzTrack, error) {
	var res QobuzSearchTracksResponse
	params := url.Values{
		"query":  {query},
		"limit":  {strconv.Itoa(limit)},
		"offset": {strconv.Itoa(offset)},
	}
	if err := c.doSignedRequest(ctx, "track/search", params, &res); err != nil {
		return nil, err
	}
	return res.Tracks.Items, nil
}

// SearchAlbums searches for albums by query
func (c *Client) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]QobuzAlbum, error) {
	var res QobuzSearchAlbumsResponse
	params := url.Values{
		"query":  {query},
		"limit":  {strconv.Itoa(limit)},
		"offset": {strconv.Itoa(offset)},
	}
	if err := c.doSignedRequest(ctx, "album/search", params, &res); err != nil {
		return nil, err
	}
	return res.Albums.Items, nil
}

// SearchArtists searches for artists by query
func (c *Client) SearchArtists(ctx context.Context, query string, limit, offset int) ([]QobuzArtist, error) {
	var res QobuzSearchArtistsResponse
	params := url.Values{
		"query":  {query},
		"limit":  {strconv.Itoa(limit)},
		"offset": {strconv.Itoa(offset)},
	}
	if err := c.doSignedRequest(ctx, "artist/search", params, &res); err != nil {
		return nil, err
	}
	return res.Artists.Items, nil
}

// SearchByISRC looks up a track on Qobuz by its ISRC
func (c *Client) SearchByISRC(ctx context.Context, isrc string) (*QobuzTrack, error) {
	isrc = strings.TrimSpace(isrc)
	if isrc == "" {
		return nil, fmt.Errorf("empty ISRC")
	}
	tracks, err := c.SearchTracks(ctx, isrc, 5, 0)
	if err != nil {
		return nil, err
	}
	for _, t := range tracks {
		if strings.EqualFold(t.ISRC, isrc) {
			return &t, nil
		}
	}
	if len(tracks) > 0 {
		return &tracks[0], nil
	}
	return nil, fmt.Errorf("no track found for ISRC %s", isrc)
}

// GetStreamURL resolves a streaming URL for a track.
// formatID: 5 = MP3 320, 6 = FLAC 16-bit 44.1kHz, 7 = FLAC 24-bit <= 96kHz, 27 = FLAC 24-bit > 96kHz
func (c *Client) GetStreamURL(ctx context.Context, trackID int64, quality string) (string, error) {
	// 1. If custom mirror is configured, try it first
	if c.customURL != "" {
		customStream, err := c.getCustomStreamURL(ctx, trackID, quality)
		if err == nil && customStream != "" {
			return customStream, nil
		}
	}

	// 2. Map quality to Qobuz format_id
	formatID := "6" // Default Lossless CD quality (16-bit / 44.1kHz)
	switch strings.ToUpper(strings.TrimSpace(quality)) {
	case "LOW", "MP3":
		formatID = "5"
	case "LOSSLESS", "16":
		formatID = "6"
	case "HI_RES", "24", "HI_RES_LOSSLESS":
		formatID = "27"
	}

	params := url.Values{
		"track_id":  {strconv.FormatInt(trackID, 10)},
		"format_id": {formatID},
		"intent":    {"stream"},
	}

	var res QobuzFileUrlResponse
	if err := c.doSignedRequest(ctx, "track/getFileUrl", params, &res); err != nil {
		return "", err
	}

	if res.URL != "" {
		return res.URL, nil
	}

	if len(res.Restrictions) > 0 {
		var codes []string
		for _, r := range res.Restrictions {
			codes = append(codes, r.Code)
		}
		return "", fmt.Errorf("qobuz stream restricted: %s", strings.Join(codes, ", "))
	}

	return "", fmt.Errorf("qobuz returned empty stream url for track %d", trackID)
}

// getCustomStreamURL queries a custom Qobuz-DL / proxy instance
func (c *Client) getCustomStreamURL(ctx context.Context, trackID int64, quality string) (string, error) {
	endpoint := fmt.Sprintf("%s/api/download-music?track_id=%d&quality=%s", c.customURL, trackID, url.QueryEscape(quality))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var parsed struct {
		Success bool `json:"success"`
		Data    struct {
			URL string `json:"url"`
		} `json:"data"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", err
	}
	if !parsed.Success || strings.TrimSpace(parsed.Data.URL) == "" {
		if parsed.Error != "" {
			return "", fmt.Errorf("custom qobuz instance: %s", parsed.Error)
		}
		return "", fmt.Errorf("custom qobuz instance failed")
	}
	return parsed.Data.URL, nil
}
