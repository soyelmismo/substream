package qobuz

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
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
	pool          *AccountPool
	mu            sync.RWMutex
}

// ClientOptions configures a Qobuz Client
type ClientOptions struct {
	BaseURL       string
	AppID         string
	AppSecret     string
	UserAuthToken string
	CustomURL     string
	DB            *db.DB
	Timeout       time.Duration
}

var (
	testAPIBaseURLMu sync.RWMutex
	testAPIBaseURL   string
)

// SetAPIBaseURLForTest allows overriding the Qobuz API base URL during unit tests.
func SetAPIBaseURLForTest(u string) func() {
	testAPIBaseURLMu.Lock()
	orig := testAPIBaseURL
	testAPIBaseURL = u
	testAPIBaseURLMu.Unlock()
	return func() {
		testAPIBaseURLMu.Lock()
		testAPIBaseURL = orig
		testAPIBaseURLMu.Unlock()
	}
}

// NewClient creates a new Qobuz API client
func NewClient(opts ClientOptions) *Client {
	baseURL := opts.BaseURL
	if baseURL == "" {
		testAPIBaseURLMu.RLock()
		if testAPIBaseURL != "" {
			baseURL = testAPIBaseURL
		} else {
			baseURL = defaultQobuzAPIBaseURL
		}
		testAPIBaseURLMu.RUnlock()
	}
	appID := opts.AppID
	if appID == "" {
		appID = os.Getenv("SUBSTREAM_QOBUZ_APP_ID")
	}
	if appID == "" {
		appID = os.Getenv("QOBUZ_APP_ID")
	}
	if appID == "" {
		appID = defaultQobuzAppID
	}
	appSecret := opts.AppSecret
	if appSecret == "" {
		appSecret = os.Getenv("SUBSTREAM_QOBUZ_APP_SECRET")
	}
	if appSecret == "" {
		appSecret = os.Getenv("QOBUZ_APP_SECRET")
	}
	if appSecret == "" {
		appSecret = defaultQobuzAppSecret
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	pool := NewAccountPool(opts.DB)
	if opts.DB != nil {
		if accs, err := opts.DB.GetActiveQobuzAccounts(); err == nil && len(accs) > 0 {
			pool.SyncAccounts(accs)
		}
	}

	token := strings.TrimSpace(opts.UserAuthToken)
	if token != "" && pool.Count() == 0 {
		pool.SyncAccounts([]*db.QobuzAccount{
			{
				ID:               1,
				UserAuthToken:    token,
				Username:         "Default Token",
				SubscriptionType: "STUDIO",
				HighestQuality:   "24B192K",
				QualityTier:      90,
				IsActive:         true,
			},
		})
	}

	return &Client{
		httpClient:    &http.Client{Timeout: timeout},
		baseURL:       strings.TrimRight(baseURL, "/"),
		appID:         appID,
		appSecret:     appSecret,
		userAuthToken: token,
		customURL:     strings.TrimRight(strings.TrimSpace(opts.CustomURL), "/"),
		pool:          pool,
	}
}

// Pool returns the underlying AccountPool
func (c *Client) Pool() *AccountPool {
	return c.pool
}

// AppID returns the configured application ID
func (c *Client) AppID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.appID
}

// AppSecret returns the configured application secret
func (c *Client) AppSecret() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.appSecret
}

// SyncAccounts synchronizes active accounts with the client pool
func (c *Client) SyncAccounts(accounts []*db.QobuzAccount) {
	if c.pool != nil {
		c.pool.SyncAccounts(accounts)
	}
}

// SetCredentials updates the application ID and application secret dynamically
func (c *Client) SetCredentials(appID, appSecret string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if appID != "" {
		c.appID = appID
	}
	if appSecret != "" {
		c.appSecret = appSecret
	}
}

// SetDB updates the database reference on the pool
func (c *Client) SetDB(dbc *db.DB) {
	if c.pool != nil {
		c.pool.SetDB(dbc)
	}
}

// qobuzRequestSignature computes the MD5 request signature expected by Qobuz API
func (c *Client) qobuzRequestSignature(endpoint string, params url.Values, ts string) string {
	c.mu.RLock()
	secret := c.appSecret
	c.mu.RUnlock()

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
	sb.WriteString(secret)

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

	c.mu.RLock()
	appID := c.appID
	var token string
	if c.pool != nil && c.pool.Count() > 0 {
		if session, err := c.pool.GetNextAccount(db.QualityTierFree); err == nil && session != nil && session.Account != nil {
			token = session.Account.UserAuthToken
		}
	}
	if token == "" {
		token = c.userAuthToken
	}
	c.mu.RUnlock()

	params.Set("app_id", appID)
	params.Set("request_ts", ts)
	params.Set("request_sig", sig)
	if token != "" {
		params.Set("user_auth_token", token)
	}

	reqURL := fmt.Sprintf("%s/%s?%s", c.baseURL, strings.TrimPrefix(endpoint, "/"), params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-App-Id", appID)
	if token != "" {
		req.Header.Set("X-User-Auth-Token", token)
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
	for i := range tracks {
		if strings.EqualFold(tracks[i].ISRC, isrc) {
			return &tracks[i], nil
		}
	}
	if len(tracks) > 0 {
		return &tracks[0], nil
	}
	return nil, fmt.Errorf("no track found for ISRC %s", isrc)
}

// GetStreamURL resolves a streaming URL for a track using multi-account pool rotation.
// formatID: 5 = MP3 320, 6 = FLAC 16-bit 44.1kHz, 7 = FLAC 24-bit <= 96kHz, 27 = FLAC 24-bit > 96kHz
func (c *Client) GetStreamURL(ctx context.Context, trackID int64, quality string) (string, error) {
	reqQ := strings.ToUpper(strings.TrimSpace(quality))
	minTier := db.QualityTierLossless
	if reqQ == "LOW" || reqQ == "MP3" {
		minTier = db.QualityTierFree
	} else if reqQ == "HI_RES" || reqQ == "HI_RES_LOSSLESS" || reqQ == "24" {
		minTier = 85
	}

	formatID := "6"
	switch reqQ {
	case "LOW", "MP3":
		formatID = "5"
	case "LOSSLESS", "16":
		formatID = "6"
	case "HI_RES", "24", "HI_RES_LOSSLESS":
		formatID = "27"
	}

	// 1. Resolve via multi-account pool with round-robin
	if c.pool != nil && c.pool.Count() > 0 {
		totalAccounts := c.pool.Count()
		var lastErr error
		for attempt := 0; attempt < totalAccounts; attempt++ {
			session, err := c.pool.GetNextAccount(minTier)
			if err != nil {
				lastErr = err
				break
			}

			streamURL, err := c.fetchStreamURLWithToken(ctx, trackID, formatID, session.Account.UserAuthToken)
			if err == nil && streamURL != "" {
				log.Printf("[QOBUZ] 🚀 Resolved stream for track %d via account %s (format: %s)", trackID, session.Account.Username, formatID)
				return streamURL, nil
			}

			if ctx.Err() != nil {
				return "", ctx.Err()
			}

			c.pool.MarkError(session.Account.ID, err)
			lastErr = err
		}

		if lastErr != nil {
			log.Printf("[QOBUZ] ⚠️ Account pool failed for track %d: %v", trackID, lastErr)
		}
	} else if c.userAuthToken != "" {
		// Single legacy token fallback
		if streamURL, err := c.fetchStreamURLWithToken(ctx, trackID, formatID, c.userAuthToken); err == nil && streamURL != "" {
			return streamURL, nil
		}
	}

	// 2. Custom mirror fallback
	if c.customURL != "" {
		customStream, err := c.getCustomStreamURL(ctx, trackID, quality)
		if err == nil && customStream != "" {
			return customStream, nil
		}
	}

	return "", fmt.Errorf("no stream available for Qobuz track %d (configure active subscriber account or token)", trackID)
}

func (c *Client) fetchStreamURLWithToken(ctx context.Context, trackID int64, formatID string, token string) (string, error) {
	params := url.Values{
		"track_id":  {strconv.FormatInt(trackID, 10)},
		"format_id": {formatID},
		"intent":    {"stream"},
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sig := c.qobuzRequestSignature("track/getFileUrl", params, ts)

	c.mu.RLock()
	appID := c.appID
	c.mu.RUnlock()

	params.Set("app_id", appID)
	params.Set("user_auth_token", token)
	params.Set("request_ts", ts)
	params.Set("request_sig", sig)

	reqURL := fmt.Sprintf("%s/track/getFileUrl?%s", c.baseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-App-Id", appID)
	req.Header.Set("X-User-Auth-Token", token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("qobuz API track/getFileUrl status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var res QobuzFileUrlResponse
	if err := json.Unmarshal(bodyBytes, &res); err != nil {
		return "", fmt.Errorf("decode getFileUrl response: %w", err)
	}

	if res.URL != "" {
		if strings.Contains(res.URL, "range=20-30") || strings.Contains(res.URL, "range=0-30") || strings.Contains(res.URL, "preview") {
			return "", fmt.Errorf("qobuz track requires subscription (preview only)")
		}
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
