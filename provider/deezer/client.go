package deezer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDeezerAPIBaseURL = "https://api.deezer.com"
	defaultUserAgent        = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

// Client interacts with the Deezer API
type Client struct {
	httpClient *http.Client
	baseURL    string
	customURL  string // Optional custom Deezer proxy/mirror
	arl        string // Optional Deezer ARL cookie
}

// ClientOptions configures Deezer client
type ClientOptions struct {
	BaseURL   string
	CustomURL string
	ARL       string
	Timeout   time.Duration
}

// NewClient creates a new Deezer client
func NewClient(opts ClientOptions) *Client {
	baseURL := opts.BaseURL
	if baseURL == "" {
		baseURL = defaultDeezerAPIBaseURL
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	return &Client{
		httpClient: &http.Client{Timeout: timeout},
		baseURL:    strings.TrimRight(baseURL, "/"),
		customURL:  strings.TrimRight(strings.TrimSpace(opts.CustomURL), "/"),
		arl:        strings.TrimSpace(opts.ARL),
	}
}

func (c *Client) get(ctx context.Context, endpoint string, query url.Values, target interface{}) error {
	u := fmt.Sprintf("%s/%s", c.baseURL, strings.TrimPrefix(endpoint, "/"))
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deezer API %s returned status %d: %s", endpoint, resp.StatusCode, string(body))
	}

	// Check if Deezer returned an API error inside 200 OK: {"error": {"type": "...", "message": "..."}}
	var errCheck struct {
		Error *struct {
			Type    string `json:"type"`
			Message string `json:"message"`
			Code    int    `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &errCheck); err == nil && errCheck.Error != nil {
		return fmt.Errorf("deezer API error: %s (code %d)", errCheck.Error.Message, errCheck.Error.Code)
	}

	if target != nil {
		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

// GetTrack fetches track details by ID
func (c *Client) GetTrack(ctx context.Context, trackID int64) (*DeezerTrack, error) {
	var track DeezerTrack
	if err := c.get(ctx, fmt.Sprintf("track/%d", trackID), nil, &track); err != nil {
		return nil, err
	}
	return &track, nil
}

// GetAlbum fetches album details by ID
func (c *Client) GetAlbum(ctx context.Context, albumID int64) (*DeezerAlbum, error) {
	var album DeezerAlbum
	if err := c.get(ctx, fmt.Sprintf("album/%d", albumID), nil, &album); err != nil {
		return nil, err
	}
	return &album, nil
}

// GetArtist fetches artist details by ID
func (c *Client) GetArtist(ctx context.Context, artistID int64) (*DeezerArtist, error) {
	var artist DeezerArtist
	if err := c.get(ctx, fmt.Sprintf("artist/%d", artistID), nil, &artist); err != nil {
		return nil, err
	}
	return &artist, nil
}

// GetArtistAlbums fetches albums for an artist
func (c *Client) GetArtistAlbums(ctx context.Context, artistID int64, limit int) ([]DeezerAlbum, error) {
	var res DeezerSearchAlbumsResponse
	params := url.Values{"limit": {strconv.Itoa(limit)}}
	if err := c.get(ctx, fmt.Sprintf("artist/%d/albums", artistID), params, &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

// GetArtistTopTracks fetches top tracks for an artist
func (c *Client) GetArtistTopTracks(ctx context.Context, artistID int64, limit int) ([]DeezerTrack, error) {
	var res DeezerSearchTracksResponse
	params := url.Values{"limit": {strconv.Itoa(limit)}}
	if err := c.get(ctx, fmt.Sprintf("artist/%d/top", artistID), params, &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

// SearchTracks searches tracks
func (c *Client) SearchTracks(ctx context.Context, query string, limit, offset int) ([]DeezerTrack, error) {
	var res DeezerSearchTracksResponse
	params := url.Values{
		"q":     {query},
		"limit": {strconv.Itoa(limit)},
		"index": {strconv.Itoa(offset)},
	}
	if err := c.get(ctx, "search/track", params, &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

// SearchAlbums searches albums
func (c *Client) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]DeezerAlbum, error) {
	var res DeezerSearchAlbumsResponse
	params := url.Values{
		"q":     {query},
		"limit": {strconv.Itoa(limit)},
		"index": {strconv.Itoa(offset)},
	}
	if err := c.get(ctx, "search/album", params, &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

// SearchArtists searches artists
func (c *Client) SearchArtists(ctx context.Context, query string, limit, offset int) ([]DeezerArtist, error) {
	var res DeezerSearchArtistsResponse
	params := url.Values{
		"q":     {query},
		"limit": {strconv.Itoa(limit)},
		"index": {strconv.Itoa(offset)},
	}
	if err := c.get(ctx, "search/artist", params, &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

// SearchByISRC looks up a track by ISRC
func (c *Client) SearchByISRC(ctx context.Context, isrc string) (*DeezerTrack, error) {
	isrc = strings.TrimSpace(isrc)
	if isrc == "" {
		return nil, fmt.Errorf("empty ISRC")
	}
	var track DeezerTrack
	if err := c.get(ctx, fmt.Sprintf("track/isrc:%s", url.PathEscape(isrc)), nil, &track); err != nil {
		return nil, err
	}
	if track.ID == 0 {
		return nil, fmt.Errorf("track not found for ISRC %s", isrc)
	}
	return &track, nil
}

// GetStreamURL returns the audio stream URL for a track
func (c *Client) GetStreamURL(ctx context.Context, track *DeezerTrack, quality string) (string, error) {
	if track == nil {
		return "", fmt.Errorf("nil track")
	}

	// 1. If custom mirror is configured, query custom instance (e.g. dzr instance)
	if c.customURL != "" {
		format := "FLAC"
		if strings.ToUpper(quality) == "LOW" || strings.ToUpper(quality) == "HIGH" {
			format = "MP3_320"
		}
		customStream, err := c.getCustomStreamURL(ctx, track, format)
		if err == nil && customStream != "" {
			return customStream, nil
		}
	}

	return "", fmt.Errorf("no full stream available for Deezer track %d (requires active mirror/subscription)", track.ID)
}

func (c *Client) getCustomStreamURL(ctx context.Context, track *DeezerTrack, format string) (string, error) {
	// Query custom instance by ISRC or track ID
	u := fmt.Sprintf("%s/stream/?id=%d&format=%s", c.customURL, track.ID, url.QueryEscape(format))
	if track.ISRC != "" {
		u = fmt.Sprintf("%s/stream/?isrc=%s&format=%s", c.customURL, url.QueryEscape(track.ISRC), url.QueryEscape(format))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusFound {
		if loc := resp.Header.Get("Location"); loc != "" {
			return loc, nil
		}
		var parsed struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err == nil && parsed.URL != "" {
			return parsed.URL, nil
		}
		return u, nil
	}

	return "", fmt.Errorf("custom Deezer instance returned %d", resp.StatusCode)
}
