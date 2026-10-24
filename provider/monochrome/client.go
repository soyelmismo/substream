package monochrome

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const DefaultBaseURL = "https://tracks.monochrome.st"

// Client handles HTTP calls to the Monochrome API
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// ClientOptions configures the Monochrome Client
type ClientOptions struct {
	BaseURL string
}

// NewClient creates a new Monochrome client
func NewClient(opts ...ClientOptions) *Client {
	baseURL := DefaultBaseURL
	if len(opts) > 0 && opts[0].BaseURL != "" {
		baseURL = opts[0].BaseURL
	}

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func (c *Client) get(ctx context.Context, endpoint string, target interface{}) error {
	u := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", "SubStream/1.0 (Monochrome Provider)")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("monochrome upstream %s returned %d: %s", endpoint, resp.StatusCode, string(body))
	}

	if target != nil {
		if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
			return fmt.Errorf("decode json from %s: %w", endpoint, err)
		}
	}

	return nil
}

// SearchAll searches across tracks, releases, and artists
func (c *Client) SearchAll(ctx context.Context, query string) (*SearchAllResponse, error) {
	endpoint := fmt.Sprintf("/search?q=%s", url.QueryEscape(query))
	var res SearchAllResponse
	if err := c.get(ctx, endpoint, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// SearchTracks searches for tracks with an optional limit
func (c *Client) SearchTracks(ctx context.Context, query string, limit int) ([]MonochromeTrack, error) {
	if limit <= 0 {
		limit = 20
	}
	endpoint := fmt.Sprintf("/search/tracks?q=%s&limit=%d", url.QueryEscape(query), limit)
	var res SearchTracksResponse
	if err := c.get(ctx, endpoint, &res); err != nil {
		return nil, err
	}
	return res.Tracks, nil
}

// SearchReleases searches for albums/releases with an optional limit
func (c *Client) SearchReleases(ctx context.Context, query string, limit int) ([]MonochromeRelease, error) {
	if limit <= 0 {
		limit = 20
	}
	endpoint := fmt.Sprintf("/search/releases?q=%s&limit=%d", url.QueryEscape(query), limit)
	var res SearchReleasesResponse
	if err := c.get(ctx, endpoint, &res); err != nil {
		return nil, err
	}
	return res.Releases, nil
}

// SearchArtists searches for artists with an optional limit
func (c *Client) SearchArtists(ctx context.Context, query string, limit int) ([]MonochromeArtist, error) {
	if limit <= 0 {
		limit = 20
	}
	endpoint := fmt.Sprintf("/search/artists?q=%s&limit=%d", url.QueryEscape(query), limit)
	var res SearchArtistsResponse
	if err := c.get(ctx, endpoint, &res); err != nil {
		return nil, err
	}
	return res.Artists, nil
}

// GetRelease fetches a release and its tracks by ID
func (c *Client) GetRelease(ctx context.Context, releaseID string) (*MonochromeRelease, error) {
	endpoint := fmt.Sprintf("/releases/%s", url.PathEscape(releaseID))
	var rel MonochromeRelease
	if err := c.get(ctx, endpoint, &rel); err != nil {
		return nil, err
	}
	return &rel, nil
}

// GetArtist fetches an artist profile, discography, and top tracks by ID
func (c *Client) GetArtist(ctx context.Context, artistID string) (*MonochromeArtistDetail, error) {
	endpoint := fmt.Sprintf("/artists/%s", url.PathEscape(artistID))
	var art MonochromeArtistDetail
	if err := c.get(ctx, endpoint, &art); err != nil {
		return nil, err
	}
	return &art, nil
}

// GetStreamURL returns the direct FLAC audio stream URL
func (c *Client) GetStreamURL(trackID string) string {
	return fmt.Sprintf("%s/track/%s", c.baseURL, url.PathEscape(trackID))
}

// CheckTrackStream sends a HEAD request to verify that the track is streamable
func (c *Client) CheckTrackStream(ctx context.Context, trackID string) (int64, error) {
	u := c.GetStreamURL(trackID)
	req, err := http.NewRequestWithContext(ctx, "HEAD", u, nil)
	if err != nil {
		return 0, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("track %s stream returned status %d", trackID, resp.StatusCode)
	}

	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return contentLength, nil
}
