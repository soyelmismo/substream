package amazon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

// Client interacts with Amazon Music endpoints
type Client struct {
	httpClient *http.Client
	customURL  string // Configured Amazon Music instance URL
}

// ClientOptions configures Amazon Music client
type ClientOptions struct {
	CustomURL string
	Timeout   time.Duration
}

// NewClient creates a new Amazon Music client
func NewClient(opts ClientOptions) *Client {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		httpClient: &http.Client{Timeout: timeout},
		customURL:  strings.TrimRight(strings.TrimSpace(opts.CustomURL), "/"),
	}
}

// GetTrack retrieves track details for an ASIN
func (c *Client) GetTrack(ctx context.Context, asin string) (*AmazonTrack, error) {
	if c.customURL == "" {
		// Return placeholder struct for ASIN if no mirror configured
		return &AmazonTrack{
			ASIN:  asin,
			Title: "Amazon Track " + asin,
		}, nil
	}

	endpoint := fmt.Sprintf("%s/api/track?asin=%s", c.customURL, url.QueryEscape(asin))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("amazon proxy returned %d", resp.StatusCode)
	}

	var track AmazonTrack
	if err := json.NewDecoder(resp.Body).Decode(&track); err != nil {
		return nil, err
	}
	return &track, nil
}

// GetStreamURL retrieves a stream URL from the configured Amazon proxy
func (c *Client) GetStreamURL(ctx context.Context, asin string, quality string) (string, error) {
	if c.customURL == "" {
		return "", fmt.Errorf("no Amazon Music instance configured (use --amazon-url)")
	}

	payload, err := json.Marshal(map[string]string{
		"id":      asin,
		"quality": quality,
		"country": "US",
	})
	if err != nil {
		return "", err
	}

	endpoint := fmt.Sprintf("%s/api/dl", c.customURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("amazon proxy returned %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		StreamURL string `json:"stream_url"`
		URL       string `json:"url"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return "", err
	}

	stream := res.StreamURL
	if stream == "" {
		stream = res.URL
	}
	if stream == "" {
		return "", fmt.Errorf("empty stream url from amazon proxy")
	}

	return stream, nil
}
