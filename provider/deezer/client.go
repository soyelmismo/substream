package deezer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
)

const (
	defaultDeezerAPIBaseURL = "https://api.deezer.com"
	defaultUserAgent        = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

// Client interacts with the Deezer API
type Client struct {
	httpClient      *http.Client
	baseURL         string
	customURL       string // Optional custom Deezer proxy/mirror
	arl             string // Optional Deezer ARL cookie
	streamFilter    *StreamFilterProfile
	cacheDir        string
	pool            *AccountPool
	downloadLocks   sync.Map
	mirrorHealthy   bool
	lastHealthCheck time.Time
	mu              sync.RWMutex
}

// ClientOptions configures Deezer client
type ClientOptions struct {
	BaseURL      string
	CustomURL    string
	ARL          string
	StreamFilter string // Optional generic stream filter activation token for direct stream decryption
	SecretKey    string // Deprecated alias for StreamFilter
	CacheDir     string
	DB           *db.DB
	Timeout      time.Duration
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

	cacheDir := opts.CacheDir
	if cacheDir == "" {
		cacheDir = "cache/deezer"
	}
	_ = os.MkdirAll(cacheDir, 0755)

	pool := NewAccountPool(opts.DB)
	if opts.DB != nil {
		if accs, err := opts.DB.GetActiveDeezerAccounts(); err == nil && len(accs) > 0 {
			pool.SyncAccounts(accs)
		}
	}

	arl := strings.TrimSpace(opts.ARL)
	if arl != "" && pool.Count() == 0 {
		pool.SyncAccounts([]*db.DeezerAccount{
			{
				ID:               1,
				ARL:              arl,
				Username:         "Default ARL",
				SubscriptionType: "PREMIUM",
				HighestQuality:   "FLAC",
				QualityTier:      db.QualityTierLossless,
				IsActive:         true,
			},
		})
	}

	spec := ResolveStreamFilterSpec(opts.StreamFilter)
	if spec == "" && opts.SecretKey != "" {
		spec = ResolveStreamFilterSpec(opts.SecretKey)
	}
	var streamFilter *StreamFilterProfile
	if spec != "" {
		p, err := ParseStreamFilterSpec(spec)
		if err != nil {
			log.Printf("[DEEZER] ⚠️ Failed to parse stream filter spec: %v", err)
		} else {
			streamFilter = p
		}
	}

	return &Client{
		httpClient:    &http.Client{Timeout: timeout},
		baseURL:       strings.TrimRight(baseURL, "/"),
		customURL:     strings.TrimRight(strings.TrimSpace(opts.CustomURL), "/"),
		arl:           arl,
		streamFilter:  streamFilter,
		cacheDir:      cacheDir,
		pool:          pool,
		mirrorHealthy: true, // Optimistically true until first check
	}
}

// SyncAccounts synchronizes active Deezer accounts with the pool
func (c *Client) SyncAccounts(accounts []*db.DeezerAccount) {
	if c.pool != nil {
		c.pool.SyncAccounts(accounts)
	}
}

// SetDB updates the database reference on the pool
func (c *Client) SetDB(dbc *db.DB) {
	if c.pool != nil {
		c.pool.SetDB(dbc)
	}
}

// SetCacheDir sets the local directory for caching decrypted streams
func (c *Client) SetCacheDir(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cacheDir = dir
	_ = os.MkdirAll(dir, 0755)
}

// Pool returns the underlying AccountPool
func (c *Client) Pool() *AccountPool {
	return c.pool
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

// isMirrorHealthy checks if the custom mirror is alive and has available accounts.
// Caches results for 5 minutes to avoid polling latency.
func (c *Client) isMirrorHealthy(ctx context.Context) bool {
	if c.customURL == "" {
		return false
	}

	c.mu.RLock()
	if time.Since(c.lastHealthCheck) < 5*time.Minute {
		healthy := c.mirrorHealthy
		c.mu.RUnlock()
		return healthy
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double check after acquiring write lock
	if time.Since(c.lastHealthCheck) < 5*time.Minute {
		return c.mirrorHealthy
	}

	c.lastHealthCheck = time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.customURL, nil)
	if err != nil {
		c.mirrorHealthy = false
		return false
	}
	req.Header.Set("User-Agent", defaultUserAgent)

	// Quick check with 3s timeout
	checkClient := &http.Client{Timeout: 3 * time.Second}
	resp, err := checkClient.Do(req)
	if err != nil {
		log.Printf("[DEEZER] ⚠️ Mirror %s health check failed (unreachable): %v", c.customURL, err)
		c.mirrorHealthy = false
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[DEEZER] ⚠️ Mirror %s returned HTTP %d on health check", c.customURL, resp.StatusCode)
		c.mirrorHealthy = false
		return false
	}

	var status struct {
		OK       *bool `json:"ok"`
		Accounts struct {
			Available int `json:"available"`
			Total     int `json:"total"`
			Dead      int `json:"dead"`
		} `json:"accounts"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.mirrorHealthy = false
		return false
	}

	if err := json.Unmarshal(body, &status); err == nil {
		// If JSON has accounts structure (like dzr / deezflow instances)
		if status.Accounts.Total > 0 && status.Accounts.Available == 0 {
			log.Printf("[DEEZER] ⚠️ Mirror %s has 0 available accounts (dead: %d). Circuit breaker open: skipping calls in 0ms.",
				c.customURL, status.Accounts.Dead)
			c.mirrorHealthy = false
			return false
		}
		if status.OK != nil && !*status.OK {
			log.Printf("[DEEZER] ⚠️ Mirror %s reported ok=false. Circuit breaker open: skipping calls in 0ms.", c.customURL)
			c.mirrorHealthy = false
			return false
		}
	}

	c.mirrorHealthy = true
	return true
}

// GetStreamURL returns the audio stream URL for a track.
// When Deezer accounts are configured via ARL, it resolves, decrypts, and caches
// lossless FLAC audio files locally, returning the absolute file path.
// If local caching is bypassed or accounts are unavailable, it falls back to custom mirror if configured.
func (c *Client) GetStreamURL(ctx context.Context, track *DeezerTrack, quality string) (string, error) {
	if track == nil || track.ID <= 0 {
		return "", fmt.Errorf("invalid Deezer track")
	}

	trackIDStr := strconv.FormatInt(track.ID, 10)
	c.mu.RLock()
	cacheDir := c.cacheDir
	c.mu.RUnlock()

	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "substream-deezer")
		_ = os.MkdirAll(cacheDir, 0755)
	}

	reqQuality := strings.ToUpper(strings.TrimSpace(quality))
	flacFile := filepath.Join(cacheDir, fmt.Sprintf("%d.flac", track.ID))
	mp3File := filepath.Join(cacheDir, fmt.Sprintf("%d.mp3", track.ID))

	// 1. Fast Cache Check:
	// If lossless FLAC already exists, serve immediately
	if fi, err := os.Stat(flacFile); err == nil && fi.Size() > 1024 {
		if absPath, absErr := filepath.Abs(flacFile); absErr == nil {
			return absPath, nil
		}
		return flacFile, nil
	}
	// For lossy requests or when no lossless account is available, serve existing MP3
	if reqQuality == "LOW" || reqQuality == "HIGH" || (c.pool != nil && !c.pool.HasLosslessAccount()) {
		if fi, err := os.Stat(mp3File); err == nil && fi.Size() > 1024 {
			if absPath, absErr := filepath.Abs(mp3File); absErr == nil {
				return absPath, nil
			}
			return mp3File, nil
		}
	}

	// 2. Direct Deezer ARL Streaming with Account Rotation
	if c.pool != nil && c.pool.Count() > 0 {
		// Serialize concurrent downloads for the same track
		lockVal, _ := c.downloadLocks.LoadOrStore(track.ID, &sync.Mutex{})
		mu := lockVal.(*sync.Mutex)
		mu.Lock()
		defer func() {
			mu.Unlock()
			c.downloadLocks.Delete(track.ID)
		}()

		// Re-check cache under lock in case another worker just downloaded it
		if fi, err := os.Stat(flacFile); err == nil && fi.Size() > 1024 {
			if absPath, absErr := filepath.Abs(flacFile); absErr == nil {
				return absPath, nil
			}
			return flacFile, nil
		}
		if reqQuality == "LOW" || reqQuality == "HIGH" || !c.pool.HasLosslessAccount() {
			if fi, err := os.Stat(mp3File); err == nil && fi.Size() > 1024 {
				if absPath, absErr := filepath.Abs(mp3File); absErr == nil {
					return absPath, nil
				}
				return mp3File, nil
			}
		}

		minTier := db.QualityTierLossless
		if reqQuality == "HIGH" {
			minTier = db.QualityTierHigh
		} else if reqQuality == "LOW" {
			minTier = db.QualityTierFree
		}

		totalAccounts := c.pool.Count()
		var lastErr error
		filter := c.StreamFilter()

		if (filter == nil || filter.ChunkSize == 0 || filter.Stride == 0) && totalAccounts > 0 {
			log.Printf("[DEEZER] ⚠️ Native stream decryption requires active stream filter engine (set DEEZER_STREAM_FILTER). Falling back to mirror/proxy.")
		} else {
			for attempt := 0; attempt < totalAccounts; attempt++ {
				session, err := c.pool.GetNextAccount(minTier)
				if err != nil {
					lastErr = err
					break
				}

				// Step A: Get track token via song.getData
				tokenRes, err := session.GetTrackToken(ctx, track.ID)
				if err != nil {
					c.pool.MarkError(session.Account.ID, fmt.Errorf("get track token: %w", err))
					lastErr = err
					continue
				}

				// Step B: Get CDN stream URL and format via media.deezer.com/v1/get_url
				cdnURL, chosenFormat, err := session.GetMediaURL(ctx, tokenRes.Results.TrackToken, quality)
				if err != nil {
					c.pool.MarkError(session.Account.ID, err)
					lastErr = fmt.Errorf("account %s get_url: %w", session.Account.Username, err)
					continue
				}

				ext := "flac"
				if strings.Contains(strings.ToUpper(chosenFormat), "MP3") {
					ext = "mp3"
				}

				// Step C: Fetch encrypted stream from CDN
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, cdnURL, nil)
				if err != nil {
					lastErr = err
					continue
				}
				req.Header.Set("User-Agent", defaultUserAgent)

				resp, err := c.httpClient.Do(req)
				if err != nil {
					c.pool.MarkError(session.Account.ID, fmt.Errorf("fetch CDN: %w", err))
					lastErr = err
					continue
				}

				if resp.StatusCode != http.StatusOK {
					resp.Body.Close()
					c.pool.MarkError(session.Account.ID, fmt.Errorf("CDN HTTP %d", resp.StatusCode))
					lastErr = fmt.Errorf("CDN HTTP %d", resp.StatusCode)
					continue
				}

				// Step D: Filter/decrypt stream chunks into a temp file
				tempFile, err := os.CreateTemp(cacheDir, fmt.Sprintf("tmp-%d-*.%s", track.ID, ext))
				if err != nil {
					resp.Body.Close()
					lastErr = fmt.Errorf("create temp file: %w", err)
					break
				}

				decryptErr := filter.DecryptStream(tempFile, resp.Body, trackIDStr)
				_ = tempFile.Close()
				_ = resp.Body.Close()

				if decryptErr != nil {
					_ = os.Remove(tempFile.Name())
					c.pool.MarkError(session.Account.ID, fmt.Errorf("decrypt stream: %w", decryptErr))
					lastErr = decryptErr
					continue
				}

				// Validate FLAC magic header if format is FLAC
				if ext == "flac" {
					fCheck, chkErr := os.Open(tempFile.Name())
					if chkErr != nil {
						_ = os.Remove(tempFile.Name())
						lastErr = fmt.Errorf("open decrypted temp file: %w", chkErr)
						continue
					}
					hdr := make([]byte, 4)
					_, _ = io.ReadFull(fCheck, hdr)
					fCheck.Close()
					if string(hdr) != "fLaC" {
						_ = os.Remove(tempFile.Name())
						c.pool.MarkError(session.Account.ID, fmt.Errorf("corrupt FLAC stream: missing fLaC magic header"))
						lastErr = fmt.Errorf("corrupt FLAC stream header")
						continue
					}
				}

				// Step E: Atomic rename to final cache path
				destFile := filepath.Join(cacheDir, fmt.Sprintf("%d.%s", track.ID, ext))
				if renErr := os.Rename(tempFile.Name(), destFile); renErr != nil {
					_ = os.Remove(tempFile.Name())
					lastErr = fmt.Errorf("commit cache file: %w", renErr)
					break
				}

				log.Printf("[DEEZER] ✅ Decrypted and cached track %d (%q - %q) -> %s (format: %s via account: %s)",
					track.ID, track.Artist.Name, track.Title, destFile, chosenFormat, session.Account.Username)

				if absPath, absErr := filepath.Abs(destFile); absErr == nil {
					return absPath, nil
				}
				return destFile, nil
			}
		}

		if lastErr != nil {
			log.Printf("[DEEZER] ⚠️ ARL account streaming failed for track %d: %v", track.ID, lastErr)
		}

		// If streaming failed but a cached MP3 is present, use it as fallback
		if fi, err := os.Stat(mp3File); err == nil && fi.Size() > 1024 {
			if absPath, absErr := filepath.Abs(mp3File); absErr == nil {
				return absPath, nil
			}
			return mp3File, nil
		}
	}

	// 3. Fallback to custom mirror (e.g. dzr instance) if configured and healthy
	if c.customURL != "" {
		if !c.isMirrorHealthy(ctx) {
			return "", fmt.Errorf("deezer mirror %s is unhealthy (no active accounts)", c.customURL)
		}

		format := "FLAC"
		if strings.ToUpper(quality) == "LOW" || strings.ToUpper(quality) == "HIGH" {
			format = "MP3_320"
		}
		customStream, err := c.getCustomStreamURL(ctx, track, format)
		if err == nil && customStream != "" {
			return customStream, nil
		}

		c.mu.Lock()
		c.mirrorHealthy = false
		c.lastHealthCheck = time.Now()
		c.mu.Unlock()
	}

	return "", fmt.Errorf("no full stream available for Deezer track %d (configure active ARL account or mirror)", track.ID)
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

// StreamFilter returns the current active stream filter profile
func (c *Client) StreamFilter() *StreamFilterProfile {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.streamFilter
}

// SetStreamFilter updates or resets the stream filter profile
func (c *Client) SetStreamFilter(spec string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.TrimSpace(spec) == "" {
		c.streamFilter = nil
		return nil
	}
	p, err := ParseStreamFilterSpec(spec)
	if err != nil {
		return err
	}
	c.streamFilter = p
	return nil
}


