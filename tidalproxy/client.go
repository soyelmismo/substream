package tidalproxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PoolConfig holds configuration for the proxy pool
type PoolConfig struct {
	HealthInterval time.Duration
	Timeout        time.Duration
	Quality        string // default streaming quality
}

type instance struct {
	url     string
	healthy atomic.Bool
}

// Pool implements TidalProxy with multi-instance failover
type Pool struct {
	instances []*instance
	current   atomic.Int32
	client    *http.Client
	quality   string
	mu        sync.RWMutex
}

// NewPool creates a proxy pool from a list of hifi-api base URLs
func NewPool(urls []string, cfg PoolConfig) *Pool {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Quality == "" {
		cfg.Quality = "HI_RES_LOSSLESS"
	}
	if cfg.HealthInterval == 0 {
		cfg.HealthInterval = 30 * time.Second
	}

	p := &Pool{
		client: &http.Client{
			Timeout: cfg.Timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse // don't follow redirects
			},
		},
		quality: cfg.Quality,
	}

	for _, u := range urls {
		p.instances = append(p.instances, &instance{url: strings.TrimSuffix(u, "/")})
	}

	go p.healthCheck(cfg.HealthInterval)
	return p
}

func (p *Pool) SetInstances(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.instances = nil
	for _, u := range urls {
		p.instances = append(p.instances, &instance{url: strings.TrimSuffix(u, "/")})
	}
}

func (p *Pool) healthCheck(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		p.mu.RLock()
		instances := p.instances
		p.mu.RUnlock()

		for _, inst := range instances {
			go func(i *instance) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				req, _ := http.NewRequestWithContext(ctx, "GET", i.url+"/info/?id=123", nil)
				resp, err := p.client.Do(req)
				healthy := err == nil && (resp.StatusCode == 200 || resp.StatusCode == 404)
				if resp != nil {
					resp.Body.Close()
				}
				i.healthy.Store(healthy)
			}(inst)
		}
	}
}

func (p *Pool) pick() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.instances) == 0 {
		return "", fmt.Errorf("no proxy instances configured")
	}

	start := p.current.Add(1) % int32(len(p.instances))
	for i := 0; i < len(p.instances); i++ {
		idx := (int(start) + i) % len(p.instances)
		if p.instances[idx].healthy.Load() || i == len(p.instances)-1 {
			return p.instances[idx].url, nil
		}
	}
	return p.instances[0].url, nil
}

// doFetchRaw performs a GET request with retries and returns the raw body bytes.
// It handles proxy selection, URL construction, header injection, and the 3-attempt retry loop.
func (p *Pool) doFetchRaw(ctx context.Context, path string, query url.Values, clientIP string) ([]byte, error) {
	var lastErr error
	for i := 0; i < 3; i++ {
		base, err := p.pick()
		if err != nil {
			return nil, err
		}

		u := base + path
		if len(query) > 0 {
			u += "?" + query.Encode()
		}

		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
		if clientIP != "" {
			req.Header.Set("X-Forwarded-For", clientIP)
			req.Header.Set("X-Real-IP", clientIP)
		}

		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request %s (try %d): %w", path, i+1, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("upstream %s (try %d) returned %d: %s", path, i+1, resp.StatusCode, string(body))
			if resp.StatusCode == 404 {
				return nil, lastErr
			}
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		return body, err
	}
	return nil, lastErr
}

// doFetchRawWithInstance performs a GET request to a specific proxy instance (by index).
// Used for retry logic when 404 errors are encountered.
func (p *Pool) doFetchRawWithInstance(ctx context.Context, path string, query url.Values, clientIP string, instanceIdx int) ([]byte, error) {
	p.mu.RLock()
	if len(p.instances) == 0 {
		p.mu.RUnlock()
		return nil, fmt.Errorf("no hifi-api proxies configured")
	}
	// Select proxy based on instanceIdx (round-robin offset)
	idx := instanceIdx % len(p.instances)
	base := p.instances[idx].url
	p.mu.RUnlock()

	u := base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
		req.Header.Set("X-Real-IP", clientIP)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", path, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("upstream %s returned %d: %s", path, resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	return body, err
}

// apiGet performs a GET to a hifi-api endpoint and decodes JSON from .data field with retries.
// It now accepts an optional clientIP to set X-Forwarded-For for Tidal's IP-locked streaming.
func (p *Pool) apiGet(ctx context.Context, path string, query url.Values, result interface{}, clientIP string) error {
	return p.apiGetWithRetry(ctx, path, query, result, clientIP, 3) // Default 3 retries
}

// apiGetWithRetry attempts the request with multiple proxies on 404 errors.
// This handles Tidal API inconsistency where an artist exists on some proxies but not others.
func (p *Pool) apiGetWithRetry(ctx context.Context, path string, query url.Values, result interface{}, clientIP string, maxRetries int) error {
	var lastErr error
	for try := 0; try < maxRetries; try++ {
		body, err := p.doFetchRawWithInstance(ctx, path, query, clientIP, try)
		if err != nil {
			lastErr = err
			// Check if it's a 404 error
			if strings.Contains(err.Error(), "404") {
				log.Printf("[TIDAL] apiGet 404 on try %d/%d for %s, retrying with next proxy...", try+1, maxRetries, path)
				continue // Try next proxy
			}
			return err // Non-404 error, fail fast
		}

		// Envelope handling: many hifi-api endpoints wrap data in a top-level JSON
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}

		if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Data) > 0 {
			if err := json.Unmarshal(envelope.Data, result); err == nil {
				return nil
			}
		}

		// Fallback: try unmarshaling directly into result
		if err := json.Unmarshal(body, result); err != nil {
			return fmt.Errorf("decode result: %w (body: %s)", err, string(body))
		}
		return nil
	}
	return fmt.Errorf("all %d proxies returned 404 or error for %s: %v", maxRetries, path, lastErr)
}

// apiGet performs a GET to a hifi-api endpoint and decodes JSON from .data field with retries.
// DEPRECATED: use apiGetWithRetry directly.
func (p *Pool) apiGetOld(ctx context.Context, path string, query url.Values, result interface{}, clientIP string) error {
	body, err := p.doFetchRaw(ctx, path, query, clientIP)
	if err != nil {
		return err
	}

	// Envelope handling: many hifi-api endpoints wrap data in a top-level JSON
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, result); err == nil {
			return nil
		}
	}

	// Fallback: try unmarshaling directly into result
	if err := json.Unmarshal(body, result); err != nil {
		return fmt.Errorf("decode result: %w (body: %s)", err, string(body))
	}
	return nil
}

// apiGetRaw returns the raw body as bytes with retries.
// It now accepts an optional clientIP to set X-Forwarded-For.
func (p *Pool) apiGetRaw(ctx context.Context, path string, query url.Values, clientIP string) ([]byte, error) {
	return p.apiGetRawWithRetry(ctx, path, query, clientIP, 3)
}

// apiGetRawWithRetry attempts the request with multiple proxies on 404 errors.
func (p *Pool) apiGetRawWithRetry(ctx context.Context, path string, query url.Values, clientIP string, maxRetries int) ([]byte, error) {
	var lastErr error
	for try := 0; try < maxRetries; try++ {
		body, err := p.doFetchRawWithInstance(ctx, path, query, clientIP, try)
		if err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "404") {
				log.Printf("[TIDAL] apiGetRaw 404 on try %d/%d for %s, retrying with next proxy...", try+1, maxRetries, path)
				continue
			}
			return nil, err
		}
		return body, nil
	}
	return nil, fmt.Errorf("all %d proxies returned 404 or error for %s: %v", maxRetries, path, lastErr)
}

// =====================================================================
// TidalProxy Interface Implementation
// =====================================================================

func (p *Pool) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	var track TidalTrack
	q := url.Values{"id": {fmt.Sprint(trackID)}}
	if err := p.apiGet(ctx, "/info/", q, &track, ""); err != nil {
		return nil, err
	}
	return &track, nil
}

func (p *Pool) GetCoverUUIDForAlbum(ctx context.Context, albumID int) string {
	if a, err := p.GetAlbumInfo(ctx, albumID); err == nil && a.Cover != "" {
		return a.Cover
	}
	return ""
}

func (p *Pool) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	body, err := p.apiGetRaw(ctx, "/album/", url.Values{"id": {fmt.Sprint(albumID)}}, "")
	if err != nil {
		return nil, err
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode album envelope: %w", err)
	}

	var album TidalAlbum
	if err := json.Unmarshal(envelope.Data, &album); err != nil {
		// fallback to direct decode
		if err2 := json.Unmarshal(body, &album); err2 != nil {
			return nil, fmt.Errorf("decode album data: %w", err)
		}
	}

	// items wrapper
	var raw struct {
		Items []struct {
			Item TidalTrack `json:"item"`
		} `json:"items"`
	}
	if json.Unmarshal(envelope.Data, &raw) == nil && len(raw.Items) > 0 {
		album.Items = make([]TidalTrack, len(raw.Items))
		for i, it := range raw.Items {
			album.Items[i] = it.Item
		}
	}

	return &album, nil
}

func (p *Pool) GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error) {
	body, err := p.apiGetRaw(ctx, "/artist/", url.Values{"id": {fmt.Sprint(artistID)}}, "")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Artist TidalArtist `json:"artist"`
		Cover  *struct {
			Size750 string `json:"750"`
		} `json:"cover"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode artist: %w", err)
	}

	return &TidalArtistDetail{
		Artist: resp.Artist,
		Cover:  resp.Cover,
	}, nil
}

func (p *Pool) GetArtistAlbums(ctx context.Context, artistID int, skipTracks bool) (*TidalArtistPage, error) {
	q := url.Values{"f": {fmt.Sprint(artistID)}}
	if skipTracks {
		q.Set("skip_tracks", "true")
	}

	body, err := p.apiGetRaw(ctx, "/artist/", q, "")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Albums struct {
			Items []TidalAlbum `json:"items"`
		} `json:"albums"`
		Tracks []TidalTrack `json:"tracks"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode artist albums: %w", err)
	}

	return &TidalArtistPage{
		Albums: resp.Albums,
		Tracks: resp.Tracks,
	}, nil
}

func (p *Pool) SearchTracks(ctx context.Context, query string, limit, offset int) ([]TidalTrack, error) {
	q := url.Values{
		"s":      {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}
	var result struct {
		Items []TidalTrack `json:"items"`
	}
	if err := p.apiGet(ctx, "/search/", q, &result, ""); err != nil {
		return nil, err
	}
	if limit > 0 && len(result.Items) > limit {
		result.Items = result.Items[:limit]
	}
	return result.Items, nil
}

func (p *Pool) SearchArtists(ctx context.Context, query string, limit, offset int) ([]TidalArtist, error) {
	q := url.Values{
		"a":      {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}
	var result struct {
		Artists struct {
			Items []TidalArtist `json:"items"`
		} `json:"artists"`
		// support flat items if proxy doesn't use top-hits
		Items []TidalArtist `json:"items"`
	}
	if err := p.apiGet(ctx, "/search/", q, &result, ""); err != nil {
		return nil, err
	}
	if len(result.Artists.Items) > 0 {
		items := result.Artists.Items
		if limit > 0 && len(items) > limit {
			items = items[:limit]
		}
		return items, nil
	}
	if limit > 0 && len(result.Items) > limit {
		result.Items = result.Items[:limit]
	}
	return result.Items, nil
}

func (p *Pool) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error) {
	q := url.Values{
		"al":     {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}
	var result struct {
		Albums struct {
			Items []TidalAlbum `json:"items"`
		} `json:"albums"`
		Items []TidalAlbum `json:"items"`
	}
	if err := p.apiGet(ctx, "/search/", q, &result, ""); err != nil {
		return nil, err
	}
	if len(result.Albums.Items) > 0 {
		items := result.Albums.Items
		if limit > 0 && len(items) > limit {
			items = items[:limit]
		}
		return items, nil
	}
	if limit > 0 && len(result.Items) > limit {
		result.Items = result.Items[:limit]
	}
	return result.Items, nil
}

func (p *Pool) GetStreamURL(ctx context.Context, trackID int, quality string, clientIP string) (string, error) {
	if quality == "" {
		quality = p.quality
	}

	qualities := []string{quality}
	if quality == "HI_RES_LOSSLESS" {
		qualities = append(qualities, "LOSSLESS", "HIGH")
	} else if quality == "LOSSLESS" {
		qualities = append(qualities, "HIGH")
	}

	var lastErr error
	previewCount := 0
	// Strategy: try up to 5 different proxies until we get a FULL track.
	// For each proxy, try the requested quality and its fallbacks.
	// Aggressive retry when PREVIEW is detected - try more proxies.
	maxRetries := 5
	for try := 0; try < maxRetries; try++ {
		for _, qStr := range qualities {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			log.Printf("[TIDAL] GetStreamURL track=%d try=%d/%d quality=%s", trackID, try, maxRetries-1, qStr)

			// 1. Try V2 OpenAPI Manifests first (HLS)
			v2Ctx, v2Cancel := context.WithTimeout(ctx, 3*time.Second)
			var formats []string
			switch qStr {
			case "HI_RES_LOSSLESS":
				formats = []string{"FLAC_HIRES", "MQA"}
			case "LOSSLESS":
				formats = []string{"FLAC"}
			case "HIGH":
				formats = []string{"AACLC"}
			case "LOW":
				formats = []string{"HEAACV1"}
			default:
				formats = []string{"FLAC_HIRES", "FLAC", "AACLC"}
			}

			qV2 := url.Values{
				"id":           {fmt.Sprint(trackID)},
				"manifestType": {"HLS"},
			}
			for _, f := range formats {
				qV2.Add("formats", f)
			}
			var v2Response struct {
				Data struct {
					Attributes struct {
						Manifest          string   `json:"manifest"`
						ManifestMimeType  string   `json:"manifestMimeType"`
						URI               string   `json:"uri"`
						Formats           []string `json:"formats"`
						TrackPresentation string   `json:"trackPresentation"`
					} `json:"attributes"`
				} `json:"data"`
			}
			err := p.apiGet(v2Ctx, "/trackManifests/", qV2, &v2Response, clientIP)
			v2Cancel()
			if err == nil {
				if v2Response.Data.Attributes.TrackPresentation == "PREVIEW" {
					lastErr = fmt.Errorf("proxy returned PREVIEW (V2)")
					previewCount++
					log.Printf("[TIDAL] GetStreamURL track=%d PREVIEW #%d from V2 API, trying next proxy...", trackID, previewCount)
					break // try next proxy in outer loop
				}
				// V2 URI is often a manifest URL (manifest.tidal.com), not direct audio.
				// Only use it if it's a direct audio URL (contains audio file extension or CDN pattern).
				uri := v2Response.Data.Attributes.URI
				if uri != "" && !strings.Contains(uri, "manifest.tidal.com") && !strings.Contains(uri, "/manifests/") {
					log.Printf("[TIDAL] GetStreamURL track=%d got direct URI via V2: %s...", trackID, uri[:min(50, len(uri))])
					return uri, nil
				}
				// If V2 returned a manifest URL, try to download and parse it directly
				if uri != "" && (strings.Contains(uri, "manifest.tidal.com") || strings.Contains(uri, "/manifests/")) {
					log.Printf("[TIDAL] GetStreamURL track=%d V2 returned manifest URL, attempting direct download: %s...", trackID, uri[:min(50, len(uri))])
					audioURL, err := p.downloadAndParseHLSManifest(ctx, uri, clientIP)
					if err == nil && audioURL != "" {
						log.Printf("[TIDAL] GetStreamURL track=%d got audio URL from V2 manifest: %s...", trackID, audioURL[:min(50, len(audioURL))])
						return audioURL, nil
					}
					log.Printf("[TIDAL] GetStreamURL track=%d failed to parse V2 manifest: %v, falling back to V1", trackID, err)
					log.Printf("[TIDAL] GetStreamURL track=%d V2 manifest parse error or manifest URL: %v", trackID, err)
				}
			}

			// 2. V1 fallback
			q := url.Values{
				"id":                {fmt.Sprint(trackID)},
				"quality":           {qStr},
				"playbackmode":      {"STREAM"},
				"assetpresentation": {"FULL"},
			}
			log.Printf("[TIDAL] GetStreamURL track=%d calling V1 /track/ with quality=%s", trackID, qStr)
			var stream TidalStreamInfo
			if err := p.apiGet(ctx, "/track/", q, &stream, clientIP); err == nil {
				if stream.TrackPresentation == "PREVIEW" {
					lastErr = fmt.Errorf("proxy returned PREVIEW (V1)")
					previewCount++
					log.Printf("[TIDAL] GetStreamURL track=%d PREVIEW #%d from V1 API, trying next proxy...", trackID, previewCount)
					break // try next proxy in outer loop
				}
				u, err := parseManifestURL(trackID, "V1", stream.ManifestMimeType, stream.Manifest)
				if err == nil {
					log.Printf("[TIDAL] GetStreamURL track=%d got URL via V1 manifest: %s...", trackID, u[:min(50, len(u))])
					return u, nil
				}
				lastErr = err
				log.Printf("[TIDAL] GetStreamURL track=%d V1 manifest parse error: %v", trackID, err)
			} else {
				lastErr = err
				log.Printf("[TIDAL] GetStreamURL track=%d V1 /track/ error: %v", trackID, err)
			}
		}
	}

	return "", fmt.Errorf("could not get full stream after %d retries (%d PREVIEWs): %v", maxRetries, previewCount, lastErr)
}

// downloadAndParseHLSManifest downloads a Tidal manifest and extracts the audio stream URL
// Tidal V2 manifests are BTS (Base64 encoded) or MPD (MPEG-DASH) format
func (p *Pool) downloadAndParseHLSManifest(ctx context.Context, manifestURL string, clientIP string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("manifest fetch failed: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read manifest body: %w", err)
	}

	bodyStr := string(body)
	log.Printf("[TIDAL] Manifest content type: %s, size: %d bytes", resp.Header.Get("Content-Type"), len(body))
	log.Printf("[TIDAL] Manifest preview (first 200 chars): %s", bodyStr[:min(200, len(bodyStr))])

	// Tidal V2 manifests are often Base64-encoded BTS format
	// Try to decode base64
	decoded, err := base64.StdEncoding.DecodeString(bodyStr)
	if err == nil && len(decoded) > 0 {
		log.Printf("[TIDAL] Manifest is Base64 encoded, decoded size: %d bytes", len(decoded))
		bodyStr = string(decoded)
	}

	// Try to parse as JSON
	var manifest struct {
		URLs []string `json:"urls"`
		URL  string   `json:"url"`
	}
	if err := json.Unmarshal([]byte(bodyStr), &manifest); err == nil {
		if len(manifest.URLs) > 0 {
			log.Printf("[TIDAL] Found audio URL in JSON manifest: %s...", manifest.URLs[0][:min(50, len(manifest.URLs[0]))])
			return manifest.URLs[0], nil
		}
		if manifest.URL != "" {
			log.Printf("[TIDAL] Found audio URL in JSON manifest: %s...", manifest.URL[:min(50, len(manifest.URL))])
			return manifest.URL, nil
		}
	}

	// Look for MPD (MPEG-DASH) BaseURL or SegmentURL
	if strings.Contains(bodyStr, "BaseURL") {
		// Extract BaseURL from MPD
		start := strings.Index(bodyStr, "<BaseURL>")
		end := strings.Index(bodyStr, "</BaseURL>")
		if start != -1 && end != -1 && end > start+9 {
			url := bodyStr[start+9 : end]
			log.Printf("[TIDAL] Found BaseURL in MPD manifest: %s...", url[:min(50, len(url))])
			return url, nil
		}
	}

	// Parse HLS M3U8 manifest - look for variant streams after #EXT-X-STREAM-INF
	lines := strings.Split(bodyStr, "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// This is a URL line (either variant URL in master playlist or segment URL)
		if strings.HasPrefix(line, "http") {
			log.Printf("[TIDAL] Found stream URL in HLS manifest: %s...", line[:min(50, len(line))])
			return line, nil
		}
		// Check if next line after STREAM-INF is a relative URL
		if i > 0 && strings.HasPrefix(lines[i-1], "#EXT-X-STREAM-INF") && !strings.HasPrefix(line, "#") {
			// Relative URL, construct absolute
			baseURL := manifestURL
			if lastSlash := strings.LastIndex(baseURL, "/"); lastSlash != -1 {
				baseURL = baseURL[:lastSlash+1]
			}
			absoluteURL := baseURL + line
			log.Printf("[TIDAL] Found relative stream URL in HLS manifest: %s...", absoluteURL[:min(50, len(absoluteURL))])
			return absoluteURL, nil
		}
	}

	return "", fmt.Errorf("no audio URL found in manifest")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (p *Pool) GetCoverURL(coverUUID string, size int) string {
	if coverUUID == "" {
		return ""
	}
	slug := strings.ReplaceAll(coverUUID, "-", "/")
	return fmt.Sprintf("https://resources.tidal.com/images/%s/%dx%d.jpg", slug, size, size)
}

func (p *Pool) GetCoverByTrackID(ctx context.Context, trackID int) (*TidalCover, error) {
	body, err := p.apiGetRaw(ctx, "/cover/", url.Values{"id": {fmt.Sprint(trackID)}}, "")
	if err != nil {
		return nil, err
	}
	var resp struct {
		Covers []struct {
			URL1280 string `json:"1280"`
			URL640  string `json:"640"`
			URL80   string `json:"80"`
		} `json:"covers"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if len(resp.Covers) == 0 {
		return nil, fmt.Errorf("no covers found")
	}
	return &TidalCover{
		URL1280: resp.Covers[0].URL1280,
		URL640:  resp.Covers[0].URL640,
		URL80:   resp.Covers[0].URL80,
	}, nil
}

func (p *Pool) GetRecommendations(ctx context.Context, trackID int) ([]TidalTrack, error) {
	// Usar endpoint directo /recommendations/ de hifi-api (usa API nativa de Tidal)
	q := url.Values{
		"id": {fmt.Sprint(trackID)},
	}
	body, err := p.apiGetRaw(ctx, "/recommendations/", q, "")
	if err != nil {
		log.Printf("[TIDAL] GetRecommendations ERROR for track %d: %v", trackID, err)
		return nil, err
	}
	
	// Estructura real de Tidal: data.items[].track
	var result struct {
		Data struct {
			Items []struct {
				Track TidalTrack `json:"track"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		log.Printf("[TIDAL] GetRecommendations PARSE ERROR for track %d: %v", trackID, err)
		return nil, err
	}
	
	// Extraer tracks del wrapper
	tracks := make([]TidalTrack, 0, len(result.Data.Items))
	for _, item := range result.Data.Items {
		if item.Track.ID != 0 {
			tracks = append(tracks, item.Track)
		}
	}
	
	log.Printf("[TIDAL] GetRecommendations track %d: got %d recommendations", trackID, len(tracks))
	return tracks, nil
}

func (p *Pool) GetTopTracks(ctx context.Context, limit int) ([]TidalTrack, error) {
	// Use a popular artist or a generic search if no direct top-tracks endpoint
	var result struct {
		Items []TidalTrack `json:"items"`
	}
	q := url.Values{
		"query": {"top"},
		"limit": {fmt.Sprint(limit)},
	}
	if err := p.apiGet(ctx, "/search/tracks/", q, &result, ""); err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (p *Pool) GetArtistTopTracks(ctx context.Context, artistID int, limit int) ([]TidalTrack, error) {
	var result struct {
		Items []TidalTrack `json:"items"`
	}
	q := url.Values{
		"id":    {fmt.Sprint(artistID)},
		"limit": {fmt.Sprint(limit)},
	}
	// Many proxies use /artist/toptracks/ or similar. 
	// The most common route in hifi-api derivatives for this is /artist/toptracks/
	if err := p.apiGet(ctx, "/artist/toptracks/", q, &result, ""); err != nil {
		return nil, err
	}
	return result.Items, nil
}

// GetArtistAlbumCount returns the number of albums for an artist
// This is used by CachedProxy to cache the count
func (p *Pool) GetArtistAlbumCount(ctx context.Context, artistID int) int {
	page, err := p.GetArtistAlbums(ctx, artistID, true)
	if err != nil || page == nil {
		return 0
	}
	return len(page.Albums.Items)
}

func (p *Pool) GetSimilarArtists(ctx context.Context, artistID int) ([]TidalArtist, error) {
	var result struct {
		Artists []TidalArtist `json:"artists"`
	}
	if err := p.apiGet(ctx, "/artist/similar/", url.Values{"id": {fmt.Sprint(artistID)}}, &result, ""); err != nil {
		return nil, err
	}
	return result.Artists, nil
}

func (p *Pool) GetLyrics(ctx context.Context, trackID int) (*TidalLyrics, error) {
	var resp struct {
		Lyrics TidalLyrics `json:"lyrics"`
	}
	if err := p.apiGet(ctx, "/lyrics/", url.Values{"id": {fmt.Sprint(trackID)}}, &resp, ""); err != nil {
		return nil, err
	}
	return &resp.Lyrics, nil
}

// Manifest Parsing
func parseManifestURL(trackID int, version, mimeType, manifest string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(manifest)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(manifest)
		if err != nil {
			decoded = []byte(manifest)
		}
	}

	content := string(decoded)
	contentPreview := content
	if len(contentPreview) > 100 {
		contentPreview = contentPreview[:100] + "..."
	}

	// DASH manifest: look for <BaseURL>
	if strings.Contains(content, "<BaseURL>") {
		start := strings.Index(content, "<BaseURL>") + len("<BaseURL>")
		end := strings.Index(content[start:], "</BaseURL>")
		if end > 0 {
			u := content[start : start+end]
			url := strings.ReplaceAll(u, "&amp;", "&")
			log.Printf("[TIDAL] parseManifestURL track=%d %s: DASH BaseURL found: %s...", trackID, version, url[:min(50, len(url))])
			return url, nil
		}
	}

	// Fallback for DASH or other XMLs: find first http URL that isn't a schema
	if strings.HasPrefix(content, "<") || strings.Contains(content, "xml") {
		lines := strings.Split(content, ">")
		for _, line := range lines {
			if strings.Contains(line, "http") && !strings.Contains(line, "w3.org") && !strings.Contains(line, "mpeg.org") {
				start := strings.Index(line, "http")
				end := strings.IndexAny(line[start:], " \"'<>")
				if end > 0 {
					u := line[start : start+end]
					url := strings.ReplaceAll(u, "&amp;", "&")
					log.Printf("[TIDAL] parseManifestURL track=%d %s: XML fallback found: %s...", trackID, version, url[:min(50, len(url))])
					return url, nil
				}
				url := strings.ReplaceAll(line[start:], "&amp;", "&")
				log.Printf("[TIDAL] parseManifestURL track=%d %s: XML fallback (no end): %s...", trackID, version, url[:min(50, len(url))])
				return url, nil
			}
		}
	}

	// JSON manifest (BTS): find "url" or "urls" array
	if strings.HasPrefix(content, "{") {
		var manifestData struct {
			URL  string   `json:"url"`
			URLs []string `json:"urls"`
		}
		if err := json.Unmarshal(decoded, &manifestData); err == nil {
			if manifestData.URL != "" {
				log.Printf("[TIDAL] parseManifestURL track=%d %s: JSON url found: %s...", trackID, version, manifestData.URL[:min(50, len(manifestData.URL))])
				return manifestData.URL, nil
			}
			if len(manifestData.URLs) > 0 {
				log.Printf("[TIDAL] parseManifestURL track=%d %s: JSON urls[0] found: %s...", trackID, version, manifestData.URLs[0][:min(50, len(manifestData.URLs[0]))])
				return manifestData.URLs[0], nil
			}
		}
	}

	// M3U8: first line that starts with http
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http") {
			log.Printf("[TIDAL] parseManifestURL track=%d %s: M3U8 found: %s...", trackID, version, line[:min(50, len(line))])
			return line, nil
		}
	}

	return "", fmt.Errorf("could not extract URL from manifest (type: %s content preview: %.50s)", mimeType, content)
}
