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
		u = strings.TrimRight(u, "/")
		inst := &instance{url: u}
		inst.healthy.Store(true)
		p.instances = append(p.instances, inst)
	}

	// background health checker
	if len(p.instances) > 0 {
		go p.healthLoop(cfg.HealthInterval)
	}

	return p
}

// pick returns the URL of a healthy instance
func (p *Pool) pick() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.instances)
	if n == 0 {
		return "", fmt.Errorf("no proxy instances configured")
	}
	start := int(p.current.Add(1)) % n
	for i := 0; i < n; i++ {
		inst := p.instances[(start+i)%n]
		if inst.healthy.Load() {
			return inst.url, nil
		}
	}
	// all unhealthy, try first anyway
	return p.instances[0].url, nil
}

func (p *Pool) SetInstances(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var newInstances []*instance
	for _, u := range urls {
		u = strings.TrimRight(u, "/")
		// keep existing structure if URL matches to preserve health status?
		// for simplicity, just rebuild
		inst := &instance{url: u}
		inst.healthy.Store(true)
		newInstances = append(newInstances, inst)
	}
	p.instances = newInstances
	log.Printf("tidalproxy: updated pool with %d instances", len(p.instances))
}

func (p *Pool) healthLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.RLock()
		instances := make([]*instance, len(p.instances))
		copy(instances, p.instances)
		p.mu.RUnlock()

		for _, inst := range instances {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			req, _ := http.NewRequestWithContext(ctx, "GET", inst.url+"/", nil)
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
			resp, err := p.client.Do(req)
			cancel()
			wasHealthy := inst.healthy.Load()
			if err != nil || resp.StatusCode >= 400 { // 4xx could also mean Cloudflare block
				inst.healthy.Store(false)
				if wasHealthy {
					log.Printf("tidalproxy: instance %s marked unhealthy", inst.url)
				}
			} else {
				inst.healthy.Store(true)
				if !wasHealthy {
					log.Printf("tidalproxy: instance %s recovered", inst.url)
				}
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}


// apiGet performs a GET to a hifi-api endpoint and decodes JSON from .data field
func (p *Pool) apiGet(ctx context.Context, path string, query url.Values, result interface{}) error {
	base, err := p.pick()
	if err != nil {
		return err
	}

	u := base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")


	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("request %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upstream %s returned %d: %s", path, resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	// hifi-api wraps responses in {"version": "...", "data": {...}}
	// try to extract .data first, fall back to full body
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &envelope) == nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, result); err != nil {
			log.Printf("tidalproxy: error decoding .data envelope: %v (raw: %s)", err, string(envelope.Data))
			return err
		}
		return nil
	}

	if err := json.Unmarshal(body, result); err != nil {
		log.Printf("tidalproxy: error decoding raw body: %v (raw: %s)", err, string(body))
		return err
	}
	return nil
}

// apiGetRaw returns the raw body as bytes (for responses that don't fit the envelope pattern)
func (p *Pool) apiGetRaw(ctx context.Context, path string, query url.Values) ([]byte, error) {
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


	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upstream %s returned %d: %s", path, resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// =====================================================================
// TidalProxy Interface Implementation
// =====================================================================

func (p *Pool) GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error) {
	var track TidalTrack
	q := url.Values{"id": {fmt.Sprint(trackID)}}
	if err := p.apiGet(ctx, "/info/", q, &track); err != nil {
		return nil, err
	}
	return &track, nil
}

func (p *Pool) GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error) {
	body, err := p.apiGetRaw(ctx, "/album/", url.Values{"id": {fmt.Sprint(albumID)}})
	if err != nil {
		return nil, err
	}

	// /album/ response has a special structure: {version, data: {album_metadata + items: [...]}}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode album envelope: %w", err)
	}

	var album TidalAlbum
	if err := json.Unmarshal(envelope.Data, &album); err != nil {
		return nil, fmt.Errorf("decode album data: %w", err)
	}

	// items from /album/ are wrapped: {item: {track data}}
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
	body, err := p.apiGetRaw(ctx, "/artist/", url.Values{"id": {fmt.Sprint(artistID)}})
	if err != nil {
		return nil, err
	}

	// response: {version, artist: {...}, cover: {...}}
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

	body, err := p.apiGetRaw(ctx, "/artist/", q)
	if err != nil {
		return nil, err
	}

	// response: {version, albums: {items: [...]}, tracks: [...]}
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
	if err := p.apiGet(ctx, "/search/", q, &result); err != nil {
		log.Printf("tidalproxy: SearchTracks error: %v", err)
		return nil, err
	}
	log.Printf("tidalproxy: SearchTracks query=%q found %d items", query, len(result.Items))
	return result.Items, nil
}

func (p *Pool) SearchArtists(ctx context.Context, query string, limit, offset int) ([]TidalArtist, error) {
	q := url.Values{
		"a":      {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}

	body, err := p.apiGetRaw(ctx, "/search/", q)
	if err != nil {
		return nil, err
	}

	// top-hits response: {data: {tracks: [...], artists: [...]}}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	var raw json.RawMessage
	if json.Unmarshal(body, &envelope) == nil && len(envelope.Data) > 0 {
		raw = envelope.Data
	} else {
		raw = body
	}

	var topHits struct {
		Artists []TidalArtist `json:"artists"`
	}
	if json.Unmarshal(raw, &topHits) == nil && len(topHits.Artists) > 0 {
		return topHits.Artists, nil
	}

	// fallback: items array directly
	var items struct {
		Items []TidalArtist `json:"items"`
	}
	if json.Unmarshal(raw, &items) == nil {
		return items.Items, nil
	}

	return nil, nil
}

func (p *Pool) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error) {
	q := url.Values{
		"al":     {query},
		"limit":  {fmt.Sprint(limit)},
		"offset": {fmt.Sprint(offset)},
	}

	body, err := p.apiGetRaw(ctx, "/search/", q)
	if err != nil {
		return nil, err
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	var raw json.RawMessage
	if json.Unmarshal(body, &envelope) == nil && len(envelope.Data) > 0 {
		raw = envelope.Data
	} else {
		raw = body
	}

	var topHits struct {
		Albums []TidalAlbum `json:"albums"`
	}
	if json.Unmarshal(raw, &topHits) == nil && len(topHits.Albums) > 0 {
		return topHits.Albums, nil
	}

	var items struct {
		Items []TidalAlbum `json:"items"`
	}
	if json.Unmarshal(raw, &items) == nil {
		return items.Items, nil
	}

	return nil, nil
}

func (p *Pool) GetStreamURL(ctx context.Context, trackID int, quality string) (string, error) {
	if quality == "" {
		quality = p.quality
	}
	q := url.Values{
		"id":      {fmt.Sprint(trackID)},
		"quality": {quality},
	}

	var stream TidalStreamInfo
	if err := p.apiGet(ctx, "/track/", q, &stream); err != nil {
		return "", err
	}

	return parseManifestURL(stream.ManifestMimeType, stream.Manifest)
}

func (p *Pool) GetCoverURL(coverUUID string, size int) string {
	if coverUUID == "" {
		return ""
	}
	slug := strings.ReplaceAll(coverUUID, "-", "/")
	return fmt.Sprintf("https://resources.tidal.com/images/%s/%dx%d.jpg", slug, size, size)
}

func (p *Pool) GetCoverByTrackID(ctx context.Context, trackID int) (*TidalCover, error) {
	body, err := p.apiGetRaw(ctx, "/cover/", url.Values{"id": {fmt.Sprint(trackID)}})
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
	var result struct {
		Items []TidalTrack `json:"items"`
	}
	q := url.Values{"id": {fmt.Sprint(trackID)}}
	if err := p.apiGet(ctx, "/recommendations/", q, &result); err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (p *Pool) GetSimilarArtists(ctx context.Context, artistID int) ([]TidalArtist, error) {
	body, err := p.apiGetRaw(ctx, "/artist/similar/", url.Values{"id": {fmt.Sprint(artistID)}})
	if err != nil {
		return nil, err
	}

	var resp struct {
		Artists []TidalArtist `json:"artists"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return resp.Artists, nil
}

func (p *Pool) GetLyrics(ctx context.Context, trackID int) (*TidalLyrics, error) {
	body, err := p.apiGetRaw(ctx, "/lyrics/", url.Values{"id": {fmt.Sprint(trackID)}})
	if err != nil {
		return nil, err
	}

	var resp struct {
		Lyrics TidalLyrics `json:"lyrics"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return &resp.Lyrics, nil
}

// =====================================================================
// Manifest Parsing
// =====================================================================

// parseManifestURL extracts the streaming URL from a Tidal manifest
func parseManifestURL(mimeType, manifest string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(manifest)
	if err != nil {
		// try raw base64
		decoded, err = base64.RawStdEncoding.DecodeString(manifest)
		if err != nil {
			return "", fmt.Errorf("decode manifest base64: %w", err)
		}
	}

	content := string(decoded)

	// BTS manifest: JSON with "urls" array
	if strings.Contains(mimeType, "bts") || strings.HasPrefix(content, "{") {
		var bts struct {
			URLs []string `json:"urls"`
		}
		if err := json.Unmarshal(decoded, &bts); err == nil && len(bts.URLs) > 0 {
			return bts.URLs[0], nil
		}
	}

	// DASH manifest: look for <BaseURL>
	if strings.Contains(content, "<BaseURL>") {
		start := strings.Index(content, "<BaseURL>") + len("<BaseURL>")
		end := strings.Index(content[start:], "</BaseURL>")
		if end > 0 {
			return content[start : start+end], nil
		}
	}

	// M3U8: first line that starts with http
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http") {
			return line, nil
		}
	}

	return "", fmt.Errorf("could not extract URL from manifest (type: %s)", mimeType)
}
