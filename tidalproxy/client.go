package tidalproxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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

// apiGet performs a GET to a hifi-api endpoint and decodes JSON from .data field with retries.
// It now accepts an optional clientIP to set X-Forwarded-For for Tidal's IP-locked streaming.
func (p *Pool) apiGet(ctx context.Context, path string, query url.Values, result interface{}, clientIP string) error {
	var lastErr error
	for i := 0; i < 3; i++ {
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
			if resp.StatusCode == 404 { // don't retry on 404
				return lastErr
			}
			continue
		}

		// Envelope handling: many hifi-api endpoints wrap data in a top-level JSON
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("read body: %w", err)
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
	return lastErr
}

// apiGetRaw returns the raw body as bytes with retries.
// It now accepts an optional clientIP to set X-Forwarded-For.
func (p *Pool) apiGetRaw(ctx context.Context, path string, query url.Values, clientIP string) ([]byte, error) {
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
	// Strategy: try up to 3 different proxies until we get a FULL track.
	// For each proxy, try the requested quality and its fallbacks.
	for try := 0; try < 3; try++ {
		for _, qStr := range qualities {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}

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
					break // try next proxy in outer loop
				}
				if v2Response.Data.Attributes.URI != "" {
					return v2Response.Data.Attributes.URI, nil
				}
				if v2Response.Data.Attributes.Manifest != "" {
					u, err := parseManifestURL(v2Response.Data.Attributes.ManifestMimeType, v2Response.Data.Attributes.Manifest)
					if err == nil {
						return u, nil
					}
				}
			}

			// 2. V1 fallback
			q := url.Values{
				"id":                {fmt.Sprint(trackID)},
				"quality":           {qStr},
				"playbackmode":      {"STREAM"},
				"assetpresentation": {"FULL"},
			}
			var stream TidalStreamInfo
			if err := p.apiGet(ctx, "/track/", q, &stream, clientIP); err == nil {
				if stream.TrackPresentation == "PREVIEW" {
					lastErr = fmt.Errorf("proxy returned PREVIEW (V1)")
					break // try next proxy in outer loop
				}
				u, err := parseManifestURL(stream.ManifestMimeType, stream.Manifest)
				if err == nil {
					return u, nil
				}
				lastErr = err
			} else {
				lastErr = err
			}
		}
	}

	return "", fmt.Errorf("could not get full stream after retries: %v", lastErr)
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
	// 1. Obtener información del track para saber su artista
	track, err := p.GetTrackInfo(ctx, trackID)
	if err != nil {
		return nil, err
	}

	artistID := track.Artist.ID
	if artistID == 0 && len(track.Artists) > 0 {
		artistID = track.Artists[0].ID
	}
	if artistID == 0 {
		return nil, fmt.Errorf("no artist found for track %d", trackID)
	}

	// 2. Obtener artistas similares
	similarArtists, err := p.GetSimilarArtists(ctx, artistID)
	if err != nil || len(similarArtists) == 0 {
		// Fallback: usar el mismo artista
		similarArtists = []TidalArtist{{ID: artistID}}
	}

	// 3. Recopilar top tracks de los artistas similares
	var allTracks []TidalTrack
	limitPerArtist := 5 // Obtener 5 tracks de unos 5 artistas = 25 tracks
	
	maxArtists := 6
	if len(similarArtists) > maxArtists {
		similarArtists = similarArtists[:maxArtists]
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	
	// Fetch concurrentemente
	for _, a := range similarArtists {
		wg.Add(1)
		go func(aID int) {
			defer wg.Done()
			page, err := p.GetArtistAlbums(ctx, aID, false)
			if err == nil && len(page.Tracks) > 0 {
				mu.Lock()
				count := limitPerArtist
				if len(page.Tracks) < count {
					count = len(page.Tracks)
				}
				allTracks = append(allTracks, page.Tracks[:count]...)
				mu.Unlock()
			}
		}(a.ID)
	}
	wg.Wait()

	if len(allTracks) == 0 {
		return nil, fmt.Errorf("no similar tracks found")
	}

	return allTracks, nil
}

func (p *Pool) GetTopTracks(ctx context.Context, limit int) ([]TidalTrack, error) {
	// Use a popular artist or a generic search if no direct top-tracks endpoint
	// Most proxies support /search/ with query if they don't have /top/
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
func parseManifestURL(mimeType, manifest string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(manifest)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(manifest)
		if err != nil {
			decoded = []byte(manifest)
		}
	}

	content := string(decoded)

	// DASH manifest: look for <BaseURL>
	if strings.Contains(content, "<BaseURL>") {
		start := strings.Index(content, "<BaseURL>") + len("<BaseURL>")
		end := strings.Index(content[start:], "</BaseURL>")
		if end > 0 {
			u := content[start : start+end]
			return strings.ReplaceAll(u, "&amp;", "&"), nil
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
					return strings.ReplaceAll(u, "&amp;", "&"), nil
				}
				return strings.ReplaceAll(line[start:], "&amp;", "&"), nil
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
				return manifestData.URL, nil
			}
			if len(manifestData.URLs) > 0 {
				return manifestData.URLs[0], nil
			}
		}
	}

	// M3U8: first line that starts with http
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http") {
			return line, nil
		}
	}

	return "", fmt.Errorf("could not extract URL from manifest (type: %s content preview: %.50s)", mimeType, content)
}
