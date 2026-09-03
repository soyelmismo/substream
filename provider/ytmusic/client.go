package ytmusic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	innertubeSearchURL = "https://music.youtube.com/youtubei/v1/search?alt=json"
	innertubePlayerURL = "https://music.youtube.com/youtubei/v1/player?alt=json"
	innertubeNextURL   = "https://music.youtube.com/youtubei/v1/next?alt=json"
	innertubeBrowseURL = "https://music.youtube.com/youtubei/v1/browse?alt=json"
	songsFilterParam   = "Eg-KAQwIABAAGAAgACgB" // Strict Songs filter in YouTube Music
	defaultUserAgent   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

	pythonWorkerScript = `
import sys, yt_dlp

ydl = yt_dlp.YoutubeDL({
    'quiet': True,
    'no_warnings': True,
    'format': 'ba/b',
    'extractor_args': {'youtube': {'player_client': ['android_music']}}
})
print('READY', flush=True)

for line in sys.stdin:
    vid = line.strip()
    if not vid: continue
    try:
        info = ydl.extract_info(f'https://www.youtube.com/watch?v={vid}', download=False)
        url = info.get('url') or ''
        print(f'OK {url}', flush=True)
    except Exception as e:
        print(f'ERR {e}', flush=True)
`
)

type cachedStream struct {
	url       string
	expiresAt time.Time
}

// Client interacts with YouTube Music via InnerTube and yt-dlp
type Client struct {
	httpClient *http.Client

	streamCache   map[string]cachedStream
	streamCacheMu sync.RWMutex

	trackCache  sync.Map // videoID -> *YTTrack
	albumCache  sync.Map // browseID -> *YTAlbum
	artistCache sync.Map // artistID -> *YTArtistDetail

	// In-flight request de-duplication
	inFlightMu sync.Mutex
	inFlight   map[string]chan struct{}

	// Persistent in-memory Python worker for ~1.8s stream resolution
	workerMu     sync.Mutex
	workerCmd    *exec.Cmd
	workerStdin  io.WriteCloser
	workerReader *bufio.Reader
}

// NewClient creates a new YouTube Music API client
func NewClient() *Client {
	c := &Client{
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		streamCache: make(map[string]cachedStream),
		inFlight:    make(map[string]chan struct{}),
	}
	// Warm up the resident python worker in background
	go c.warmupWorker()
	return c
}

func (c *Client) buildContextPayload() map[string]interface{} {
	return map[string]interface{}{
		"client": map[string]interface{}{
			"clientName":    "WEB_REMIX",
			"clientVersion": "1.20260401.01.00",
			"hl":            "en",
			"gl":            "US",
		},
	}
}

// SearchTracks searches YouTube Music strictly for official songs
func (c *Client) SearchTracks(ctx context.Context, query string, limit int) ([]*YTTrack, error) {
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}

	bodyData := map[string]interface{}{
		"context": c.buildContextPayload(),
		"query":   query,
	}
	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		return nil, fmt.Errorf("marshal search payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, innertubeSearchURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Origin", "https://music.youtube.com")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("innertube returned %d: %s", resp.StatusCode, string(b))
	}

	var parsed map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}

	return c.parseSearchSongs(parsed, limit), nil
}

func (c *Client) parseSearchSongs(data map[string]interface{}, limit int) []*YTTrack {
	var tracks []*YTTrack
	if limit <= 0 {
		limit = 25
	}

	contents, _ := data["contents"].(map[string]interface{})
	tabbed, _ := contents["tabbedSearchResultsRenderer"].(map[string]interface{})
	tabs, _ := tabbed["tabs"].([]interface{})
	if len(tabs) == 0 {
		return tracks
	}

	tab0, _ := tabs[0].(map[string]interface{})
	tabRenderer, _ := tab0["tabRenderer"].(map[string]interface{})
	tabContent, _ := tabRenderer["content"].(map[string]interface{})
	sectionList, _ := tabContent["sectionListRenderer"].(map[string]interface{})
	sectionContents, _ := sectionList["contents"].([]interface{})

	for _, sc := range sectionContents {
		secMap, _ := sc.(map[string]interface{})

		// 1. Check musicCardShelfRenderer (Top Result card)
		if card, ok := secMap["musicCardShelfRenderer"].(map[string]interface{}); ok {
			title := ""
			if tObj, ok := card["title"].(map[string]interface{}); ok {
				if runs, _ := tObj["runs"].([]interface{}); len(runs) > 0 {
					if r0, ok := runs[0].(map[string]interface{}); ok {
						title, _ = r0["text"].(string)
					}
				}
			}
			videoID := ""
			if onTap, ok := card["onTap"].(map[string]interface{}); ok {
				if w, ok := onTap["watchEndpoint"].(map[string]interface{}); ok {
					videoID, _ = w["videoId"].(string)
				}
			}
			artist := ""
			duration := 0
			if sub, ok := card["subtitle"].(map[string]interface{}); ok {
				if runs, _ := sub["runs"].([]interface{}); len(runs) > 0 {
					for _, r := range runs {
						if rm, ok := r.(map[string]interface{}); ok {
							txt, _ := rm["text"].(string)
							if strings.Contains(txt, ":") {
								duration = parseTimeString(txt)
							} else if txt != "•" && txt != "Song" && artist == "" {
								artist = strings.TrimSpace(txt)
							}
						}
					}
				}
			}
			if videoID != "" && title != "" {
				tracks = append(tracks, &YTTrack{
					VideoID:  videoID,
					Title:    title,
					Artist:   artist,
					Duration: duration,
				})
			}
		}

		// 2. Check itemSectionRenderer
		if isr, ok := secMap["itemSectionRenderer"].(map[string]interface{}); ok {
			items, _ := isr["contents"].([]interface{})
			for _, itm := range items {
				itmMap, _ := itm.(map[string]interface{})
				if flex, ok := itmMap["musicResponsiveListItemRenderer"].(map[string]interface{}); ok {
					track := c.parseFlexItem(flex)
					if track != nil && track.VideoID != "" {
						tracks = append(tracks, track)
						if len(tracks) >= limit {
							return tracks
						}
					}
				}
			}
		}

		// 3. Check musicShelfRenderer
		if msr, ok := secMap["musicShelfRenderer"].(map[string]interface{}); ok {
			items, _ := msr["contents"].([]interface{})
			for _, itm := range items {
				itmMap, _ := itm.(map[string]interface{})
				if flex, ok := itmMap["musicResponsiveListItemRenderer"].(map[string]interface{}); ok {
					track := c.parseFlexItem(flex)
					if track != nil && track.VideoID != "" {
						tracks = append(tracks, track)
						if len(tracks) >= limit {
							return tracks
						}
					}
				}
			}
		}
	}

	return tracks
}

func (c *Client) parseFlexItem(flex map[string]interface{}) *YTTrack {
	// Video ID from overlay or navigation
	overlay, _ := flex["overlay"].(map[string]interface{})
	thumbOverlay, _ := overlay["musicItemThumbnailOverlayRenderer"].(map[string]interface{})
	content, _ := thumbOverlay["content"].(map[string]interface{})
	btn, _ := content["musicPlayButtonRenderer"].(map[string]interface{})
	playNav, _ := btn["playNavigationEndpoint"].(map[string]interface{})
	watch, _ := playNav["watchEndpoint"].(map[string]interface{})
	videoID, _ := watch["videoId"].(string)

	flexCols, _ := flex["flexColumns"].([]interface{})
	if len(flexCols) < 2 {
		return nil
	}

	// Column 0: Title
	col0, _ := flexCols[0].(map[string]interface{})
	rend0, _ := col0["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{})
	text0, _ := rend0["text"].(map[string]interface{})
	runs0, _ := text0["runs"].([]interface{})
	title := ""
	if len(runs0) > 0 {
		r0, _ := runs0[0].(map[string]interface{})
		title, _ = r0["text"].(string)
		if videoID == "" {
			nav, _ := r0["navigationEndpoint"].(map[string]interface{})
			w, _ := nav["watchEndpoint"].(map[string]interface{})
			videoID, _ = w["videoId"].(string)
		}
	}

	if videoID == "" || title == "" {
		return nil
	}

	// Column 1: Artist, Album, Duration
	col1, _ := flexCols[1].(map[string]interface{})
	rend1, _ := col1["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{})
	text1, _ := rend1["text"].(map[string]interface{})
	runs1, _ := text1["runs"].([]interface{})

	artist := ""
	artistID := ""
	album := ""
	albumID := ""
	duration := 0

	for _, r := range runs1 {
		rm, _ := r.(map[string]interface{})
		t, _ := rm["text"].(string)
		nav, _ := rm["navigationEndpoint"].(map[string]interface{})
		browse, _ := nav["browseEndpoint"].(map[string]interface{})
		browseID, _ := browse["browseId"].(string)

		if strings.HasPrefix(browseID, "UC") || strings.HasPrefix(browseID, "FEmusic_library_privately_owned_artist") {
			if artist == "" {
				artist = t
				artistID = browseID
			}
		} else if strings.HasPrefix(browseID, "MPRE") || strings.HasPrefix(browseID, "OLAK") || strings.HasPrefix(browseID, "FEmusic_library_privately_owned_release") {
			if album == "" {
				album = t
				albumID = browseID
			}
		} else if strings.Contains(t, ":") {
			duration = parseTimeString(t)
		}
	}

	if artist == "" && len(runs1) > 0 {
		r0, _ := runs1[0].(map[string]interface{})
		artist, _ = r0["text"].(string)
	}

	// Thumbnail
	thumbObj, _ := flex["thumbnail"].(map[string]interface{})
	musicThumb, _ := thumbObj["musicThumbnailRenderer"].(map[string]interface{})
	thumbnail, _ := musicThumb["thumbnail"].(map[string]interface{})
	thumbs, _ := thumbnail["thumbnails"].([]interface{})
	bestThumb := ""
	if len(thumbs) > 0 {
		lastThumb, _ := thumbs[len(thumbs)-1].(map[string]interface{})
		bestThumb, _ = lastThumb["url"].(string)
	}

	return &YTTrack{
		VideoID:   videoID,
		Title:     title,
		Artist:    artist,
		ArtistID:  artistID,
		Album:     album,
		AlbumID:   albumID,
		Duration:  duration,
		Thumbnail: bestThumb,
	}
}

func parseTimeString(s string) int {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) == 2 {
		m, _ := strconv.Atoi(parts[0])
		sec, _ := strconv.Atoi(parts[1])
		return m*60 + sec
	} else if len(parts) == 3 {
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		sec, _ := strconv.Atoi(parts[2])
		return h*3600 + m*60 + sec
	}
	return 0
}

// GetCachedTrack returns cached track metadata if available
func (c *Client) GetCachedTrack(videoID string) (*YTTrack, bool) {
	if val, ok := c.trackCache.Load(videoID); ok {
		return val.(*YTTrack), true
	}
	return nil, false
}

// GetTrack fetches track metadata by video ID, enriching with album, artist, and year
func (c *Client) GetTrack(ctx context.Context, videoID string) (*YTTrack, error) {
	if videoID == "" {
		return nil, fmt.Errorf("empty videoId")
	}

	if val, ok := c.trackCache.Load(videoID); ok {
		return val.(*YTTrack), nil
	}

	// 1. Try next endpoint for rich metadata (artist, album, year, duration, cover)
	track := c.fetchTrackViaNext(ctx, videoID)
	if track != nil && track.Title != "" {
		c.trackCache.Store(videoID, track)
		return track, nil
	}

	// 2. Fallback to player endpoint
	track, err := c.fetchTrackViaPlayer(ctx, videoID)
	if err == nil && track != nil {
		c.trackCache.Store(videoID, track)
		return track, nil
	}

	return nil, err
}

func extractRuns(obj map[string]interface{}) string {
	if obj == nil {
		return ""
	}
	runs, ok := obj["runs"].([]interface{})
	if !ok {
		return ""
	}
	var sb strings.Builder
	for _, r := range runs {
		if rMap, ok := r.(map[string]interface{}); ok {
			if txt, ok := rMap["text"].(string); ok {
				sb.WriteString(txt)
			}
		}
	}
	return sb.String()
}

func (c *Client) fetchTrackViaNext(ctx context.Context, videoID string) *YTTrack {
	bodyData := map[string]interface{}{
		"context": c.buildContextPayload(),
		"videoId": videoID,
	}
	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, innertubeNextURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var parsed map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil
	}

	contents, _ := parsed["contents"].(map[string]interface{})
	singleCol, _ := contents["singleColumnMusicWatchNextResultsRenderer"].(map[string]interface{})
	tabbed, _ := singleCol["tabbedRenderer"].(map[string]interface{})
	watchNextTabbed, _ := tabbed["watchNextTabbedResultsRenderer"].(map[string]interface{})
	tabs, _ := watchNextTabbed["tabs"].([]interface{})
	if len(tabs) == 0 {
		return nil
	}

	tab0, _ := tabs[0].(map[string]interface{})
	tabRenderer, _ := tab0["tabRenderer"].(map[string]interface{})
	tabContent, _ := tabRenderer["content"].(map[string]interface{})
	musicQueue, _ := tabContent["musicQueueRenderer"].(map[string]interface{})
	queueContent, _ := musicQueue["content"].(map[string]interface{})
	playlistPanel, _ := queueContent["playlistPanelRenderer"].(map[string]interface{})
	panelContents, _ := playlistPanel["contents"].([]interface{})
	if len(panelContents) == 0 {
		return nil
	}

	first, _ := panelContents[0].(map[string]interface{})
	renderer, _ := first["playlistPanelVideoRenderer"].(map[string]interface{})
	if renderer == nil {
		return nil
	}

	titleObj, _ := renderer["title"].(map[string]interface{})
	title := extractRuns(titleObj)

	bylineObj, _ := renderer["longBylineText"].(map[string]interface{})
	bylineRuns, _ := bylineObj["runs"].([]interface{})
	var bylineTexts []string
	for _, r := range bylineRuns {
		if rMap, ok := r.(map[string]interface{}); ok {
			if txt, ok := rMap["text"].(string); ok && txt != " • " && strings.TrimSpace(txt) != "" {
				bylineTexts = append(bylineTexts, txt)
			}
		}
	}

	isStatsText := func(s string) bool {
		lower := strings.ToLower(s)
		return strings.Contains(lower, "view") ||
			strings.Contains(lower, "visualiz") ||
			strings.Contains(lower, "reprod") ||
			strings.Contains(lower, "like") ||
			strings.Contains(lower, "vue") ||
			strings.Contains(lower, "aufruf") ||
			strings.Contains(lower, "suscriptor") ||
			strings.Contains(lower, "subscriber")
	}

	artist := ""
	artistID := ""
	album := ""
	albumID := ""
	year := 0

	for _, r := range bylineRuns {
		if rMap, ok := r.(map[string]interface{}); ok {
			txt, _ := rMap["text"].(string)
			txt = strings.TrimSpace(txt)
			if txt == "" || txt == "•" {
				continue
			}
			nav, _ := rMap["navigationEndpoint"].(map[string]interface{})
			browse, _ := nav["browseEndpoint"].(map[string]interface{})
			browseID, _ := browse["browseId"].(string)
			pageConfig, _ := browse["browseEndpointContextSupportedConfigs"].(map[string]interface{})
			musicConfig, _ := pageConfig["browseEndpointContextMusicConfig"].(map[string]interface{})
			pageType, _ := musicConfig["pageType"].(string)

			if pageType == "MUSIC_PAGE_TYPE_ARTIST" || strings.HasPrefix(browseID, "UC") {
				if artist == "" {
					artist = txt
					artistID = browseID
				}
			} else if pageType == "MUSIC_PAGE_TYPE_ALBUM" || strings.HasPrefix(browseID, "MPRE") || strings.HasPrefix(browseID, "OLAK") {
				if album == "" {
					album = txt
					albumID = browseID
				}
			} else if len(txt) == 4 {
				if y, err := strconv.Atoi(txt); err == nil && y > 1900 && y < 2100 {
					year = y
				}
			}
		}
	}

	// Fallback to bylineTexts if not resolved via navigation endpoints
	if artist == "" && len(bylineTexts) > 0 {
		artist = bylineTexts[0]
	}
	if album == "" && len(bylineTexts) > 1 && !isStatsText(bylineTexts[1]) {
		album = bylineTexts[1]
	}
	if year == 0 && len(bylineTexts) > 2 && !isStatsText(bylineTexts[2]) {
		y, _ := strconv.Atoi(bylineTexts[2])
		year = y
	}

	// Thumbnails
	bestThumb := ""
	if thumbObj, ok := renderer["thumbnail"].(map[string]interface{}); ok {
		if thumbs, ok := thumbObj["thumbnails"].([]interface{}); ok && len(thumbs) > 0 {
			if last, ok := thumbs[len(thumbs)-1].(map[string]interface{}); ok {
				if u, ok := last["url"].(string); ok {
					bestThumb = u
				}
			}
		}
	}

	// Duration
	duration := 0
	if lenObj, ok := renderer["lengthText"].(map[string]interface{}); ok {
		if durStr := extractRuns(lenObj); durStr != "" {
			duration = parseTimeString(durStr)
		}
	}

	if title == "" {
		return nil
	}

	return &YTTrack{
		VideoID:   videoID,
		Title:     title,
		Artist:    artist,
		ArtistID:  artistID,
		Album:     album,
		AlbumID:   albumID,
		Year:      year,
		Duration:  duration,
		Thumbnail: bestThumb,
	}
}

func (c *Client) fetchTrackViaPlayer(ctx context.Context, videoID string) (*YTTrack, error) {
	bodyData := map[string]interface{}{
		"context": c.buildContextPayload(),
		"videoId": videoID,
	}
	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, innertubePlayerURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var parsed struct {
		VideoDetails struct {
			VideoID       string `json:"videoId"`
			Title         string `json:"title"`
			Author        string `json:"author"`
			ChannelID     string `json:"channelId"`
			LengthSeconds string `json:"lengthSeconds"`
			Thumbnail     struct {
				Thumbnails []struct {
					URL string `json:"url"`
				} `json:"thumbnails"`
			} `json:"thumbnail"`
		} `json:"videoDetails"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode player info: %w", err)
	}

	dur, _ := strconv.Atoi(parsed.VideoDetails.LengthSeconds)
	bestThumb := ""
	if len(parsed.VideoDetails.Thumbnail.Thumbnails) > 0 {
		bestThumb = parsed.VideoDetails.Thumbnail.Thumbnails[len(parsed.VideoDetails.Thumbnail.Thumbnails)-1].URL
	}

	return &YTTrack{
		VideoID:   parsed.VideoDetails.VideoID,
		Title:     parsed.VideoDetails.Title,
		Artist:    parsed.VideoDetails.Author,
		ArtistID:  parsed.VideoDetails.ChannelID,
		Duration:  dur,
		Thumbnail: bestThumb,
	}, nil
}

func (c *Client) warmupWorker() {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()
	if err := c.ensureWorkerLocked(); err != nil {
		log.Printf("[YTMUSIC] Note: Worker warmup failed (%v), will retry on demand", err)
	}
}

func (c *Client) ensureWorkerLocked() error {
	if c.workerCmd != nil && c.workerCmd.Process != nil && c.workerStdin != nil {
		return nil
	}

	cmd := exec.Command("python3", "-u", "-c", pythonWorkerScript)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("worker stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("worker stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start worker process: %w", err)
	}

	reader := bufio.NewReader(stdout)
	readyChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		line, err := reader.ReadString('\n')
		if err != nil {
			errChan <- err
			return
		}
		readyChan <- line
	}()

	select {
	case line := <-readyChan:
		if !strings.Contains(line, "READY") {
			_ = cmd.Process.Kill()
			return fmt.Errorf("unexpected worker greeting: %s", line)
		}
	case err := <-errChan:
		_ = cmd.Process.Kill()
		return fmt.Errorf("worker init read error: %w", err)
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		return fmt.Errorf("worker startup timed out")
	}

	c.workerCmd = cmd
	c.workerStdin = stdin
	c.workerReader = reader
	log.Printf("[YTMUSIC] 🚀 Persistent Python extraction worker initialized (resident in RAM)")
	return nil
}

func (c *Client) resolveViaWorker(videoID string) (string, error) {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()

	if err := c.ensureWorkerLocked(); err != nil {
		return "", err
	}

	if _, err := io.WriteString(c.workerStdin, videoID+"\n"); err != nil {
		if c.workerCmd != nil && c.workerCmd.Process != nil {
			_ = c.workerCmd.Process.Kill()
		}
		c.workerCmd = nil
		c.workerStdin = nil
		c.workerReader = nil
		return "", fmt.Errorf("worker stdin write: %w", err)
	}

	lineChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		line, err := c.workerReader.ReadString('\n')
		if err != nil {
			errChan <- err
			return
		}
		lineChan <- line
	}()

	select {
	case line := <-lineChan:
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "OK ") {
			url := strings.TrimSpace(strings.TrimPrefix(line, "OK "))
			if url != "" {
				return url, nil
			}
			return "", fmt.Errorf("worker returned empty URL")
		}
		return "", fmt.Errorf("worker extraction error: %s", line)
	case err := <-errChan:
		if c.workerCmd != nil && c.workerCmd.Process != nil {
			_ = c.workerCmd.Process.Kill()
		}
		c.workerCmd = nil
		c.workerStdin = nil
		c.workerReader = nil
		return "", fmt.Errorf("worker stdout read: %w", err)
	case <-time.After(8 * time.Second):
		if c.workerCmd != nil && c.workerCmd.Process != nil {
			_ = c.workerCmd.Process.Kill()
		}
		c.workerCmd = nil
		c.workerStdin = nil
		c.workerReader = nil
		return "", fmt.Errorf("worker extraction timed out for %s", videoID)
	}
}

func (c *Client) resolveViaCLI(ctx context.Context, videoID string) (string, error) {
	targetURL := fmt.Sprintf("https://www.youtube.com/watch?v=%s", videoID)
	cmd := exec.CommandContext(ctx, "yt-dlp",
		"--no-warnings",
		"--no-playlist",
		"--extractor-args", "youtube:player_client=android_music",
		"-f", "ba/b",
		"-g", targetURL,
	)

	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("yt-dlp cli failed: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	streamURL := strings.TrimSpace(lines[0])
	if streamURL == "" {
		return "", fmt.Errorf("empty stream url returned by yt-dlp")
	}
	return streamURL, nil
}

// GetStreamURL resolves the direct audio stream URL using persistent worker or CLI fallback
func (c *Client) GetStreamURL(ctx context.Context, videoID string) (string, error) {
	// 1. Fast path: check in-memory cache
	c.streamCacheMu.RLock()
	if entry, ok := c.streamCache[videoID]; ok && time.Now().Before(entry.expiresAt) {
		c.streamCacheMu.RUnlock()
		return entry.url, nil
	}
	c.streamCacheMu.RUnlock()

	// 2. Singleflight: de-duplicate simultaneous requests for the same videoID
	c.inFlightMu.Lock()
	if ch, running := c.inFlight[videoID]; running {
		c.inFlightMu.Unlock()
		select {
		case <-ch:
			c.streamCacheMu.RLock()
			entry, ok := c.streamCache[videoID]
			c.streamCacheMu.RUnlock()
			if ok && time.Now().Before(entry.expiresAt) {
				return entry.url, nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	doneCh := make(chan struct{})
	c.inFlight[videoID] = doneCh
	c.inFlightMu.Unlock()

	defer func() {
		c.inFlightMu.Lock()
		delete(c.inFlight, videoID)
		close(doneCh)
		c.inFlightMu.Unlock()
	}()

	// 3. Try fast persistent resident worker (~1.8s)
	t0 := time.Now()
	streamURL, err := c.resolveViaWorker(videoID)
	if err != nil || streamURL == "" {
		log.Printf("[YTMUSIC] ⚠️ Worker failed (%v), falling back to optimized CLI...", err)
		streamURL, err = c.resolveViaCLI(ctx, videoID)
		if err != nil {
			return "", fmt.Errorf("stream resolution failed for %s: %w", videoID, err)
		}
	}

	dur := time.Since(t0)
	log.Printf("[YTMUSIC] ✅ Resolved direct stream for track %s in %v", videoID, dur)

	c.streamCacheMu.Lock()
	c.streamCache[videoID] = cachedStream{
		url:       streamURL,
		expiresAt: time.Now().Add(1 * time.Hour),
	}
	c.streamCacheMu.Unlock()

	return streamURL, nil
}

// GetCachedAlbum returns cached album metadata if available
func (c *Client) GetCachedAlbum(browseID string) (*YTAlbum, bool) {
	if val, ok := c.albumCache.Load(browseID); ok {
		return val.(*YTAlbum), true
	}
	return nil, false
}

// GetAlbum fetches album metadata and its tracks from YouTube Music via InnerTube browse endpoint
func (c *Client) GetAlbum(ctx context.Context, browseID string) (*YTAlbum, []*YTTrack, error) {
	if browseID == "" {
		return nil, nil, fmt.Errorf("empty browseId")
	}

	bodyData := map[string]interface{}{
		"context":  c.buildContextPayload(),
		"browseId": browseID,
	}
	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal album payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, innertubeBrowseURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("build album request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Origin", "https://music.youtube.com")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("execute album browse: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("innertube browse returned %d: %s", resp.StatusCode, string(b))
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, nil, fmt.Errorf("decode album response: %w", err)
	}

	album := &YTAlbum{ID: browseID}
	var tracks []*YTTrack

	// 1. Find musicResponsiveHeaderRenderer for album header
	var findHeader func(v interface{}) map[string]interface{}
	findHeader = func(v interface{}) map[string]interface{} {
		m, ok := v.(map[string]interface{})
		if !ok {
			if arr, ok := v.([]interface{}); ok {
				for _, item := range arr {
					if res := findHeader(item); res != nil {
						return res
					}
				}
			}
			return nil
		}
		if h, ok := m["musicResponsiveHeaderRenderer"].(map[string]interface{}); ok {
			return h
		}
		for _, val := range m {
			if res := findHeader(val); res != nil {
				return res
			}
		}
		return nil
	}

	if header := findHeader(data); header != nil {
		if tObj, _ := header["title"].(map[string]interface{}); tObj != nil {
			if runs, _ := tObj["runs"].([]interface{}); len(runs) > 0 {
				if r0, _ := runs[0].(map[string]interface{}); r0 != nil {
					album.Title, _ = r0["text"].(string)
				}
			}
		}
		if strapline, _ := header["straplineTextOne"].(map[string]interface{}); strapline != nil {
			if runs, _ := strapline["runs"].([]interface{}); len(runs) > 0 {
				if r0, _ := runs[0].(map[string]interface{}); r0 != nil {
					album.Artist, _ = r0["text"].(string)
					if nav, _ := r0["navigationEndpoint"].(map[string]interface{}); nav != nil {
						if bEnd, _ := nav["browseEndpoint"].(map[string]interface{}); bEnd != nil {
							album.ArtistID, _ = bEnd["browseId"].(string)
						}
					}
				}
			}
		}
		if sub, _ := header["subtitle"].(map[string]interface{}); sub != nil {
			if runs, _ := sub["runs"].([]interface{}); len(runs) > 0 {
				for _, r := range runs {
					if rm, _ := r.(map[string]interface{}); rm != nil {
						txt, _ := rm["text"].(string)
						txt = strings.TrimSpace(txt)
						if len(txt) == 4 {
							if y, err := strconv.Atoi(txt); err == nil && y > 1900 && y < 2100 {
								album.Year = y
							}
						}
					}
				}
			}
		}
		if thumbObj, _ := header["thumbnail"].(map[string]interface{}); thumbObj != nil {
			if mThumb, _ := thumbObj["musicThumbnailRenderer"].(map[string]interface{}); mThumb != nil {
				if thumb, _ := mThumb["thumbnail"].(map[string]interface{}); thumb != nil {
					if thumbs, _ := thumb["thumbnails"].([]interface{}); len(thumbs) > 0 {
						if last, _ := thumbs[len(thumbs)-1].(map[string]interface{}); last != nil {
							album.Thumbnail, _ = last["url"].(string)
						}
					}
				}
			}
		}
	}

	// 2. Find all shelves (musicShelfRenderer or musicPlaylistShelfRenderer)
	var findShelves func(v interface{}) []map[string]interface{}
	findShelves = func(v interface{}) []map[string]interface{} {
		var res []map[string]interface{}
		m, ok := v.(map[string]interface{})
		if !ok {
			if arr, ok := v.([]interface{}); ok {
				for _, item := range arr {
					res = append(res, findShelves(item)...)
				}
			}
			return res
		}
		if shelf, ok := m["musicShelfRenderer"].(map[string]interface{}); ok {
			res = append(res, shelf)
		}
		if playlistShelf, ok := m["musicPlaylistShelfRenderer"].(map[string]interface{}); ok {
			res = append(res, playlistShelf)
		}
		for _, val := range m {
			res = append(res, findShelves(val)...)
		}
		return res
	}

	shelves := findShelves(data)
	trackIdx := 1
	for _, shelf := range shelves {
		contents, _ := shelf["contents"].([]interface{})
		for _, item := range contents {
			itmMap, _ := item.(map[string]interface{})
			flex, ok := itmMap["musicResponsiveListItemRenderer"].(map[string]interface{})
			if !ok {
				continue
			}

			// VideoID
			videoID := ""
			if plItem, ok := flex["playlistItemData"].(map[string]interface{}); ok {
				videoID, _ = plItem["videoId"].(string)
			}
			if videoID == "" {
				if overlay, _ := flex["overlay"].(map[string]interface{}); overlay != nil {
					if thumbOverlay, _ := overlay["musicItemThumbnailOverlayRenderer"].(map[string]interface{}); thumbOverlay != nil {
						if content, _ := thumbOverlay["content"].(map[string]interface{}); content != nil {
							if btn, _ := content["musicPlayButtonRenderer"].(map[string]interface{}); btn != nil {
								if playNav, _ := btn["playNavigationEndpoint"].(map[string]interface{}); playNav != nil {
									if watch, _ := playNav["watchEndpoint"].(map[string]interface{}); watch != nil {
										videoID, _ = watch["videoId"].(string)
									}
								}
							}
						}
					}
				}
			}

			// Title
			title := ""
			flexCols, _ := flex["flexColumns"].([]interface{})
			if len(flexCols) > 0 {
				if col0, _ := flexCols[0].(map[string]interface{}); col0 != nil {
					if rend0, _ := col0["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{}); rend0 != nil {
						if text0, _ := rend0["text"].(map[string]interface{}); text0 != nil {
							if runs0, _ := text0["runs"].([]interface{}); len(runs0) > 0 {
								if r0, _ := runs0[0].(map[string]interface{}); r0 != nil {
									title, _ = r0["text"].(string)
								}
							}
						}
					}
				}
			}

			if title == "" || videoID == "" {
				continue
			}

			// Track index / number
			trNum := trackIdx
			if idxObj, _ := flex["index"].(map[string]interface{}); idxObj != nil {
				if runs, _ := idxObj["runs"].([]interface{}); len(runs) > 0 {
					if r0, _ := runs[0].(map[string]interface{}); r0 != nil {
						if numStr, _ := r0["text"].(string); numStr != "" {
							if n, err := strconv.Atoi(numStr); err == nil && n > 0 {
								trNum = n
							}
						}
					}
				}
			}

			// Duration
			duration := 0
			fixedCols, _ := flex["fixedColumns"].([]interface{})
			if len(fixedCols) > 0 {
				if col0, _ := fixedCols[0].(map[string]interface{}); col0 != nil {
					if rend0, _ := col0["musicResponsiveListItemFixedColumnRenderer"].(map[string]interface{}); rend0 != nil {
						if text0, _ := rend0["text"].(map[string]interface{}); text0 != nil {
							if runs0, _ := text0["runs"].([]interface{}); len(runs0) > 0 {
								if r0, _ := runs0[0].(map[string]interface{}); r0 != nil {
									if durStr, _ := r0["text"].(string); durStr != "" {
										duration = parseTimeString(durStr)
									}
								}
							}
						}
					}
				}
			}

			// Artist if different or present in flexCols[1]
			trackArtist := album.Artist
			trackArtistID := album.ArtistID
			if len(flexCols) > 1 {
				if col1, _ := flexCols[1].(map[string]interface{}); col1 != nil {
					if rend1, _ := col1["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{}); rend1 != nil {
						if text1, _ := rend1["text"].(map[string]interface{}); text1 != nil {
							if runs1, _ := text1["runs"].([]interface{}); len(runs1) > 0 {
								for _, r := range runs1 {
									if rm, _ := r.(map[string]interface{}); rm != nil {
										txt, _ := rm["text"].(string)
										txt = strings.TrimSpace(txt)
										if txt != "" && txt != "•" {
											if nav, _ := rm["navigationEndpoint"].(map[string]interface{}); nav != nil {
												if bEnd, _ := nav["browseEndpoint"].(map[string]interface{}); bEnd != nil {
													bID, _ := bEnd["browseId"].(string)
													if strings.HasPrefix(bID, "UC") {
														trackArtist = txt
														trackArtistID = bID
														break
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			track := &YTTrack{
				VideoID:     videoID,
				Title:       title,
				Artist:      trackArtist,
				ArtistID:    trackArtistID,
				Album:       album.Title,
				AlbumID:     album.ID,
				Year:        album.Year,
				Duration:    duration,
				Thumbnail:   album.Thumbnail,
				TrackNumber: trNum,
			}

			// Cache track for instant lookup
			c.trackCache.Store(videoID, track)

			tracks = append(tracks, track)
			trackIdx++
		}
	}

	if album.Title != "" {
		c.albumCache.Store(browseID, album)
	}

	return album, tracks, nil
}

// GetCachedArtist returns cached artist metadata if available
func (c *Client) GetCachedArtist(artistID string) (*YTArtist, bool) {
	if val, ok := c.artistCache.Load(artistID); ok {
		detail := val.(*YTArtistDetail)
		if detail != nil && detail.Artist != nil {
			return detail.Artist, true
		}
	}
	return nil, false
}

// ResolveArtistID resolves an artist name or query to a YouTube Music channel ID (e.g. UC...)
func (c *Client) ResolveArtistID(ctx context.Context, nameOrID string) (string, error) {
	if strings.HasPrefix(nameOrID, "UC") || strings.HasPrefix(nameOrID, "FEmusic") {
		return nameOrID, nil
	}
	if strings.TrimSpace(nameOrID) == "" {
		return "", fmt.Errorf("empty artist query")
	}

	bodyData := map[string]interface{}{
		"context": c.buildContextPayload(),
		"query":   nameOrID,
	}
	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, innertubeSearchURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Origin", "https://music.youtube.com")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}

	var findArtistBrowseID func(v interface{}) string
	findArtistBrowseID = func(v interface{}) string {
		m, ok := v.(map[string]interface{})
		if !ok {
			if arr, ok := v.([]interface{}); ok {
				for _, item := range arr {
					if id := findArtistBrowseID(item); id != "" {
						return id
					}
				}
			}
			return ""
		}
		if bEnd, ok := m["browseEndpoint"].(map[string]interface{}); ok {
			if bID, ok := bEnd["browseId"].(string); ok && strings.HasPrefix(bID, "UC") {
				if cfg, ok := bEnd["browseEndpointContextSupportedConfigs"].(map[string]interface{}); ok {
					if mCfg, ok := cfg["browseEndpointContextMusicConfig"].(map[string]interface{}); ok {
						if pType, ok := mCfg["pageType"].(string); ok && pType == "MUSIC_PAGE_TYPE_ARTIST" {
							return bID
						}
					}
				}
			}
		}
		for _, val := range m {
			if id := findArtistBrowseID(val); id != "" {
				return id
			}
		}
		return ""
	}

	if id := findArtistBrowseID(data); id != "" {
		return id, nil
	}
	return "", fmt.Errorf("artist ID not found for query %q", nameOrID)
}

// GetArtist fetches full artist details including discography, top songs, and similar artists
func (c *Client) GetArtist(ctx context.Context, nameOrID string) (*YTArtistDetail, error) {
	if strings.TrimSpace(nameOrID) == "" {
		return nil, fmt.Errorf("empty artist ID")
	}

	artistID := nameOrID
	if !strings.HasPrefix(artistID, "UC") && !strings.HasPrefix(artistID, "FEmusic") {
		if resolved, err := c.ResolveArtistID(ctx, nameOrID); err == nil && resolved != "" {
			artistID = resolved
		}
	}

	// Fast path: check in-memory cache
	if val, ok := c.artistCache.Load(artistID); ok {
		return val.(*YTArtistDetail), nil
	}
	if val, ok := c.artistCache.Load(nameOrID); ok {
		return val.(*YTArtistDetail), nil
	}

	bodyData := map[string]interface{}{
		"context":  c.buildContextPayload(),
		"browseId": artistID,
	}
	bodyBytes, err := json.Marshal(bodyData)
	if err != nil {
		return nil, fmt.Errorf("marshal artist payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, innertubeBrowseURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build artist request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Origin", "https://music.youtube.com")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute artist browse: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("innertube artist browse returned %d: %s", resp.StatusCode, string(b))
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("decode artist response: %w", err)
	}

	detail := &YTArtistDetail{
		Artist: &YTArtist{ID: artistID},
	}

	// 1. Parse artist header
	var findHeader func(v interface{}) map[string]interface{}
	findHeader = func(v interface{}) map[string]interface{} {
		m, ok := v.(map[string]interface{})
		if !ok {
			if arr, ok := v.([]interface{}); ok {
				for _, item := range arr {
					if res := findHeader(item); res != nil {
						return res
					}
				}
			}
			return nil
		}
		if h, ok := m["musicImmersiveHeaderRenderer"].(map[string]interface{}); ok {
			return h
		}
		if h, ok := m["musicVisualHeaderRenderer"].(map[string]interface{}); ok {
			return h
		}
		if h, ok := m["musicHeaderRenderer"].(map[string]interface{}); ok {
			return h
		}
		for _, val := range m {
			if res := findHeader(val); res != nil {
				return res
			}
		}
		return nil
	}

	if header := findHeader(data); header != nil {
		if tObj, _ := header["title"].(map[string]interface{}); tObj != nil {
			if runs, _ := tObj["runs"].([]interface{}); len(runs) > 0 {
				if r0, _ := runs[0].(map[string]interface{}); r0 != nil {
					detail.Artist.Name, _ = r0["text"].(string)
				}
			}
		}
		if descObj, _ := header["description"].(map[string]interface{}); descObj != nil {
			if runs, _ := descObj["runs"].([]interface{}); len(runs) > 0 {
				var sb strings.Builder
				for _, r := range runs {
					if rm, _ := r.(map[string]interface{}); rm != nil {
						if txt, _ := rm["text"].(string); txt != "" {
							sb.WriteString(txt)
						}
					}
				}
				detail.Artist.Description = sb.String()
			}
		}
		if thumbObj, _ := header["thumbnail"].(map[string]interface{}); thumbObj != nil {
			if mThumb, _ := thumbObj["musicThumbnailRenderer"].(map[string]interface{}); mThumb != nil {
				if thumb, _ := mThumb["thumbnail"].(map[string]interface{}); thumb != nil {
					if thumbs, _ := thumb["thumbnails"].([]interface{}); len(thumbs) > 0 {
						if last, _ := thumbs[len(thumbs)-1].(map[string]interface{}); last != nil {
							detail.Artist.Thumbnail, _ = last["url"].(string)
						}
					}
				}
			}
		}
	}

	// If artist name wasn't in header, fallback to query
	if detail.Artist.Name == "" {
		detail.Artist.Name = nameOrID
	}

	// 2. Scan sections for Top songs, Albums, Singles & EPs, Fans might also like
	contents, _ := data["contents"].(map[string]interface{})
	singleCol, _ := contents["singleColumnBrowseResultsRenderer"].(map[string]interface{})
	tabs, _ := singleCol["tabs"].([]interface{})
	var sectionContents []interface{}
	if len(tabs) > 0 {
		tab0, _ := tabs[0].(map[string]interface{})
		tabRend, _ := tab0["tabRenderer"].(map[string]interface{})
		tabContent, _ := tabRend["content"].(map[string]interface{})
		secList, _ := tabContent["sectionListRenderer"].(map[string]interface{})
		sectionContents, _ = secList["contents"].([]interface{})
	}

	for _, sec := range sectionContents {
		secMap, ok := sec.(map[string]interface{})
		if !ok {
			continue
		}

		// A. Check musicShelfRenderer (Top songs)
		if shelf, ok := secMap["musicShelfRenderer"].(map[string]interface{}); ok {
			title := ""
			if tObj, _ := shelf["title"].(map[string]interface{}); tObj != nil {
				if runs, _ := tObj["runs"].([]interface{}); len(runs) > 0 {
					if r0, _ := runs[0].(map[string]interface{}); r0 != nil {
						title, _ = r0["text"].(string)
					}
				}
			}
			if strings.EqualFold(title, "Top songs") || strings.EqualFold(title, "Songs") {
				items, _ := shelf["contents"].([]interface{})
				for trIdx, item := range items {
					itmMap, _ := item.(map[string]interface{})
					flex, ok := itmMap["musicResponsiveListItemRenderer"].(map[string]interface{})
					if !ok {
						continue
					}
					videoID := ""
					if plItem, _ := flex["playlistItemData"].(map[string]interface{}); plItem != nil {
						videoID, _ = plItem["videoId"].(string)
					}
					songTitle := ""
					flexCols, _ := flex["flexColumns"].([]interface{})
					if len(flexCols) > 0 {
						if col0, _ := flexCols[0].(map[string]interface{}); col0 != nil {
							if rend0, _ := col0["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{}); rend0 != nil {
								if text0, _ := rend0["text"].(map[string]interface{}); text0 != nil {
									if runs0, _ := text0["runs"].([]interface{}); len(runs0) > 0 {
										if r0, _ := runs0[0].(map[string]interface{}); r0 != nil {
											songTitle, _ = r0["text"].(string)
											if videoID == "" {
												nav, _ := r0["navigationEndpoint"].(map[string]interface{})
												watch, _ := nav["watchEndpoint"].(map[string]interface{})
												videoID, _ = watch["videoId"].(string)
											}
										}
									}
								}
							}
						}
					}
					albumTitle := ""
					albumID := ""
					duration := 0
					if len(flexCols) > 1 {
						if col1, _ := flexCols[1].(map[string]interface{}); col1 != nil {
							if rend1, _ := col1["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{}); rend1 != nil {
								if text1, _ := rend1["text"].(map[string]interface{}); text1 != nil {
									if runs1, _ := text1["runs"].([]interface{}); len(runs1) > 0 {
										for _, r := range runs1 {
											if rm, _ := r.(map[string]interface{}); rm != nil {
												txt, _ := rm["text"].(string)
												nav, _ := rm["navigationEndpoint"].(map[string]interface{})
												browse, _ := nav["browseEndpoint"].(map[string]interface{})
												bID, _ := browse["browseId"].(string)
												if strings.HasPrefix(bID, "MPRE") || strings.HasPrefix(bID, "OLAK") {
													albumTitle = txt
													albumID = bID
												} else if strings.Contains(txt, ":") {
													duration = parseTimeString(txt)
												}
											}
										}
									}
								}
							}
						}
					}
					if duration == 0 {
						fixedCols, _ := flex["fixedColumns"].([]interface{})
						if len(fixedCols) > 0 {
							if col0, _ := fixedCols[0].(map[string]interface{}); col0 != nil {
								if rend0, _ := col0["musicResponsiveListItemFixedColumnRenderer"].(map[string]interface{}); rend0 != nil {
									if text0, _ := rend0["text"].(map[string]interface{}); text0 != nil {
										if runs0, _ := text0["runs"].([]interface{}); len(runs0) > 0 {
											if r0, _ := runs0[0].(map[string]interface{}); r0 != nil {
												if durStr, _ := r0["text"].(string); durStr != "" {
													duration = parseTimeString(durStr)
												}
											}
										}
									}
								}
							}
						}
					}

					thumb := ""
					if thumbObj, _ := flex["thumbnail"].(map[string]interface{}); thumbObj != nil {
						if mThumb, _ := thumbObj["musicThumbnailRenderer"].(map[string]interface{}); mThumb != nil {
							if thumbnail, _ := mThumb["thumbnail"].(map[string]interface{}); thumbnail != nil {
								if thumbs, _ := thumbnail["thumbnails"].([]interface{}); len(thumbs) > 0 {
									if last, _ := thumbs[len(thumbs)-1].(map[string]interface{}); last != nil {
										thumb, _ = last["url"].(string)
									}
								}
							}
						}
					}

					if songTitle != "" && videoID != "" {
						tr := &YTTrack{
							VideoID:     videoID,
							Title:       songTitle,
							Artist:      detail.Artist.Name,
							ArtistID:    detail.Artist.ID,
							Album:       albumTitle,
							AlbumID:     albumID,
							Duration:    duration,
							Thumbnail:   thumb,
							TrackNumber: trIdx + 1,
						}
						c.trackCache.Store(videoID, tr)
						detail.TopTracks = append(detail.TopTracks, tr)
					}
				}
			}
		}

		// B. Check musicCarouselShelfRenderer (Albums, Singles & EPs, Fans might also like)
		if carousel, ok := secMap["musicCarouselShelfRenderer"].(map[string]interface{}); ok {
			headerObj, _ := carousel["header"].(map[string]interface{})
			basicHeader, _ := headerObj["musicCarouselShelfBasicHeaderRenderer"].(map[string]interface{})
			cTitleObj, _ := basicHeader["title"].(map[string]interface{})
			runs, _ := cTitleObj["runs"].([]interface{})
			cTitle := ""
			if len(runs) > 0 {
				if r0, _ := runs[0].(map[string]interface{}); r0 != nil {
					cTitle, _ = r0["text"].(string)
				}
			}

			items, _ := carousel["contents"].([]interface{})
			for _, itm := range items {
				itmMap, _ := itm.(map[string]interface{})
				twoRow, ok := itmMap["musicTwoRowItemRenderer"].(map[string]interface{})
				if !ok {
					continue
				}

				rowTitle := ""
				rowBrowseID := ""
				if tObj, _ := twoRow["title"].(map[string]interface{}); tObj != nil {
					if tRuns, _ := tObj["runs"].([]interface{}); len(tRuns) > 0 {
						if r0, _ := tRuns[0].(map[string]interface{}); r0 != nil {
							rowTitle, _ = r0["text"].(string)
							if nav, _ := r0["navigationEndpoint"].(map[string]interface{}); nav != nil {
								if bEnd, _ := nav["browseEndpoint"].(map[string]interface{}); bEnd != nil {
									rowBrowseID, _ = bEnd["browseId"].(string)
								}
							}
						}
					}
				}

				rowThumb := ""
				if thumbRend, _ := twoRow["thumbnailRenderer"].(map[string]interface{}); thumbRend != nil {
					if mThumb, _ := thumbRend["musicThumbnailRenderer"].(map[string]interface{}); mThumb != nil {
						if thumbnail, _ := mThumb["thumbnail"].(map[string]interface{}); thumbnail != nil {
							if thumbs, _ := thumbnail["thumbnails"].([]interface{}); len(thumbs) > 0 {
								if last, _ := thumbs[len(thumbs)-1].(map[string]interface{}); last != nil {
									rowThumb, _ = last["url"].(string)
								}
							}
						}
					}
				}

				rowYear := 0
				if subObj, _ := twoRow["subtitle"].(map[string]interface{}); subObj != nil {
					if sRuns, _ := subObj["runs"].([]interface{}); len(sRuns) > 0 {
						for _, r := range sRuns {
							if rm, _ := r.(map[string]interface{}); rm != nil {
								txt, _ := rm["text"].(string)
								txt = strings.TrimSpace(txt)
								if len(txt) == 4 {
									if y, err := strconv.Atoi(txt); err == nil && y > 1900 && y < 2100 {
										rowYear = y
										break
									}
								}
							}
						}
					}
				}

				if strings.Contains(cTitle, "Albums") {
					alb := &YTAlbum{
						ID:        rowBrowseID,
						Title:     rowTitle,
						Artist:    detail.Artist.Name,
						ArtistID:  detail.Artist.ID,
						Year:      rowYear,
						Thumbnail: rowThumb,
					}
					c.albumCache.Store(rowBrowseID, alb)
					detail.Albums = append(detail.Albums, alb)
				} else if strings.Contains(cTitle, "Singles") || strings.Contains(cTitle, "EPs") {
					alb := &YTAlbum{
						ID:        rowBrowseID,
						Title:     rowTitle,
						Artist:    detail.Artist.Name,
						ArtistID:  detail.Artist.ID,
						Year:      rowYear,
						Thumbnail: rowThumb,
					}
					c.albumCache.Store(rowBrowseID, alb)
					detail.Singles = append(detail.Singles, alb)
				} else if strings.Contains(cTitle, "Fans might also like") || strings.Contains(cTitle, "Similar") {
					detail.SimilarArtists = append(detail.SimilarArtists, &YTArtist{
						ID:        rowBrowseID,
						Name:      rowTitle,
						Thumbnail: rowThumb,
					})
				}
			}
		}
	}

	if detail.Artist.Name != "" {
		c.artistCache.Store(artistID, detail)
		if nameOrID != artistID {
			c.artistCache.Store(nameOrID, detail)
		}
	}

	return detail, nil
}


