package ytmusic

import (
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
	songsFilterParam   = "Eg-KAQwIABAAGAAgACgB" // Strict Songs filter in YouTube Music
	defaultUserAgent   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
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
}

// NewClient creates a new YouTube Music API client
func NewClient() *Client {
	return &Client{
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		streamCache: make(map[string]cachedStream),
	}
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
		} else if strings.HasPrefix(browseID, "MPRE") || strings.HasPrefix(browseID, "FEmusic_library_privately_owned_release") {
			if album == "" {
				album = t
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

// GetTrack fetches track metadata by video ID
func (c *Client) GetTrack(ctx context.Context, videoID string) (*YTTrack, error) {
	if videoID == "" {
		return nil, fmt.Errorf("empty videoId")
	}

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

// GetStreamURL resolves the direct audio stream URL using yt-dlp on the host
func (c *Client) GetStreamURL(ctx context.Context, videoID string) (string, error) {
	c.streamCacheMu.RLock()
	if entry, ok := c.streamCache[videoID]; ok && time.Now().Before(entry.expiresAt) {
		c.streamCacheMu.RUnlock()
		return entry.url, nil
	}
	c.streamCacheMu.RUnlock()

	targetURL := fmt.Sprintf("https://www.youtube.com/watch?v=%s", videoID)
	cmd := exec.CommandContext(ctx, "yt-dlp",
		"--no-warnings",
		"--js-runtimes", "node",
		"-f", "ba/b",
		"-g", targetURL,
	)

	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("yt-dlp stream resolution failed for %s: %w", videoID, err)
	}

	rawURL := strings.TrimSpace(string(out))
	lines := strings.Split(rawURL, "\n")
	streamURL := strings.TrimSpace(lines[0])

	if streamURL == "" {
		return "", fmt.Errorf("empty stream url returned by yt-dlp for %s", videoID)
	}

	log.Printf("[YTMUSIC] ✅ Resolved direct stream URL for track %s via yt-dlp", videoID)

	c.streamCacheMu.Lock()
	c.streamCache[videoID] = cachedStream{
		url:       streamURL,
		expiresAt: time.Now().Add(1 * time.Hour), // Google video links typically expire in 6 hours
	}
	c.streamCacheMu.Unlock()

	return streamURL, nil
}
