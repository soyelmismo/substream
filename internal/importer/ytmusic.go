package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ytPlaylistRegex = regexp.MustCompile(`(?:music\.youtube\.com|youtube\.com)/playlist\?.*list=([a-zA-Z0-9_-]+)`)
)

// YouTubeMusicProvider implements playlist extraction from YouTube Music InnerTube API
type YouTubeMusicProvider struct {
	client *http.Client
}

// NewYouTubeMusicProvider creates a new YouTube Music playlist provider
func NewYouTubeMusicProvider() *YouTubeMusicProvider {
	return &YouTubeMusicProvider{
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// Name returns the provider identifier
func (y *YouTubeMusicProvider) Name() string {
	return "ytmusic"
}

// Match checks if the URL is a YouTube Music or YouTube playlist link
func (y *YouTubeMusicProvider) Match(rawURL string) bool {
	return ytPlaylistRegex.MatchString(rawURL)
}

// Fetch retrieves playlist metadata and tracks from YouTube Music
func (y *YouTubeMusicProvider) Fetch(ctx context.Context, rawURL string) (*ImportedPlaylist, error) {
	matches := ytPlaylistRegex.FindStringSubmatch(rawURL)
	if len(matches) < 2 || matches[1] == "" {
		// Fallback parse query param
		if u, err := url.Parse(rawURL); err == nil {
			listID := u.Query().Get("list")
			if listID != "" {
				matches = []string{rawURL, listID}
			}
		}
	}
	if len(matches) < 2 || matches[1] == "" {
		return nil, fmt.Errorf("invalid YouTube Music playlist URL")
	}

	playlistID := matches[1]
	browseID := playlistID
	if !strings.HasPrefix(browseID, "VL") {
		browseID = "VL" + browseID
	}

	payload := map[string]interface{}{
		"context": map[string]interface{}{
			"client": map[string]interface{}{
				"clientName":    "WEB_REMIX",
				"clientVersion": "1.20260401.01.00",
				"hl":            "en",
				"gl":            "US",
			},
		},
		"browseId": browseID,
	}

	bodyJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://music.youtube.com/youtubei/v1/browse?alt=json", bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	req.Header.Set("Origin", "https://music.youtube.com")

	resp, err := y.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("innertube request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("innertube returned %d: %s", resp.StatusCode, string(data))
	}

	var root map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&root); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	result := &ImportedPlaylist{
		Title:  "YouTube Music Playlist",
		Tracks: make([]ImportedTrack, 0),
	}

	// Extract title, description, cover from header
	y.extractHeader(root, result)

	// Recursively collect musicResponsiveListItemRenderer items
	var renderers []map[string]interface{}
	y.collectRenderers(root, &renderers)

	for _, item := range renderers {
		track := y.parseTrackItem(item)
		if track.Title != "" && (track.VideoID != "" || track.Artist != "") {
			result.Tracks = append(result.Tracks, track)
		}
	}

	log.Printf("[YTMUSIC] Imported playlist %q with %d tracks", result.Title, len(result.Tracks))
	return result, nil
}

func (y *YouTubeMusicProvider) extractHeader(root map[string]interface{}, pl *ImportedPlaylist) {
	// 1. Try microformatDataRenderer first (contains canonical full title and 1200x1200 thumbnail)
	if mf, ok := root["microformat"].(map[string]interface{}); ok {
		if mfd, ok := mf["microformatDataRenderer"].(map[string]interface{}); ok {
			if title, ok := mfd["title"].(string); ok && title != "" {
				pl.Title = title
			}
			if desc, ok := mfd["description"].(string); ok && desc != "" {
				pl.Description = desc
			}
			if thumbObj, ok := mfd["thumbnail"].(map[string]interface{}); ok {
				if thumbs, ok := thumbObj["thumbnails"].([]interface{}); ok && len(thumbs) > 0 {
					var bestURL string
					var bestW int
					for _, t := range thumbs {
						if tMap, ok := t.(map[string]interface{}); ok {
							w, _ := tMap["width"].(float64)
							if u, ok := tMap["url"].(string); ok && u != "" {
								if int(w) >= bestW {
									bestW = int(w)
									bestURL = u
								}
							}
						}
					}
					if bestURL != "" {
						pl.CoverURL = bestURL
					}
				}
			}
		}
	}

	// 2. Fallback to header renderers if microformat was missing or incomplete
	header, ok := root["header"].(map[string]interface{})
	if !ok {
		return
	}

	for _, hKey := range []string{"musicDetailHeaderRenderer", "musicResponsiveHeaderRenderer", "musicEditablePlaylistDetailHeaderRenderer"} {
		if hObj, exists := header[hKey].(map[string]interface{}); exists {
			if pl.Title == "" || pl.Title == "YouTube Music Playlist" {
				if tRuns := extractRunsText(hObj["title"]); tRuns != "" {
					pl.Title = tRuns
				}
			}
			if pl.Description == "" {
				if dRuns := extractRunsText(hObj["description"]); dRuns != "" {
					pl.Description = dRuns
				}
			}
			if pl.CoverURL == "" {
				if thumbs := extractThumbnails(hObj["thumbnail"]); len(thumbs) > 0 {
					pl.CoverURL = thumbs[len(thumbs)-1]
				}
			}
			break
		}
	}
}

func (y *YouTubeMusicProvider) collectRenderers(node interface{}, out *[]map[string]interface{}) {
	switch v := node.(type) {
	case map[string]interface{}:
		if item, ok := v["musicResponsiveListItemRenderer"].(map[string]interface{}); ok {
			*out = append(*out, item)
		}
		for _, val := range v {
			y.collectRenderers(val, out)
		}
	case []interface{}:
		for _, item := range v {
			y.collectRenderers(item, out)
		}
	}
}

func (y *YouTubeMusicProvider) parseTrackItem(item map[string]interface{}) ImportedTrack {
	track := ImportedTrack{}

	// Video ID
	if pData, ok := item["playlistItemData"].(map[string]interface{}); ok {
		if vid, ok := pData["videoId"].(string); ok {
			track.VideoID = vid
			track.SourceURI = "yt:tr:" + vid
		}
	}

	// Flex columns (Column 0 = Title, Column 1 = Artist / Album)
	if flexCols, ok := item["flexColumns"].([]interface{}); ok {
		if len(flexCols) > 0 {
			track.Title = extractFlexColumnText(flexCols[0])
		}
		if len(flexCols) > 1 {
			artistAndAlbum := extractFlexColumnRuns(flexCols[1])
			if len(artistAndAlbum) > 0 {
				track.Artist = artistAndAlbum[0]
			}
			if len(artistAndAlbum) > 1 {
				track.Album = artistAndAlbum[len(artistAndAlbum)-1]
			}
		}
	}

	// Fixed columns (Duration)
	if fixedCols, ok := item["fixedColumns"].([]interface{}); ok && len(fixedCols) > 0 {
		durStr := extractFixedColumnText(fixedCols[0])
		track.Duration = parseDuration(durStr)
	}

	return track
}

func extractFlexColumnText(col interface{}) string {
	m, ok := col.(map[string]interface{})
	if !ok {
		return ""
	}
	renderer, ok := m["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{})
	if !ok {
		return ""
	}
	return extractRunsText(renderer["text"])
}

func extractFlexColumnRuns(col interface{}) []string {
	m, ok := col.(map[string]interface{})
	if !ok {
		return nil
	}
	renderer, ok := m["musicResponsiveListItemFlexColumnRenderer"].(map[string]interface{})
	if !ok {
		return nil
	}
	textObj, ok := renderer["text"].(map[string]interface{})
	if !ok {
		return nil
	}
	runs, ok := textObj["runs"].([]interface{})
	if !ok {
		return nil
	}

	var res []string
	for _, r := range runs {
		if rMap, ok := r.(map[string]interface{}); ok {
			if t, ok := rMap["text"].(string); ok {
				t = strings.TrimSpace(t)
				if t != "" && t != "•" && t != "&" {
					res = append(res, t)
				}
			}
		}
	}
	return res
}

func extractFixedColumnText(col interface{}) string {
	m, ok := col.(map[string]interface{})
	if !ok {
		return ""
	}
	renderer, ok := m["musicResponsiveListItemFixedColumnRenderer"].(map[string]interface{})
	if !ok {
		return ""
	}
	return extractRunsText(renderer["text"])
}

func extractRunsText(textObj interface{}) string {
	m, ok := textObj.(map[string]interface{})
	if !ok {
		return ""
	}
	runs, ok := m["runs"].([]interface{})
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, r := range runs {
		if rMap, ok := r.(map[string]interface{}); ok {
			if t, ok := rMap["text"].(string); ok {
				b.WriteString(t)
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func extractThumbnails(thumbObj interface{}) []string {
	m, ok := thumbObj.(map[string]interface{})
	if !ok {
		return nil
	}
	cropper, ok := m["musicThumbnailRenderer"].(map[string]interface{})
	if ok {
		m = cropper
	}
	tObj, ok := m["thumbnail"].(map[string]interface{})
	if !ok {
		return nil
	}
	thumbs, ok := tObj["thumbnails"].([]interface{})
	if !ok {
		return nil
	}
	var res []string
	for _, item := range thumbs {
		if tMap, ok := item.(map[string]interface{}); ok {
			if u, ok := tMap["url"].(string); ok && u != "" {
				res = append(res, u)
			}
		}
	}
	return res
}

func parseDuration(durStr string) int {
	parts := strings.Split(strings.TrimSpace(durStr), ":")
	if len(parts) == 2 {
		m, _ := strconv.Atoi(parts[0])
		s, _ := strconv.Atoi(parts[1])
		return m*60 + s
	} else if len(parts) == 3 {
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		s, _ := strconv.Atoi(parts[2])
		return h*3600 + m*60 + s
	}
	return 0
}
