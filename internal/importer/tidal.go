package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"go.senan.xyz/gonic/tidalproxy"
)

var tidalPlaylistRegex = regexp.MustCompile(`tidal\.com/playlist/([a-fA-F0-9-]+)|listen\.tidal\.com/playlist/([a-fA-F0-9-]+)`)

const tidalUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

// TidalProvider implements playlist extraction from Tidal using hifi-api with web fallback
type TidalProvider struct {
	proxy  tidalproxy.TidalProxy
	client *http.Client
}

// NewTidalProvider creates a new TidalProvider with the given proxy
func NewTidalProvider(proxy tidalproxy.TidalProxy) *TidalProvider {
	return &TidalProvider{
		proxy: proxy,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Name returns the provider identifier
func (t *TidalProvider) Name() string {
	return "tidal"
}

// Match checks if the URL is a Tidal playlist link
func (t *TidalProvider) Match(url string) bool {
	return tidalPlaylistRegex.MatchString(url)
}

// Fetch retrieves playlist data from Tidal via hifi-api proxy, falling back to web scraping
func (t *TidalProvider) Fetch(ctx context.Context, playlistURL string) (*ImportedPlaylist, error) {
	// Extract playlist UUID
	matches := tidalPlaylistRegex.FindStringSubmatch(playlistURL)
	var playlistUUID string
	if len(matches) >= 2 && matches[1] != "" {
		playlistUUID = matches[1]
	} else if len(matches) >= 3 && matches[2] != "" {
		playlistUUID = matches[2]
	} else {
		return nil, fmt.Errorf("invalid Tidal playlist URL")
	}

	// Try hifi-api first if available
	if t.proxy != nil {
		playlist, err := t.proxy.GetPlaylist(ctx, playlistUUID)
		if err == nil {
			return t.convertTidalPlaylist(playlist), nil
		}
		// If error contains 4xx codes (auth/permission issues), fallback to web scraping
		errStr := strings.ToLower(err.Error())
		if !strings.Contains(errStr, "400") && !strings.Contains(errStr, "401") &&
			!strings.Contains(errStr, "403") && !strings.Contains(errStr, "404") &&
			!strings.Contains(errStr, "upstream api error") && !strings.Contains(errStr, "all") {
			return nil, fmt.Errorf("fetch playlist: %w", err)
		}
		log.Printf("[TIDAL:IMPORT] hifi-api failed (%v), falling back to web scraping", err)
		// Fall through to web scraping
	}

	// Fallback: scrape from Tidal web page
	return t.fetchFromWeb(ctx, playlistUUID)
}

// convertTidalPlaylist converts tidalproxy.TidalPlaylist to ImportedPlaylist
func (t *TidalProvider) convertTidalPlaylist(playlist *tidalproxy.TidalPlaylist) *ImportedPlaylist {
	result := &ImportedPlaylist{
		Title:       playlist.Title,
		Description: playlist.Description,
		Tracks:      make([]ImportedTrack, 0, len(playlist.Tracks)),
	}

	if playlist.SquareImage != "" {
		result.CoverURL = playlist.SquareImage
	} else if playlist.Image != "" {
		result.CoverURL = playlist.Image
	}

	for _, track := range playlist.Tracks {
		artist := track.Artist.Name
		if artist == "" && len(track.Artists) > 0 {
			artist = track.Artists[0].Name
		}

		result.Tracks = append(result.Tracks, ImportedTrack{
			Title:   track.Title,
			Artist:  artist,
			Album:   track.Album.Title,
			ISRC:    track.ISRC,
			TidalID: track.ID, // Direct from Tidal API - no search needed
		})
	}

	return result
}

// fetchFromWeb scrapes playlist data from Tidal's web interface
func (t *TidalProvider) fetchFromWeb(ctx context.Context, playlistUUID string) (*ImportedPlaylist, error) {
	url := fmt.Sprintf("https://tidal.com/playlist/%s", playlistUUID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("User-Agent", tidalUserAgent)
	req.Header.Set("Accept", "text/html")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch web page: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tidal web returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return t.parseWebData(body)
}

// parseWebData extracts playlist data from Tidal's HTML
func (t *TidalProvider) parseWebData(body []byte) (*ImportedPlaylist, error) {
	result := &ImportedPlaylist{
		Tracks: []ImportedTrack{},
	}

	// Try multiple patterns for __INITIAL_STATE__
	patterns := []string{
		`window\.__INITIAL_STATE__\s*=\s*({.+?});`,
		`window\.__PRELOADED_STATE__\s*=\s*({.+?});`,
		`"initialState":\s*({.+?})\s*,`,
	}

	var state map[string]interface{}
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		match := re.FindSubmatch(body)
		if match != nil {
			if err := json.Unmarshal(match[1], &state); err == nil {
				log.Printf("[TIDAL:WEB] Found state data using pattern: %s", pattern[:30])
				break
			}
		}
	}

	if state == nil {
		return t.parseWebDataAlternative(body)
	}

	// Extract playlist info from various possible paths
	t.extractPlaylistInfo(state, result)

	// Extract tracks from various possible structures
	t.extractTracksFromState(state, result)

	if len(result.Tracks) == 0 {
		return t.parseWebDataAlternative(body)
	}

	return result, nil
}

// extractPlaylistInfo extracts playlist metadata from state
func (t *TidalProvider) extractPlaylistInfo(state map[string]interface{}, result *ImportedPlaylist) {
	// Try various paths for playlist data
	paths := []string{"playlist", "data.playlist", "content.playlist", "entities.playlist"}

	for _, path := range paths {
		var playlist map[string]interface{}
		parts := strings.Split(path, ".")
		current := state
		found := true

		for _, part := range parts {
			if val, ok := current[part].(map[string]interface{}); ok {
				current = val
			} else {
				found = false
				break
			}
		}

		if found {
			playlist = current
			result.Title = getString(playlist, "title", "name", "Unknown Playlist")
			result.Description = getString(playlist, "description", "", "")

			if image, ok := playlist["image"].(string); ok && image != "" {
				result.CoverURL = image
			} else if squareImage, ok := playlist["squareImage"].(string); ok && squareImage != "" {
				result.CoverURL = squareImage
			} else if cover, ok := playlist["cover"].(string); ok && cover != "" {
				result.CoverURL = cover
			}
			break
		}
	}
}

// parseWebDataAlternative tries alternative parsing methods
func (t *TidalProvider) parseWebDataAlternative(body []byte) (*ImportedPlaylist, error) {
	result := &ImportedPlaylist{
		Tracks: []ImportedTrack{},
	}

	// Look for JSON-LD structured data
	jsonLDRegex := regexp.MustCompile(`<script type="application/ld\+json">(.+?)</script>`)
	matches := jsonLDRegex.FindAllSubmatch(body, -1)
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		var ldData map[string]interface{}
		if err := json.Unmarshal(m[1], &ldData); err != nil {
			continue
		}
		if ldData["@type"] == "MusicPlaylist" {
			if name, ok := ldData["name"].(string); ok {
				result.Title = name
			}
			if tracks, ok := ldData["track"].([]interface{}); ok {
				for _, trackItem := range tracks {
					track, ok := trackItem.(map[string]interface{})
					if !ok {
						continue
					}
					trackName := ""
					if name, ok := track["name"].(string); ok {
						trackName = name
					}
					artistName := ""
					if byArtist, ok := track["byArtist"].(map[string]interface{}); ok {
						if name, ok := byArtist["name"].(string); ok {
							artistName = name
						}
					}
					inAlbum := ""
					if album, ok := track["inAlbum"].(map[string]interface{}); ok {
						if name, ok := album["name"].(string); ok {
							inAlbum = name
						}
					}
					result.Tracks = append(result.Tracks, ImportedTrack{
						Title:  trackName,
						Artist: artistName,
						Album:  inAlbum,
					})
				}
			}
			return result, nil
		}
	}

	return nil, fmt.Errorf("could not extract playlist data from web page")
}

// extractTracksFromState extracts tracks from various state structures
func (t *TidalProvider) extractTracksFromState(state map[string]interface{}, result *ImportedPlaylist) {
	// Try different paths for tracks
	var tracks []interface{}

	if t, ok := state["tracks"].([]interface{}); ok {
		tracks = t
	} else if entities, ok := state["entities"].(map[string]interface{}); ok {
		if t, ok := entities["tracks"].([]interface{}); ok {
			tracks = t
		}
	} else if content, ok := state["content"].([]interface{}); ok {
		tracks = content
	}

	for _, item := range tracks {
		track, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		importedTrack := ImportedTrack{
			Title:  getString(track, "title", "name", "Unknown"),
			Artist: t.extractArtistFromTrack(track),
			Album:  t.extractAlbumFromTrack(track),
			ISRC:   getString(track, "isrc", "", ""),
		}
		result.Tracks = append(result.Tracks, importedTrack)
	}
}

// extractArtistFromTrack extracts artist name from track data
func (t *TidalProvider) extractArtistFromTrack(track map[string]interface{}) string {
	// Try artist object
	if artist, ok := track["artist"].(map[string]interface{}); ok {
		if name, ok := artist["name"].(string); ok {
			return name
		}
	}
	// Try artists array
	if artists, ok := track["artists"].([]interface{}); ok && len(artists) > 0 {
		if first, ok := artists[0].(map[string]interface{}); ok {
			if name, ok := first["name"].(string); ok {
				return name
			}
		}
	}
	return getString(track, "artist", "artistName", "Unknown Artist")
}

// extractAlbumFromTrack extracts album name from track data
func (t *TidalProvider) extractAlbumFromTrack(track map[string]interface{}) string {
	if album, ok := track["album"].(map[string]interface{}); ok {
		if title, ok := album["title"].(string); ok {
			return title
		}
	}
	return getString(track, "album", "albumTitle", "")
}
