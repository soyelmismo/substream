package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.senan.xyz/gonic/provider/soundcloud"
)

var (
	scURLRegex = regexp.MustCompile(`(?:soundcloud\.com/|on\.soundcloud\.com/)`)
)

// SoundCloudProvider implements playlist extraction from SoundCloud API v2
type SoundCloudProvider struct {
	client     *soundcloud.Client
	httpClient *http.Client
}

// NewSoundCloudProvider creates a new SoundCloud playlist importer provider
func NewSoundCloudProvider() *SoundCloudProvider {
	return &SoundCloudProvider{
		client: soundcloud.NewClient(soundcloud.ClientOptions{Timeout: 15 * time.Second}),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
	}
}

// Name returns the provider identifier
func (s *SoundCloudProvider) Name() string {
	return "soundcloud"
}

// Match checks if the URL is a SoundCloud playlist, track, or user link
func (s *SoundCloudProvider) Match(rawURL string) bool {
	return scURLRegex.MatchString(rawURL)
}

// Fetch retrieves playlist metadata and tracks from SoundCloud
func (s *SoundCloudProvider) Fetch(ctx context.Context, rawURL string) (*ImportedPlaylist, error) {
	targetURL := strings.TrimSpace(rawURL)

	// Follow redirects if it is an on.soundcloud.com short URL
	if strings.Contains(targetURL, "on.soundcloud.com") {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, targetURL, nil)
		if err == nil {
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
			if resp, err := s.httpClient.Do(req); err == nil {
				defer resp.Body.Close()
				if finalURL := resp.Request.URL.String(); finalURL != "" && !strings.Contains(finalURL, "on.soundcloud.com") {
					targetURL = finalURL
				}
			}
		}
	}

	// Resolve the URL using the SoundCloud API v2 resolve endpoint
	rawJSON, err := s.client.Resolve(ctx, targetURL)
	if err != nil {
		return nil, fmt.Errorf("resolve soundcloud URL %s: %w", targetURL, err)
	}

	var header soundcloud.SoundCloudResolveHeader
	if err := json.Unmarshal(rawJSON, &header); err != nil {
		return nil, fmt.Errorf("parse soundcloud response kind: %w", err)
	}

	switch header.Kind {
	case "playlist":
		var pl soundcloud.SoundCloudPlaylist
		if err := json.Unmarshal(rawJSON, &pl); err != nil {
			return nil, fmt.Errorf("unmarshal playlist: %w", err)
		}

		coverURL := pl.ArtworkURL
		if coverURL == "" {
			coverURL = pl.User.AvatarURL
		}
		if strings.Contains(coverURL, "-large.") {
			coverURL = strings.Replace(coverURL, "-large.", "-t500x500.", 1)
		}

		title := strings.TrimSpace(pl.Title)
		if title == "" {
			title = "SoundCloud Playlist"
		}

		res := &ImportedPlaylist{
			Title:       title,
			Description: pl.User.Username,
			CoverURL:    coverURL,
			Tracks:      make([]ImportedTrack, 0, len(pl.Tracks)),
		}

		// Identify stub tracks (missing title or user) that need batch resolution
		var stubIDs []string
		for _, t := range pl.Tracks {
			if t.Title == "" && t.ID > 0 {
				stubIDs = append(stubIDs, strconv.FormatInt(t.ID, 10))
			}
		}

		stubMap := make(map[int64]*soundcloud.SoundCloudTrack)
		if len(stubIDs) > 0 {
			if resolvedTracks, err := s.client.BatchGetTracks(ctx, stubIDs); err == nil {
				for _, rt := range resolvedTracks {
					if rt != nil {
						stubMap[rt.ID] = rt
					}
				}
			}
		}

		for _, t := range pl.Tracks {
			trackTitle := t.Title
			artist := t.User.Username
			duration := t.Duration / 1000

			if trackTitle == "" {
				if full, ok := stubMap[t.ID]; ok && full != nil {
					trackTitle = full.Title
					artist = full.User.Username
					duration = full.Duration / 1000
				}
			}

			if trackTitle == "" {
				continue
			}
			if artist == "" {
				artist = pl.User.Username
			}

			res.Tracks = append(res.Tracks, ImportedTrack{
				Title:     trackTitle,
				Artist:    artist,
				Album:     title,
				Duration:  duration,
				SourceURI: fmt.Sprintf("sc:tr:%d", t.ID),
			})
		}
		return res, nil

	case "track":
		var tr soundcloud.SoundCloudTrack
		if err := json.Unmarshal(rawJSON, &tr); err != nil {
			return nil, fmt.Errorf("unmarshal track: %w", err)
		}
		coverURL := tr.ArtworkURL
		if coverURL == "" {
			coverURL = tr.User.AvatarURL
		}
		if strings.Contains(coverURL, "-large.") {
			coverURL = strings.Replace(coverURL, "-large.", "-t500x500.", 1)
		}
		res := &ImportedPlaylist{
			Title:       tr.Title,
			Description: tr.User.Username,
			CoverURL:    coverURL,
			Tracks: []ImportedTrack{
				{
					Title:     tr.Title,
					Artist:    tr.User.Username,
					Album:     "SoundCloud Single",
					Duration:  tr.Duration / 1000,
					SourceURI: fmt.Sprintf("sc:tr:%d", tr.ID),
				},
			},
		}
		return res, nil

	case "user":
		var u soundcloud.SoundCloudUser
		if err := json.Unmarshal(rawJSON, &u); err != nil {
			return nil, fmt.Errorf("unmarshal user: %w", err)
		}
		coverURL := u.AvatarURL
		if strings.Contains(coverURL, "-large.") {
			coverURL = strings.Replace(coverURL, "-large.", "-t500x500.", 1)
		}
		tracks, err := s.client.GetUserTracks(ctx, strconv.FormatInt(u.ID, 10), 50)
		if err != nil {
			return nil, fmt.Errorf("fetch user tracks: %w", err)
		}
		title := fmt.Sprintf("%s's Tracks", u.Username)
		res := &ImportedPlaylist{
			Title:       title,
			Description: u.Description,
			CoverURL:    coverURL,
			Tracks:      make([]ImportedTrack, 0, len(tracks)),
		}
		for _, t := range tracks {
			if t == nil {
				continue
			}
			res.Tracks = append(res.Tracks, ImportedTrack{
				Title:     t.Title,
				Artist:    t.User.Username,
				Album:     title,
				Duration:  t.Duration / 1000,
				SourceURI: fmt.Sprintf("sc:tr:%d", t.ID),
			})
		}
		return res, nil

	default:
		return nil, fmt.Errorf("unsupported soundcloud resource kind: %s", header.Kind)
	}
}
