package ctrlsubsonic

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// ServeDownload handles download requests for albums/playlists as ZIP files
// OpenSubsonic extension: https://opensubsonic.netlify.app/docs/endpoints/download/
func (c *Controller) ServeDownload(w http.ResponseWriter, r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	
	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "please provide an 'id' parameter")
	}

	user := r.Context().Value(CtxUser).(*db.User)

	// Determine what type of entity we're downloading
	switch id.Type() {
	case specid.Album:
		return c.serveAlbumDownload(w, r, id.Value(), user)
	case specid.Playlist:
		return c.servePlaylistDownload(w, r, id.Value(), user)
	case specid.Track:
		// For single tracks, download with proper filename
		return c.serveTrackDownload(w, r, id.Value(), user)
	default:
		return spec.NewError(10, "unsupported id type for download")
	}
}

func (c *Controller) serveAlbumDownload(w http.ResponseWriter, r *http.Request, albumID int, user *db.User) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	// Get album info from Tidal API
	album, err := c.proxy.GetAlbumInfo(r.Context(), albumID)
	if err != nil {
		return spec.NewError(70, "album not found: %v", err)
	}

	// Use album items as tracks
	tracks := album.Items
	if len(tracks) == 0 {
		return spec.NewError(0, "no tracks found for album")
	}

	// Sanitize filename - use first artist name and album title
	artistName := "Unknown"
	if len(album.Artists) > 0 {
		artistName = album.Artists[0].Name
	}
	filename := fmt.Sprintf("%s - %s.zip", artistName, album.Title)
	filename = sanitizeFilename(filename)

	// Get quality preference from request (same logic as streaming)
	bitrate := p.GetOrInt("maxBitRate", 0)
	tidalQuality := "LOSSLESS" // Default to LOSSLESS for downloads (best quality)
	switch {
	case bitrate == 0:
		tidalQuality = "LOSSLESS" // CD Quality FLAC
	case bitrate <= 128:
		tidalQuality = "LOW"
	case bitrate <= 320:
		tidalQuality = "HIGH"
	case bitrate >= 900:
		tidalQuality = "HI_RES_LOSSLESS"
	default:
		tidalQuality = "LOSSLESS"
	}

	// Set headers for ZIP download
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Header().Set("Cache-Control", "no-cache")

	// Create ZIP writer
	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()

	log.Printf("[DOWNLOAD] Starting album ZIP download: album=%d name=%q tracks=%d quality=%s", albumID, album.Title, len(tracks), tidalQuality)

	// Add each track to the ZIP
	for _, track := range tracks {
		// Get stream URL for this track with requested quality
		ctx := r.Context()
		streamURL, err := c.proxy.GetStreamURL(ctx, track.ID, tidalQuality, getClientIP(r))
		if err != nil {
			log.Printf("[DOWNLOAD] Error getting stream URL for track %d: %v", track.ID, err)
			continue
		}
		
		// Determine extension based on quality setting, not stream URL
		// Tidal returns manifests that don't reflect the actual codec in URL
		ext := "m4a" // Default AAC/MP4
		switch tidalQuality {
		case "LOSSLESS", "HI_RES_LOSSLESS":
			ext = "flac"
		case "LOW", "HIGH":
			ext = "m4a"
		default:
			// Fallback to URL detection for unknown qualities
			if strings.Contains(streamURL, ".flac") {
				ext = "flac"
			} else if strings.Contains(streamURL, ".mp3") {
				ext = "mp3"
			}
		}
		
		trackFile := fmt.Sprintf("%02d - %s.%s", track.TrackNumber, track.Title, ext)
		trackFile = sanitizeFilename(trackFile)

		// Create ZIP entry
		entry, err := zipWriter.Create(trackFile)
		if err != nil {
			log.Printf("[DOWNLOAD] Error creating ZIP entry for %s: %v", trackFile, err)
			continue
		}

		// Download and add track content
		if err := c.downloadAndAddToZip(ctx, streamURL, entry, getClientIP(r)); err != nil {
			log.Printf("[DOWNLOAD] Error downloading track %d: %v", track.ID, err)
			continue
		}

		log.Printf("[DOWNLOAD] Added track to ZIP: %s", trackFile)
	}

	log.Printf("[DOWNLOAD] Album ZIP download complete: album=%d", albumID)
	return nil
}

func (c *Controller) servePlaylistDownload(w http.ResponseWriter, r *http.Request, playlistID int, user *db.User) *spec.Response {
	// Playlist download not supported - fall back to single track download
	return spec.NewError(0, "playlist download not supported")
}

func (c *Controller) serveTrackDownload(w http.ResponseWriter, r *http.Request, trackID int, user *db.User) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)

	// Get track info
	track, err := c.proxy.GetTrackInfo(r.Context(), trackID)
	if err != nil {
		return spec.NewError(70, "track not found: %v", err)
	}

	// Get quality preference from request
	bitrate := p.GetOrInt("maxBitRate", 0)
	tidalQuality := "LOSSLESS"
	switch {
	case bitrate == 0:
		tidalQuality = "LOSSLESS"
	case bitrate <= 128:
		tidalQuality = "LOW"
	case bitrate <= 320:
		tidalQuality = "HIGH"
	case bitrate >= 900:
		tidalQuality = "HI_RES_LOSSLESS"
	default:
		tidalQuality = "LOSSLESS"
	}

	// Get stream URL
	ctx := r.Context()
	streamURL, err := c.proxy.GetStreamURL(ctx, trackID, tidalQuality, getClientIP(r))
	if err != nil {
		return spec.NewError(0, "error getting stream URL: %v", err)
	}

	// Determine extension based on quality
	ext := "m4a"
	switch tidalQuality {
	case "LOSSLESS", "HI_RES_LOSSLESS":
		ext = "flac"
	case "LOW", "HIGH":
		ext = "m4a"
	}

	// Create filename
	filename := fmt.Sprintf("%s - %s.%s", track.Artist.Name, track.Title, ext)
	filename = sanitizeFilename(filename)

	// Set headers for download
	w.Header().Set("Content-Type", "audio/"+ext)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Header().Set("Cache-Control", "no-cache")

	log.Printf("[DOWNLOAD] Starting single track download: track=%d artist=%q title=%q quality=%s", trackID, track.Artist.Name, track.Title, tidalQuality)

	// Download and stream the track
	if err := c.downloadAndAddToZip(ctx, streamURL, w, getClientIP(r)); err != nil {
		log.Printf("[DOWNLOAD] Error downloading track %d: %v", trackID, err)
		return spec.NewError(0, "download failed: %v", err)
	}

	log.Printf("[DOWNLOAD] Single track download complete: track=%d", trackID)
	return nil
}

func (c *Controller) downloadAndAddToZip(ctx context.Context, streamURL string, w io.Writer, clientIP string) error {
	// Check if this is a manifest that needs proxy/stitching
	isHLS := strings.Contains(streamURL, ".m3u8") || strings.Contains(streamURL, "manifestType=HLS")
	isDASH := strings.Contains(streamURL, ".mpd") || strings.Contains(streamURL, "manifestType=MPEG_DASH")
	
	if isHLS {
		// Use HLS stitcher - download and concatenate all segments (no seeking for downloads)
		return c.downloadAndStitchHLS(ctx, streamURL, w, clientIP, nil, 0)
	}
	if isDASH {
		// Use DASH stitcher
		return c.downloadAndStitchDASH(ctx, streamURL, w, clientIP, nil)
	}
	
	// Direct download for non-manifest URLs
	// Use shared client with keep-alive for better performance
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true, // Audio is already compressed
		},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", streamURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream returned %d", resp.StatusCode)
	}

	// Use larger buffer for faster copying
	buf := make([]byte, 32*1024) // 32KB buffer
	_, err = io.CopyBuffer(w, resp.Body, buf)
	return err
}

func sanitizeFilename(name string) string {
	// Replace characters that are invalid in filenames
	invalid := []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|"}
	for _, char := range invalid {
		name = strings.ReplaceAll(name, char, "_")
	}
	return name
}

func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP if multiple are present
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return xff
	}
	// Fall back to RemoteAddr
	if r.RemoteAddr != "" {
		// Strip port if present
		if idx := strings.LastIndex(r.RemoteAddr, ":"); idx != -1 {
			return r.RemoteAddr[:idx]
		}
		return r.RemoteAddr
	}
	return ""
}
