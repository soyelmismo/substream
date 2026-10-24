package ctrlsubsonic

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/internal/media/streamer"
	"go.senan.xyz/gonic/internal/media/tagger"
	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/provider/tidal"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// nonAsciiUnsafe matches any character that is NOT a safe filename char.
// Allows: Unicode letters (incluye tildes, ñ, ü, etc.), numbers, space, period, hyphen, underscore
var nonAsciiUnsafe = regexp.MustCompile(`[^\p{L}\p{N} ._-]+`)

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
		return c.serveAlbumDownload(w, r, &id, user)
	case specid.Playlist:
		return c.servePlaylistDownload(w, r, id.Value(), user)
	case specid.Track:
		// For single tracks, download with proper filename
		return c.serveTrackDownload(w, r, &id, user)
	default:
		return spec.NewError(10, "unsupported id type for download")
	}
}

func (c *Controller) serveAlbumDownload(w http.ResponseWriter, r *http.Request, id *specid.ID, _ *db.User) *spec.Response {
	albumID := id.Value()
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

		// Determine extension based on quality
		ext := "flac" // Default lossless FLAC
		switch tidalQuality {
		case "LOW":
			ext = "m4a"
		default:
			if strings.Contains(streamURL, ".m4a") {
				ext = "m4a"
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
		if err := c.downloadAndAddToZip(ctx, streamURL, entry, getClientIP(r), &track); err != nil {
			log.Printf("[DOWNLOAD] Error downloading track %d: %v", track.ID, err)
			continue
		}

		log.Printf("[DOWNLOAD] Added track to ZIP: %s", trackFile)
	}

	log.Printf("[DOWNLOAD] Album ZIP download complete: album=%d", albumID)
	return nil
}

func (c *Controller) servePlaylistDownload(_ http.ResponseWriter, _ *http.Request, _ int, _ *db.User) *spec.Response {
	// Playlist download not supported - fall back to single track download
	return spec.NewError(0, "playlist download not supported")
}

func (c *Controller) buildTaggerMetadata(ctx context.Context, track *tidal.TidalTrack) *tagger.Metadata {
	if track == nil {
		return nil
	}
	coverURL := ""
	if track.Album.Cover != "" {
		coverURL = c.proxy.GetCoverURL(track.Album.Cover, 1280)
	} else if track.Album.ID > 0 {
		covUUID := c.proxy.GetCoverUUIDForAlbum(ctx, track.Album.ID)
		if covUUID != "" {
			coverURL = c.proxy.GetCoverURL(covUUID, 1280)
		}
	}
	artistName := track.Artist.Name
	albumArtist := artistName
	if len(track.Artists) > 0 {
		albumArtist = track.Artists[0].Name
	}

	return &tagger.Metadata{
		Title:       track.Title,
		Artist:      artistName,
		Album:       track.Album.Title,
		AlbumArtist: albumArtist,
		TrackNumber: track.TrackNumber,
		DiscNumber:  track.VolumeNumber,
		Date:        track.Album.ReleaseDate,
		ISRC:        track.ISRC,
		CoverURL:    coverURL,
	}
}

func (c *Controller) serveTrackDownload(w http.ResponseWriter, r *http.Request, id *specid.ID, _ *db.User) *spec.Response {
	prep, err := c.prepareStream(r.Context(), r, id)
	if err != nil {
		return spec.NewError(0, "error preparing download: %v", err)
	}

	cleanArtist := "Unknown Artist"
	cleanTitle := "Unknown Title"
	ext := "flac"
	if prep.Quality == "LOW" || prep.Quality == "HIGH" || (prep.Track != nil && !prep.Track.IsLossless()) {
		ext = "m4a"
	} else if prep.Ext != "" {
		ext = prep.Ext
	}

	var meta *tagger.Metadata
	if prep.Track != nil {
		meta = c.buildTaggerMetadata(r.Context(), prep.Track)
		if prep.Track.Artist.Name != "" {
			cleanArtist = prep.Track.Artist.Name
		}
		if prep.Track.Title != "" {
			cleanTitle = prep.Track.Title
		}
	}

	prov := c.getProvider(id.Provider())
	if prov != nil {
		if tc, err := prov.GetTrack(r.Context(), id.RawID()); err == nil && tc != nil {
			if tc.Artist != "" {
				cleanArtist = tc.Artist
			}
			if tc.Title != "" {
				cleanTitle = tc.Title
			}
			if meta == nil {
				coverURL := ""
				if tc.CoverID != nil {
					coverURL = fmt.Sprintf("https://%s/rest/getCoverArt?id=%s", r.Host, tc.CoverID.String())
				}
				meta = &tagger.Metadata{
					Title:       tc.Title,
					Artist:      tc.Artist,
					Album:       tc.Album,
					AlbumArtist: tc.Artist,
					TrackNumber: tc.TrackNumber,
					DiscNumber:  tc.DiscNumber,
					CoverURL:    coverURL,
				}
			}
		}
	}

	contentType := "audio/" + ext
	if ext == "m4a" {
		contentType = "audio/mp4"
	}

	filename := fmt.Sprintf("%s - %s.%s", cleanArtist, cleanTitle, ext)
	filename = sanitizeFilename(filename)

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Header().Set("Cache-Control", "no-cache")

	// Intercepción para Cloudflare Worker Download Offload:
	// Si la petición viene del Worker solicitando metadatos para descargar y empaquetar en el Edge,
	// y NO es un stream segmentado HLS (requiere ffmpeg/remux), devolvemos JSON con la URL directa y metadatos.
	secret := strings.TrimSpace(os.Getenv("SUBSTREAM_WORKER_SECRET"))
	workerAuthHeader := r.Header.Get("X-Substream-Worker-Secret")
	isWorker := (secret == "" && (r.Header.Get("X-Substream-Resolve-Only") == "1" || r.Header.Get("X-Worker-Proxy") == "1")) ||
		(secret != "" && workerAuthHeader == secret)

	if isWorker {
		isHLS := prep.IsHLS || strings.Contains(prep.StreamURL, ".m3u8") || strings.Contains(prep.StreamURL, "manifestType=HLS")
		if !isHLS && !strings.HasPrefix(prep.StreamURL, "data:") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success":      true,
				"is_download":  true,
				"track_id":     id.String(),
				"upstream_url": prep.StreamURL,
				"ext":          ext,
				"content_type": contentType,
				"filename":     filename,
				"metadata":     meta,
			})
			return nil
		}
	}

	log.Printf("[DOWNLOAD] Starting single track download: id=%s artist=%q title=%q quality=%s", id.String(), cleanArtist, cleanTitle, prep.Quality)

	// Delegate to provider if it implements MediaDownloader (e.g. Tidal, YouTube Music)
	if prov != nil {
		if dl, ok := prov.(provider.MediaDownloader); ok {
			req := provider.DownloadRequest{
				TrackID:       id.RawID(),
				Quality:       prep.Quality,
				ClientIP:      prep.ClientIP,
				ClientName:    prep.ClientName,
				OffsetSeconds: 0,
				Metadata:      meta,
			}
			if err := dl.DownloadTrack(r.Context(), req, w); err != nil {
				log.Printf("[DOWNLOAD] Provider %s download failed: %v", id.Provider(), err)
				return spec.NewError(0, "download failed: %v", err)
			}
			log.Printf("[DOWNLOAD] Single track download complete via %s: id=%s", id.Provider(), id.String())
			return nil
		}
	}

	// Fallback to universal downloader
	if err := c.downloadAndAddToZip(r.Context(), prep.StreamURL, w, prep.ClientIP, prep.Track); err != nil {
		log.Printf("[DOWNLOAD] Error downloading track %s: %v", id.String(), err)
		return spec.NewError(0, "download failed: %v", err)
	}

	log.Printf("[DOWNLOAD] Single track download complete: id=%s", id.String())
	return nil
}

func (c *Controller) downloadAndAddToZip(ctx context.Context, streamURL string, w io.Writer, clientIP string, track *tidal.TidalTrack) error {
	// Check if this is a manifest that needs proxy/stitching
	isHLS := strings.Contains(streamURL, ".m3u8") || strings.Contains(streamURL, "manifestType=HLS")
	isDASH := strings.Contains(streamURL, ".mpd") || strings.Contains(streamURL, "manifestType=MPEG_DASH")

	if isHLS {
		return c.downloadAndStitchHLS(ctx, streamURL, w, clientIP, track, 0, 0, 0, "download")
	}
	if isDASH {
		return c.downloadAndStitchDASH(ctx, streamURL, w, clientIP, track, 0)
	}

	meta := c.buildTaggerMetadata(ctx, track)
	return streamer.Download(ctx, streamURL, w, meta, clientIP, c.streamClient)
}

// sanitizeFilename sanitizes a string for safe use in filenames.
// Removes unsafe filesystem characters but keeps Unicode letters (tildes, ñ, ü, etc.)
func sanitizeFilename(name string) string {
	// First replace common unsafe chars with hyphen
	replacer := strings.NewReplacer(
		"/", "-",
		"\\", "-",
		":", "-",
		"*", "",
		"?", "",
		"\"", "'",
		"<", "",
		">", "",
		"|", "-",
	)
	name = replacer.Replace(name)
	// Then remove symbols and non-ASCII unicode (but keep letters with tildes)
	name = nonAsciiUnsafe.ReplaceAllString(name, "")
	return strings.TrimSpace(name)
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
