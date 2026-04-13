package importer

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/internal/matchutil"
	"go.senan.xyz/gonic/tidalproxy"
)

const (
	maxConcurrentImports = 3
	searchDelay          = 100 * time.Millisecond // Rate limit between searches
	maxSearchResults     = 5                      // Limit search results to check
)

// JobManager manages background import jobs
type JobManager struct {
	registry   *Registry
	semaphore  chan struct{} // Limits concurrent jobs
	activeJobs sync.Map      // playlistID -> context.CancelFunc
	db         *db.DB
	proxy      tidalproxy.TidalProxy
	cachePath  string // Path for storing generated playlist covers
}

// coverInfo holds info for creating composite covers
type coverInfo struct {
	url     string
	albumID int
}

// NewJobManager creates a new import job manager
func NewJobManager(dbc *db.DB, proxy tidalproxy.TidalProxy, cachePath string) *JobManager {
	return &JobManager{
		registry:  NewRegistry(proxy),
		semaphore: make(chan struct{}, maxConcurrentImports),
		db:        dbc,
		proxy:     proxy,
		cachePath: cachePath,
	}
}

// extractIDFromURI extracts the numeric ID from a URI string (e.g., "td:tr:12345" -> 12345)
func extractIDFromURI(uri string) int {
	if uri == "" {
		return 0
	}
	parts := strings.Split(uri, ":")
	if len(parts) >= 3 {
		id, err := strconv.Atoi(parts[len(parts)-1])
		if err == nil {
			return id
		}
	}
	// Try legacy format "tr-12345"
	parts = strings.Split(uri, "-")
	if len(parts) >= 2 {
		id, err := strconv.Atoi(parts[len(parts)-1])
		if err == nil {
			return id
		}
	}
	return 0
}

// StartImport begins a background import job
// Returns immediately with the placeholder playlist ID
func (jm *JobManager) StartImport(ctx context.Context, userID int, sourceURL string) (*db.Playlist, error) {
	// Find the appropriate provider
	provider := jm.registry.Find(sourceURL)
	if provider == nil {
		return nil, fmt.Errorf("no provider found for URL: %s", sourceURL)
	}

	// Fetch playlist metadata first (this is quick)
	playlist, err := provider.Fetch(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("fetch playlist: %w", err)
	}

	// Create placeholder playlist in DB
	placeholder := db.Playlist{
		UserID:    userID,
		Name:      fmt.Sprintf("Importing: %s...", truncate(playlist.Title, 40)),
		Comment:   fmt.Sprintf("Importing %d tracks from %s...", len(playlist.Tracks), provider.Name()),
		CoverURL:  playlist.CoverURL, // Store cover URL from source (Tidal, etc.)
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := jm.db.Create(&placeholder).Error; err != nil {
		return nil, fmt.Errorf("create placeholder playlist: %w", err)
	}
	log.Printf("[IMPORT] Playlist created: ID=%d Name=%s CoverURL=%q Tracks=%d", placeholder.ID, placeholder.Name, playlist.CoverURL, len(playlist.Tracks))

	// Check if already importing
	if _, exists := jm.activeJobs.Load(placeholder.ID); exists {
		// Return existing placeholder
		return &placeholder, nil
	}

	// Create cancellable context for this job
	jobCtx, cancel := context.WithCancel(context.Background())
	jm.activeJobs.Store(placeholder.ID, cancel)

	// Start background job
	go jm.runJob(jobCtx, cancel, placeholder.ID, playlist)

	log.Printf("[IMPORT] Started background import job for playlist %d: %s (%d tracks)",
		placeholder.ID, playlist.Title, len(playlist.Tracks))

	return &placeholder, nil
}

// runJob executes the import in background with panic recovery
func (jm *JobManager) runJob(ctx context.Context, cancel context.CancelFunc, playlistID int, playlist *ImportedPlaylist) {
	// Ensure cleanup
	defer func() {
		jm.activeJobs.Delete(playlistID)
		cancel()
		if r := recover(); r != nil {
			log.Printf("[IMPORT] PANIC in import job for playlist %d: %v\n%s",
				playlistID, r, debug.Stack())
			// Update playlist to show error state
			jm.updatePlaylistError(playlistID, "Import failed due to internal error")
		}
	}()

	// Acquire semaphore slot
	select {
	case jm.semaphore <- struct{}{}:
		defer func() { <-jm.semaphore }()
	case <-ctx.Done():
		return
	}

	log.Printf("[IMPORT] Processing playlist %d: %s (%d tracks) with %d workers",
		playlistID, playlist.Title, len(playlist.Tracks), maxConcurrentImports)

	// Process tracks concurrently using worker pool
	results := jm.processTracksConcurrent(ctx, playlist.Tracks)

	// Collect results and maintain order
	usedTrackIDs := make(map[int]bool)
	successCount := 0
	failCount := 0
	skippedDuplicates := 0

	for _, result := range results {
		if result.tidalID == 0 {
			failCount++
			log.Printf("[IMPORT] Track %d/%d not found: %s - %s",
				result.index+1, len(playlist.Tracks), result.artist, result.title)
			continue
		}

		// Skip duplicates
		if usedTrackIDs[result.tidalID] {
			skippedDuplicates++
			log.Printf("[IMPORT] Track %d/%d skipped (duplicate Tidal ID %d): %s - %s",
				result.index+1, len(playlist.Tracks), result.tidalID, result.artist, result.title)
			continue
		}

		// Add to playlist
		if err := jm.addTrack(playlistID, result.tidalID, successCount); err != nil {
			failCount++
			log.Printf("[IMPORT] Failed to add track %d/%d: %v", result.index+1, len(playlist.Tracks), err)
			continue
		}

		usedTrackIDs[result.tidalID] = true
		successCount++
	}

	// Update playlist with final state
	totalProcessed := successCount + failCount + skippedDuplicates
	if successCount == 0 {
		jm.updatePlaylistError(playlistID, fmt.Sprintf("No tracks could be matched (tried %d)", len(playlist.Tracks)))
		log.Printf("[IMPORT] Playlist %d import failed: no tracks matched", playlistID)
	} else {
		jm.finalizePlaylist(playlistID, playlist.Title, playlist.Description, successCount)
		if skippedDuplicates > 0 {
			log.Printf("[IMPORT] Playlist %d import complete: %d imported, %d duplicates skipped, %d failed (total: %d/%d)",
				playlistID, successCount, skippedDuplicates, failCount, totalProcessed, len(playlist.Tracks))
		} else {
			log.Printf("[IMPORT] Playlist %d import complete: %d/%d tracks imported successfully",
				playlistID, successCount, len(playlist.Tracks))
		}
	}
}

// findTrack searches for a track in Tidal by ID, ISRC, or text
// Priority: TidalID (direct) > ISRC > Text search
func (jm *JobManager) findTrack(ctx context.Context, track ImportedTrack) int {
	// Priority 1: Use TidalID directly if available (from Tidal playlist import)
	// This avoids unnecessary search calls to the proxy
	if track.TidalID > 0 {
		// Verify the track exists by fetching its info
		if _, err := jm.proxy.GetTrackInfo(ctx, track.TidalID); err == nil {
			log.Printf("[IMPORT] Direct ID match: %d (%s - %s)", track.TidalID, track.Artist, track.Title)
			return track.TidalID
		}
		// Track ID exists but fetch failed - fall through to search
		log.Printf("[IMPORT] TidalID %d failed validation, falling back to search (%s - %s)",
			track.TidalID, track.Artist, track.Title)
	}

	// Priority 2: Try ISRC if available
	if track.ISRC != "" {
		tidalTrack := jm.searchByISRC(ctx, track.ISRC)
		if tidalTrack != nil {
			return tidalTrack.ID
		}
	}

	// Priority 3: Fallback to text search (with album for better matching)
	if track.Album == "" {
		log.Printf("[IMPORT DEBUG] No album info for track: %s - %s", track.Artist, track.Title)
	}
	return jm.searchByText(ctx, track.Artist, track.Title, track.Album)
}

// searchByISRC searches for a track by ISRC code
func (jm *JobManager) searchByISRC(ctx context.Context, isrc string) *tidalproxy.TidalTrack {
	// ISRC search via query parameter
	results, err := jm.proxy.SearchTracks(ctx, "isrc:"+isrc, maxSearchResults, 0)
	if err != nil {
		return nil
	}

	for _, t := range results {
		if strings.EqualFold(t.ISRC, isrc) {
			return &t
		}
	}
	return nil
}

// searchByText searches for a track by artist, title, and optionally album
func (jm *JobManager) searchByText(ctx context.Context, artist, title, album string) int {
	// For tracks with non-ASCII characters (Japanese, Chinese, etc.)
	// searching by title + album often works better than full artist+title
	query := fmt.Sprintf("%s %s", artist, title)
	hasNonASCII := false
	for _, r := range artist {
		if r > 127 {
			hasNonASCII = true
			break
		}
	}

	results, err := jm.proxy.SearchTracks(ctx, query, maxSearchResults, 0)
	if err != nil {
		// Try title+album fallback for non-ASCII
		if hasNonASCII && album != "" {
			results, err = jm.proxy.SearchTracks(ctx, fmt.Sprintf("%s %s", title, album), maxSearchResults, 0)
			if err != nil {
				results, err = jm.proxy.SearchTracks(ctx, title, maxSearchResults, 0)
			}
		} else {
			return 0
		}
	}

	// If no good results and we have non-ASCII, try title+album search
	if hasNonASCII && len(results) > 0 && album != "" {
		// Check if we got good matches
		best := 0.0
		for _, t := range results {
			score := matchutil.MatchScoreWithAlbum(artist, title, album, t)
			if score > best {
				best = score
			}
		}
		// If best score is poor, try title+album search
		if best < 0.5 {
			albumResults, err := jm.proxy.SearchTracks(ctx, fmt.Sprintf("%s %s", title, album), maxSearchResults, 0)
			if err == nil && len(albumResults) > 0 {
				results = albumResults
			}
		}
	}

	// Try to find best match using fuzzy comparison
	bestMatch := 0
	bestScore := 0.0
	anyMatch := 0
	anyScore := 0.0

	for _, t := range results {
		score := matchutil.MatchScore(artist, title, t)
		if score > bestScore {
			bestScore = score
			bestMatch = t.ID
		}
		// Track best overall even if below threshold
		if score > anyScore {
			anyScore = score
			anyMatch = t.ID
		}
	}

	// If we have a good match above threshold, use it
	if bestMatch != 0 && bestScore >= 0.5 {
		if bestScore < 0.7 {
			log.Printf("[IMPORT] Low confidence match (%.0f%%): %s - %s -> Tidal ID %d",
				bestScore*100, artist, title, bestMatch)
		}
		return bestMatch
	}

	// For non-ASCII tracks (Japanese, etc.), require high title similarity
	// to avoid importing wrong tracks from same artist
	if hasNonASCII {
		// Find best title match specifically
		bestTitleMatch := 0.0
		bestTitleTrack := 0
		for _, t := range results {
			qTitle := strings.ToLower(title)
			tTitle := strings.ToLower(t.Title)

			// Exact match
			if qTitle == tTitle {
				return t.ID
			}

			// Check title similarity
			titleSim := matchutil.Similarity(qTitle, tTitle)
			if titleSim > bestTitleMatch && titleSim >= 0.7 { // Require 70% title match
				bestTitleMatch = titleSim
				bestTitleTrack = t.ID
			}
		}
		if bestTitleTrack != 0 {
			log.Printf("[IMPORT] Title match (%.0f%%) for non-ASCII track: %s - %s -> Tidal ID %d",
				bestTitleMatch*100, artist, title, bestTitleTrack)
			return bestTitleTrack
		}
		// No good title match found - fail this track
		return 0
	}

	// For ASCII tracks without album info, can use fallback with caution
	if album == "" && anyMatch != 0 && anyScore >= 0.3 {
		log.Printf("[IMPORT] Fallback match (%.0f%%) - no album info: %s - %s -> Tidal ID %d",
			anyScore*100, artist, title, anyMatch)
		return anyMatch
	}

	return 0
}

// trackResult represents the result of processing a single track
type trackResult struct {
	index   int
	tidalID int
	artist  string
	title   string
	album   string
}

// processTracksConcurrent processes tracks using a worker pool for concurrent searches
func (jm *JobManager) processTracksConcurrent(ctx context.Context, tracks []ImportedTrack) []trackResult {
	if len(tracks) == 0 {
		return nil
	}

	// Use fewer workers for small playlists
	workers := maxConcurrentImports
	if len(tracks) < workers {
		workers = len(tracks)
	}

	// Channels for work distribution and result collection
	jobs := make(chan struct {
		index int
		track ImportedTrack
	}, len(tracks))
	results := make(chan trackResult, len(tracks))

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobs {
				// Check context
				select {
				case <-ctx.Done():
					results <- trackResult{index: job.index, tidalID: 0, artist: job.track.Artist, title: job.track.Title}
					continue
				default:
				}

				// Find track
				tidalID := jm.findTrack(ctx, job.track)
				results <- trackResult{
					index:   job.index,
					tidalID: tidalID,
					artist:  job.track.Artist,
					title:   job.track.Title,
					album:   job.track.Album,
				}

				// Rate limiting per worker (still respects proxy limits)
				time.Sleep(searchDelay)
			}
		}(w)
	}

	// Send jobs
	for i, track := range tracks {
		jobs <- struct {
			index int
			track ImportedTrack
		}{index: i, track: track}
	}
	close(jobs)

	// Wait for workers and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	resultSlice := make([]trackResult, 0, len(tracks))
	for r := range results {
		resultSlice = append(resultSlice, r)
	}

	// Sort by index to maintain playlist order
	for i := 0; i < len(resultSlice); i++ {
		for j := i + 1; j < len(resultSlice); j++ {
			if resultSlice[j].index < resultSlice[i].index {
				resultSlice[i], resultSlice[j] = resultSlice[j], resultSlice[i]
			}
		}
	}

	return resultSlice
}

// addTrack adds a track to the playlist
func (jm *JobManager) addTrack(playlistID, tidalID, position int) error {
	pt := db.PlaylistTrack{
		PlaylistID: playlistID,
		URI:        fmt.Sprintf("td:tr:%d", tidalID),
		Position:   position,
	}
	return jm.db.Create(&pt).Error
}

// updatePlaylistError updates playlist to show error state
func (jm *JobManager) updatePlaylistError(playlistID int, message string) {
	jm.db.Model(&db.Playlist{}).
		Where("id = ?", playlistID).
		Updates(map[string]interface{}{
			"name":       "Import Failed",
			"comment":    message,
			"updated_at": time.Now(),
		})
}

// finalizePlaylist updates playlist with real metadata
func (jm *JobManager) finalizePlaylist(playlistID int, title, description string, trackCount int) {
	// First, get current playlist to check existing comment
	var pl db.Playlist
	var finalDescription string

	if err := jm.db.Where("id = ?", playlistID).First(&pl).Error; err == nil {
		// If we have a description from source, use it
		if description != "" {
			finalDescription = description
		} else if pl.Comment != "" && !strings.HasPrefix(pl.Comment, "Importing") {
			// Keep existing comment if it's not the placeholder
			finalDescription = pl.Comment
		} else {
			// Fallback message
			finalDescription = fmt.Sprintf("Imported from external source (%d tracks)", trackCount)
		}

		// Generate composite cover if no cover exists and we have tracks
		if pl.CoverURL == "" && pl.CoverPath == "" {
			if coverPath := jm.generateCompositeCover(playlistID); coverPath != "" {
				pl.CoverPath = coverPath
				jm.db.Save(&pl)
				log.Printf("[IMPORT] Generated composite cover for playlist %d", playlistID)
			}
		}
	} else {
		// DB error, just use what we have
		if description != "" {
			finalDescription = description
		} else {
			finalDescription = fmt.Sprintf("Imported from external source (%d tracks)", trackCount)
		}
	}

	// Update name and comment
	updates := map[string]interface{}{
		"name":       title,
		"comment":    finalDescription,
		"updated_at": time.Now(),
	}

	jm.db.Model(&db.Playlist{}).
		Where("id = ?", playlistID).
		Updates(updates)
}

// generateCompositeCover creates a 2x2 grid cover from the first 4 tracks' album art
func (jm *JobManager) generateCompositeCover(playlistID int) string {
	// Get first 4 tracks from playlist
	var tracks []db.PlaylistTrack
	jm.db.Where("playlist_id = ?", playlistID).Order("position ASC").Limit(4).Find(&tracks)

	if len(tracks) == 0 {
		return ""
	}

	// Collect album cover URLs from tracks
	var covers []coverInfo

	for _, track := range tracks {
		tidalID := extractIDFromURI(track.URI)
		if tidalID == 0 {
			continue
		}

		// Get track info to find album
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		trackInfo, err := jm.proxy.GetTrackInfo(ctx, tidalID)
		cancel()

		if err == nil && trackInfo.Album.ID > 0 {
			// Get album cover
			albumID := trackInfo.Album.ID
			coverUUID := jm.proxy.GetCoverUUIDForAlbum(context.Background(), albumID)
			if coverUUID != "" {
				coverURL := jm.proxy.GetCoverURL(coverUUID, 320) // Medium size for grid
				if coverURL != "" {
					covers = append(covers, coverInfo{url: coverURL, albumID: albumID})
				}
			}
		}
	}

	if len(covers) == 0 {
		return ""
	}

	// Generate composite image
	compositePath := filepath.Join(jm.cachePath, "playlist-covers", fmt.Sprintf("pl-%d.jpg", playlistID))
	if err := os.MkdirAll(filepath.Dir(compositePath), 0755); err != nil {
		return ""
	}

	if err := createCompositeImage(covers, compositePath); err != nil {
		log.Printf("[IMPORT] Failed to create composite cover for playlist %d: %v", playlistID, err)
		return ""
	}

	return compositePath
}

// createCompositeImage creates a 2x2 grid from up to 4 cover URLs
func createCompositeImage(covers []coverInfo, outputPath string) error {
	if len(covers) == 0 {
		return fmt.Errorf("no covers provided")
	}

	const gridSize = 2
	const tileSize = 320  // Each tile is 320x320
	const finalSize = 640 // Final image is 640x640

	// Create blank canvas
	canvas := image.NewRGBA(image.Rect(0, 0, finalSize, finalSize))

	// Download and draw each cover
	client := &http.Client{Timeout: 10 * time.Second}
	drawn := 0

	for i, cover := range covers {
		if drawn >= 4 {
			break
		}

		// Download cover
		resp, err := client.Get(cover.url)
		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}

		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		// Decode image
		img, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			continue
		}

		// Resize to tile size
		resized := resizeImage(img, tileSize, tileSize)

		// Calculate position in 2x2 grid
		row := i / gridSize
		col := i % gridSize
		x := col * tileSize
		y := row * tileSize

		// Draw onto canvas
		draw.Draw(canvas, image.Rect(x, y, x+tileSize, y+tileSize), resized, image.Point{}, draw.Over)
		drawn++
	}

	if drawn == 0 {
		return fmt.Errorf("no covers could be drawn")
	}

	// If only 1 cover, use it directly (no need for grid)
	if drawn == 1 {
		// Just save the single cover we downloaded
		resp, _ := client.Get(covers[0].url)
		if resp != nil && resp.StatusCode == http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if len(data) > 0 {
				return os.WriteFile(outputPath, data, 0644)
			}
		}
	}

	// Save composite
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	return jpeg.Encode(file, canvas, &jpeg.Options{Quality: 85})
}

// resizeImage scales an image to fit within maxWidth x maxHeight while maintaining aspect ratio
func resizeImage(src image.Image, maxWidth, maxHeight int) image.Image {
	bounds := src.Bounds()
	srcW := bounds.Dx()
	srcH := bounds.Dy()

	// Calculate scale to fit within max dimensions
	scaleX := float64(maxWidth) / float64(srcW)
	scaleY := float64(maxHeight) / float64(srcH)
	scale := scaleX
	if scaleY < scaleX {
		scale = scaleY
	}

	newW := int(float64(srcW) * scale)
	newH := int(float64(srcH) * scale)

	// Use simple nearest-neighbor for speed (could use better interpolation)
	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))

	for y := 0; y < newH; y++ {
		for x := 0; x < newW; x++ {
			srcX := int(float64(x) / scale)
			srcY := int(float64(y) / scale)
			dst.Set(x, y, src.At(srcX+bounds.Min.X, srcY+bounds.Min.Y))
		}
	}

	return dst
}

// CancelImport cancels an active import job
func (jm *JobManager) CancelImport(playlistID int) bool {
	if cancel, ok := jm.activeJobs.Load(playlistID); ok {
		if cf, ok := cancel.(context.CancelFunc); ok {
			cf()
			return true
		}
	}
	return false
}

// truncate truncates a string to max length
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// Registry returns the provider registry (for testing/additional providers)
func (jm *JobManager) Registry() *Registry {
	return jm.registry
}
