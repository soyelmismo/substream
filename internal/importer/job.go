package importer

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
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
	semaphore  chan struct{}        // Limits concurrent jobs
	activeJobs sync.Map             // playlistID -> context.CancelFunc
	db         *db.DB
	proxy      tidalproxy.TidalProxy
}

// NewJobManager creates a new import job manager
func NewJobManager(dbc *db.DB, proxy tidalproxy.TidalProxy) *JobManager {
	return &JobManager{
		registry:  NewRegistry(),
		semaphore: make(chan struct{}, maxConcurrentImports),
		db:        dbc,
		proxy:     proxy,
	}
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
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := jm.db.Create(&placeholder).Error; err != nil {
		return nil, fmt.Errorf("create placeholder playlist: %w", err)
	}

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

// findTrack searches for a track in Tidal by ISRC or text
func (jm *JobManager) findTrack(ctx context.Context, track ImportedTrack) int {
	// Try ISRC first if available
	if track.ISRC != "" {
		tidalTrack := jm.searchByISRC(ctx, track.ISRC)
		if tidalTrack != nil {
			return tidalTrack.ID
		}
	}

	// Fallback to text search
	return jm.searchByText(ctx, track.Artist, track.Title)
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

// searchByText searches for a track by artist and title with fuzzy matching
func (jm *JobManager) searchByText(ctx context.Context, artist, title string) int {
	// For tracks with non-ASCII characters (Japanese, Chinese, etc.)
	// searching by title only often works better than full artist+title
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
		// Try title-only fallback for non-ASCII
		if hasNonASCII {
			results, err = jm.proxy.SearchTracks(ctx, title, maxSearchResults, 0)
			if err != nil {
				return 0
			}
		} else {
			return 0
		}
	}

	// If no good results and we have non-ASCII, try title-only search
	if hasNonASCII && len(results) > 0 {
		// Check if we got good matches
		best := 0.0
		for _, t := range results {
			score := matchScore(artist, title, t)
			if score > best {
				best = score
			}
		}
		// If best score is poor, try title-only search
		if best < 0.5 {
			titleResults, err := jm.proxy.SearchTracks(ctx, title, maxSearchResults, 0)
			if err == nil && len(titleResults) > 0 {
				// Use title-only results if they give better matches
				results = titleResults
			}
		}
	}

	// Try to find best match using fuzzy comparison
	bestMatch := 0
	bestScore := 0.0
	anyMatch := 0
	anyScore := 0.0

	for _, t := range results {
		score := matchScore(artist, title, t)
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

	// AGGRESSIVE FALLBACK: If no good match but we have results, take the best available
	// This handles cases where Tidal has the track but fuzzy matching fails (e.g., Japanese characters)
	// MINIMUM 20% threshold to prevent completely wrong matches
	if anyMatch != 0 && anyScore >= 0.2 {
		log.Printf("[IMPORT] Very low confidence match (%.0f%%) - using anyway: %s - %s -> Tidal ID %d",
			anyScore*100, artist, title, anyMatch)
		return anyMatch
	}

	return 0
}

// matchScore calculates similarity between search query and result
// Returns 0.0-1.0 where 1.0 is perfect match
func matchScore(queryArtist, queryTitle string, track tidalproxy.TidalTrack) float64 {
	// Normalize strings for comparison
	normalize := func(s string) string {
		s = strings.ToLower(s)
		// Remove common suffixes/prefixes that might differ
		s = strings.ReplaceAll(s, " (feat.", " ")
		s = strings.ReplaceAll(s, " (ft.", " ")
		s = strings.ReplaceAll(s, " (featuring", " ")
		s = strings.ReplaceAll(s, " - ", " ")
		s = strings.ReplaceAll(s, "  ", " ")
		return strings.TrimSpace(s)
	}

	qArtist := normalize(queryArtist)
	qTitle := normalize(queryTitle)
	tArtist := normalize(track.Artist.Name)
	tTitle := normalize(track.Title)

	// Check all artists if available
	artistMatch := similarity(qArtist, tArtist)
	for _, a := range track.Artists {
		s := similarity(qArtist, normalize(a.Name))
		if s > artistMatch {
			artistMatch = s
		}
	}

	titleMatch := similarity(qTitle, tTitle)

	// Bonus for exact or near-exact title match (prevents wrong track from same album)
	if qTitle == tTitle || strings.EqualFold(qTitle, tTitle) {
		titleMatch = 1.0 // Perfect title match
	}

	// If artist matches well but title is poor, reduce score significantly
	// This prevents picking wrong track from same album
	if artistMatch > 0.7 && titleMatch < 0.5 {
		// Penalize - likely wrong track from same album
		return artistMatch * 0.3 // Very low score
	}

	// Weight: artist 30%, title 70% (title is more important for track identification)
	return (artistMatch*0.3 + titleMatch*0.7)
}

// similarity calculates string similarity
// Returns 0.0-1.0 where 1.0 is identical
func similarity(a, b string) float64 {
	if a == "" || b == "" {
		return 0.0
	}
	if a == b {
		return 1.0
	}

	// Case-insensitive comparison (handles Unicode better than ToLower for some chars)
	if strings.EqualFold(a, b) {
		return 0.98 // Almost perfect
	}

	// Check for substring match (strong signal)
	if strings.Contains(a, b) || strings.Contains(b, a) {
		longer := float64(max(len([]rune(a)), len([]rune(b))))
		shorter := float64(min(len([]rune(a)), len([]rune(b))))
		return 0.75 + (0.25 * shorter / longer) // 0.75-1.0 based on length ratio
	}

	// Contains word match (check if any word from a is in b)
	aWords := strings.Fields(a)
	bWords := strings.Fields(b)
	if len(aWords) == 0 || len(bWords) == 0 {
		return 0.0
	}

	// Count matching words (case-insensitive)
	matches := 0
	for _, aw := range aWords {
		awLower := strings.ToLower(aw)
		for _, bw := range bWords {
			bwLower := strings.ToLower(bw)
			// Exact match
			if awLower == bwLower {
				matches++
				break
			}
			// One contains the other (for partial matches)
			if strings.Contains(awLower, bwLower) || strings.Contains(bwLower, awLower) {
				matches++
				break
			}
		}
	}

	return float64(matches) / float64(max(len(aWords), len(bWords)))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// trackResult represents the result of processing a single track
type trackResult struct {
	index   int
	tidalID int
	artist  string
	title   string
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
		TidalID:    tidalID,
		Position:   position,
	}
	return jm.db.Create(&pt).Error
}

// updatePlaylistError updates playlist to show error state
func (jm *JobManager) updatePlaylistError(playlistID int, message string) {
	jm.db.Model(&db.Playlist{}).
		Where("id = ?", playlistID).
		Updates(map[string]interface{}{
			"name":    "Import Failed",
			"comment": message,
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
