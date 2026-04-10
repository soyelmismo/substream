package ctrlsubsonic

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/tidalproxy"

	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// genreTracksCacheEntry holds cached track IDs with expiry time.
type genreTracksCacheEntry struct {
	trackIDs []int
	expiry   time.Time
	element  *list.Element // Pointer to list element for O(1) access
}

// genreCache is a thread-safe cache for genre tracks with LRU eviction.
// Uses container/list for O(1) operations on move-to-front and eviction.
type genreCache struct {
	mu      sync.RWMutex
	entries map[string]*genreTracksCacheEntry
	lru     *list.List // Doubly-linked list for LRU ordering
	maxSize int
}

// newGenreCache creates a new genre cache with the specified maximum size.
func newGenreCache(maxSize int) *genreCache {
	return &genreCache{
		entries: make(map[string]*genreTracksCacheEntry),
		lru:     list.New(),
		maxSize: maxSize,
	}
}

// get retrieves cached track IDs for a genre, returning nil if not found or expired.
// Moves accessed entry to front of LRU list.
func (gc *genreCache) get(key string) []int {
	// Try read lock first for fast path
	gc.mu.RLock()
	entry, ok := gc.entries[key]
	if !ok {
		gc.mu.RUnlock()
		return nil
	}
	
	// Check if expired
	expired := time.Now().After(entry.expiry)
	if expired {
		gc.mu.RUnlock()
		// Upgrade to write lock to remove expired entry
		gc.mu.Lock()
		defer gc.mu.Unlock()
		// Double-check after acquiring write lock
		entry, ok = gc.entries[key]
		if ok && time.Now().After(entry.expiry) {
			gc.removeEntry(key, entry)
		}
		return nil
	}
	
	// Move to front (most recently used)
	// Need write lock for list modification
	gc.mu.RUnlock()
	gc.mu.Lock()
	defer gc.mu.Unlock()
	
	// Re-fetch entry after lock upgrade
	entry, ok = gc.entries[key]
	if !ok {
		return nil
	}
	gc.lru.MoveToFront(entry.element)
	return entry.trackIDs
}

// set stores track IDs for a genre with the given TTL, evicting the oldest entry if necessary.
func (gc *genreCache) set(key string, trackIDs []int, ttl time.Duration) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	// If entry exists, remove it first (will be re-added)
	if entry, exists := gc.entries[key]; exists {
		gc.removeEntry(key, entry)
	}

	// Add new entry at front
	element := gc.lru.PushFront(key)
	gc.entries[key] = &genreTracksCacheEntry{
		trackIDs: trackIDs,
		expiry:   time.Now().Add(ttl),
		element:  element,
	}

	// Evict oldest if at capacity
	if gc.lru.Len() > gc.maxSize {
		gc.evictOldest()
	}
}

// removeEntry removes an entry from both map and list.
// Must be called with lock held.
func (gc *genreCache) removeEntry(key string, entry *genreTracksCacheEntry) {
	gc.lru.Remove(entry.element)
	delete(gc.entries, key)
}

// evictOldest removes the least recently used cache entry.
// Must be called with lock held.
func (gc *genreCache) evictOldest() {
	if gc.lru.Len() == 0 {
		return
	}
	oldest := gc.lru.Back()
	if oldest != nil {
		key := oldest.Value.(string)
		if entry, ok := gc.entries[key]; ok {
			gc.removeEntry(key, entry)
			log.Printf("[RANDOM] Evicted oldest genre cache entry: %s", key)
		}
	}
}

// genreAlbumsCacheEntry holds cached albums with expiry time.
type genreAlbumsCacheEntry struct {
	albums []tidalproxy.TidalAlbum
	expiry time.Time
}

// genreAlbumCache is a simple TTL cache for genre albums to avoid duplicate API calls.
// Unlike genreCache, this doesn't need LRU since we cache by genre key directly.
type genreAlbumCache struct {
	mu      sync.RWMutex
	entries map[string]*genreAlbumsCacheEntry
}

func newGenreAlbumCache(maxSize int) *genreAlbumCache {
	return &genreAlbumCache{
		entries: make(map[string]*genreAlbumsCacheEntry),
	}
}

func (gac *genreAlbumCache) get(key string) []tidalproxy.TidalAlbum {
	gac.mu.RLock()
	defer gac.mu.RUnlock()

	entry, exists := gac.entries[key]
	if !exists || time.Now().After(entry.expiry) {
		return nil
	}
	return entry.albums
}

func (gac *genreAlbumCache) set(key string, albums []tidalproxy.TidalAlbum, ttl time.Duration) {
	gac.mu.Lock()
	defer gac.mu.Unlock()

	gac.entries[key] = &genreAlbumsCacheEntry{
		albums: albums,
		expiry: time.Now().Add(ttl),
	}
}

func (c *Controller) ServeGetRandomSongs(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)
	p := r.Context().Value(CtxParams).(params.Params)
	size := p.GetOrInt("size", 10)
	genre, _ := p.Get("genre")

	var tidalIDs []int

	// If genre specified, try to get genre tracks from hot.monochrome.tf
	if genre != "" {
		log.Printf("[RANDOM] Genre requested: %s", genre)
		tidalIDs = c.fetchHotGenreTracks(r.Context(), genre, size)
		if len(tidalIDs) > 0 {
			tracks := c.batchFetchTracks(r, tidalIDs)
			sub := spec.NewResponse()
			sub.RandomTracks = &spec.RandomTracks{List: tracks}
			return sub
		}
		log.Printf("[RANDOM] No tracks found for genre %s, falling back to random", genre)
	}

	// 1. Get some from starred (50%)
	favSize := size / 2
	if favSize < 1 {
		favSize = 1
	}

	var stars []db.TrackStar
	c.dbc.Where("user_id=?", user.ID).Order("RANDOM()").Limit(favSize).Find(&stars)

	for _, s := range stars {
		tidalIDs = appendUnique(tidalIDs, s.TidalID)
	}

	// 2. Get some from Discovery (Tidal Top Tracks)
	discoverySize := size - len(tidalIDs)
	if discoverySize > 0 {
		top, err := c.proxy.GetTopTracks(r.Context(), discoverySize+randomSongsDiscoveryExtra)
		if err == nil {
			for _, t := range top {
				if len(tidalIDs) >= size {
					break
				}
				tidalIDs = appendUnique(tidalIDs, t.ID)
			}
		}
	}

	tracks := c.batchFetchTracks(r, tidalIDs)

	sub := spec.NewResponse()
	sub.RandomTracks = &spec.RandomTracks{List: tracks}
	return sub
}

// fetchHotGenreTracks fetches tracks for a specific genre from hot.monochrome.tf.
// It first checks the cache for existing results, then fetches from the API if needed.
// The function tries to extract track IDs directly from sections, or falls back to
// extracting them from album data. Results are cached with a TTL to reduce API calls.
func (c *Controller) fetchHotGenreTracks(ctx context.Context, genre string, limit int) []int {
	// Validate and cap limit to prevent excessive requests
	const maxFetchLimit = 100
	if limit <= 0 {
		return nil
	}
	if limit > maxFetchLimit {
		limit = maxFetchLimit
		log.Printf("[RANDOM] Limit capped to %d for genre %s", maxFetchLimit, genre)
	}

	// Check cache first
	cacheKey := strings.ToLower(genre)
	if cached := c.genreCache.get(cacheKey); cached != nil {
		log.Printf("[RANDOM] Cache hit for genre %s", genre)
		// Return cached tracks (up to limit)
		if len(cached) > limit {
			return cached[:limit]
		}
		return cached
	}

	// Map genre name to hot.monochrome.tf format
	hotGenre, ok := hotGenreMapping[genre]
	if !ok {
		hotGenre = strings.ToLower(genre)
	}

	url := fmt.Sprintf("%s/explore/genre/?id=%s", hotMonochromeURL, hotGenre)
	log.Printf("[RANDOM] Fetching genre tracks from: %s", url)

	var result hotResponse
	if err := fetchJSON(ctx, c.httpClient, url, "RANDOM", &result); err != nil {
		log.Printf("[RANDOM] Error fetching genre tracks from hot.monochrome.tf: %v", err)
		return nil
	}

	var trackIDs []int
	var albumIDs []int

	// Priority 1: Extract from top_tracks (direct tracks)
	for _, track := range result.TopTracks {
		if track.ID != 0 {
			trackIDs = append(trackIDs, track.ID)
			if len(trackIDs) >= limit {
				break
			}
		}
	}

	// Priority 2: Extract from new_releases (albums)
	if len(trackIDs) < limit {
		for _, album := range result.NewReleases {
			if album.ID != 0 && album.StreamReady {
				albumIDs = appendUnique(albumIDs, album.ID)
			}
		}
	}

	// Priority 3: Extract from sections (ALBUM_LIST)
	if len(trackIDs) < limit {
		for _, section := range result.Sections {
			if section.Type == "ALBUM_LIST" {
				for _, item := range section.Items {
					if item.ID != 0 {
						albumIDs = appendUnique(albumIDs, item.ID)
					}
				}
			}
		}
	}

	// Extract tracks from albums (limit to max albums to avoid too many API calls)
	if len(trackIDs) == 0 && len(albumIDs) > 0 {
		maxAlbums := hotFetchMaxAlbums
		if maxAlbums > len(albumIDs) {
			maxAlbums = len(albumIDs)
		}
		for i := 0; i < maxAlbums; i++ {
			albumInfo, err := c.proxy.GetAlbumInfo(ctx, albumIDs[i])
			if err == nil && albumInfo != nil && len(albumInfo.Items) > 0 {
				// Add first few tracks from album
				for j, item := range albumInfo.Items {
					if j >= hotFetchMaxTracks || len(trackIDs) >= limit {
						break
					}
					trackIDs = appendUnique(trackIDs, item.ID)
				}
			}
		}
	}

	log.Printf("[RANDOM] Found %d tracks for genre %s", len(trackIDs), genre)

	// Cache the result with TTL
	if len(trackIDs) > 0 {
		c.genreCache.set(cacheKey, trackIDs, genreCacheTTL)
	}

	return trackIDs
}

func (c *Controller) ServeGetSimilarSongsTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	count := p.GetOrInt("count", 10)
	if count > similarSongsMaxCount {
		count = similarSongsMaxCount
	}

	id, err := p.GetID("id")
	if err != nil {
		return spec.NewError(10, "provide an `id` parameter")
	}

	var trackID int
	switch id.Type {
	case specid.Track:
		trackID = id.Value
	case specid.Artist:
		// fast timeout for artist
		ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
		page, err := c.proxy.GetArtistAlbums(ctx, id.Value, false)
		cancel()
		if err != nil || page == nil || len(page.Tracks) == 0 {
			return c.ServeGetRandomSongs(r)
		}
		trackID = page.Tracks[0].ID
	default:
		return c.ServeGetRandomSongs(r)
	}

	// fast timeout for recommendations
	ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
	recs, err := c.proxy.GetRecommendations(ctx, trackID)
	cancel()

	if err != nil || len(recs) == 0 {
		log.Printf("[DISC] GetSimilarSongs2 fallback to random (track %d)", trackID)
		return c.ServeGetRandomSongs(r)
	}

	if len(recs) > count {
		recs = recs[:count]
	}

	ids := make([]int, len(recs))
	for i := range recs {
		ids[i] = recs[i].ID
	}

	tracks := c.batchFetchTracks(r, ids)

	sub := spec.NewResponse()
	sub.SimilarSongsTwo = &spec.SimilarSongsTwo{Tracks: tracks}
	return sub
}

func (c *Controller) ServeGetSimilarSongs(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	count := p.GetOrInt("count", 10)
	if count > similarSongsMaxCount {
		count = similarSongsMaxCount
	}

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Track {
		// Only works with tracks - fallback to random songs
		return c.ServeGetRandomSongs(r)
	}

	// Try recommendations with short timeout
	ctx, cancel := context.WithTimeout(r.Context(), similarSongsTimeout)
	recs, err := c.proxy.GetRecommendations(ctx, id.Value)
	cancel()

	// If failed or empty, fallback to random songs quickly
	if err != nil {
		log.Printf("[DISC] GetRecommendations ERROR for track %d: %v", id.Value, err)
		return c.ServeGetRandomSongs(r)
	}
	if len(recs) == 0 {
		log.Printf("[DISC] GetRecommendations EMPTY for track %d", id.Value)
		return c.ServeGetRandomSongs(r)
	}

	if len(recs) > count {
		recs = recs[:count]
	}

	ids := make([]int, len(recs))
	for i := range recs {
		ids[i] = recs[i].ID
	}

	tracks := c.batchFetchTracks(r, ids)

	sub := spec.NewResponse()
	sub.SimilarSongs = &spec.SimilarSongs{Tracks: tracks}
	return sub
}

func (c *Controller) ServeGetTopSongs(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)
	count := p.GetOrInt("count", 10)

	artistName, err := p.Get("artist")
	if err != nil {
		return spec.NewError(10, "provide an `artist` parameter")
	}

	// search artist by name to get ID
	// fetch more candidates to find exact match if Tidal search is fuzzy
	candidates, err := c.proxy.SearchArtists(r.Context(), artistName, topSongsSearchCandidates, 0)
	if err != nil || len(candidates) == 0 {
		return spec.NewResponse()
	}

	artistID := 0
	for _, a := range candidates {
		if strings.EqualFold(a.Name, artistName) {
			artistID = a.ID
			break
		}
	}

	// If no exact match, fallback to first ONLY if it's a very similar name
	if artistID == 0 {
		first := candidates[0]
		if strings.Contains(strings.ToLower(first.Name), strings.ToLower(artistName)) ||
			strings.Contains(strings.ToLower(artistName), strings.ToLower(first.Name)) {
			artistID = first.ID
		}
	}

	if artistID == 0 {
		return spec.NewResponse()
	}

	// get artist top tracks (precise)
	topTracks, err := c.proxy.GetArtistTopTracks(r.Context(), artistID, count)
	if err != nil {
		log.Printf("[DISC] error fetching artist top tracks for %d: %v", artistID, err)
		// Fallback to searching tracks by artist name if this fails?
		// For now just return empty or error.
		return spec.NewResponse()
	}

	tracks := make([]*spec.TrackChild, len(topTracks))
	for i := range topTracks {
		tracks[i] = spec.NewTrackFromTidal(&topTracks[i])
		c.applyTrackStar(user.ID, tracks[i])
		c.applyTrackPlayCount(user.ID, tracks[i])
	}

	sub := spec.NewResponse()
	sub.TopSongs = &spec.TopSongs{Tracks: tracks}
	return sub
}
