package ctrlsubsonic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
)

// Genre mapping from hot.monochrome.tf
var hotGenres = []struct {
	ID   string
	Name string
}{
	{ID: "hip_hop", Name: "Hip-Hop"},
	{ID: "rnb", Name: "R&B / Soul"},
	{ID: "blues", Name: "Blues"},
	{ID: "classical", Name: "Classical"},
	{ID: "country", Name: "Country"},
	{ID: "dance_electronic", Name: "Dance & Electronic"},
	{ID: "americana", Name: "Folk / Americana"},
	{ID: "world", Name: "Global"},
	{ID: "gospel", Name: "Gospel / Christian"},
	{ID: "jazz", Name: "Jazz"},
	{ID: "kpop", Name: "K-Pop"},
	{ID: "kids", Name: "Kids"},
	{ID: "latin", Name: "Latin"},
	{ID: "metal", Name: "Metal"},
	{ID: "pop", Name: "Pop"},
	{ID: "reggae", Name: "Reggae / Dancehall"},
	{ID: "retro", Name: "Legacy"},
	{ID: "indierock", Name: "Rock / Indie"},
}

// hotTrack represents a track from hot.monochrome.tf
type hotTrack struct {
	ID       int    `json:"id"`
	Title    string `json:"title"`
	Artist   string `json:"artist"`
	Album    string `json:"album"`
	Cover    string `json:"cover"`
	Duration int    `json:"duration"`
}

// hotAlbum represents an album from hot.monochrome.tf
type hotAlbum struct {
	ID          int    `json:"id"`
	Title       string `json:"title"`
	Artist      string `json:"artist"`
	Cover       string `json:"cover"`
	ReleaseDate string `json:"releaseDate"`
}

// hotSection represents a section from hot.monochrome.tf
type hotSection struct {
	Title string `json:"title"`
	Type  string `json:"type"`
	Items []struct {
		ID       int    `json:"id"`
		Title    string `json:"title"`
		Artist   string `json:"artist,omitempty"`
		Album    string `json:"album,omitempty"`
		Cover    string `json:"cover"`
		Duration int    `json:"duration,omitempty"`
	} `json:"items"`
}

// hotGenreResponse represents the response from hot.monochrome.tf/explore/genre
type hotGenreResponse struct {
	Sections []hotSection `json:"sections"`
}

// cachedGenreCounts holds cached counts with timestamp
type cachedGenreCounts struct {
	counts    map[string]genreCount
	timestamp time.Time
}

var (
	genreCountsCache     *cachedGenreCounts
	genreCountsCacheMu   sync.RWMutex
	genreCountsCacheTTL  = 5 * time.Minute
)

func (c *Controller) ServeGetGenres(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	
	// Get cached counts (fetch fresh if expired)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	
	genreCounts := getCachedGenreCounts(ctx)
	
	// Convert hot genres to spec.Genre with counts
	genres := make([]*spec.Genre, 0, len(hotGenres))
	for _, g := range hotGenres {
		counts := genreCounts[g.ID]
		genres = append(genres, &spec.Genre{
			Name:       g.Name,
			SongCount:  counts.songs,
			AlbumCount: counts.albums,
		})
	}
	
	log.Printf("[GENRES] Returning %d genres with counts from hot.monochrome.tf", len(genres))
	
	sub.Genres = &spec.Genres{
		List: genres,
	}
	return sub
}

// genreCount holds counts for a genre
type genreCount struct {
	songs  int
	albums int
}

// getCachedGenreCounts returns cached counts or fetches new ones if expired
func getCachedGenreCounts(ctx context.Context) map[string]genreCount {
	// Check cache first
	genreCountsCacheMu.RLock()
	if genreCountsCache != nil && time.Since(genreCountsCache.timestamp) < genreCountsCacheTTL {
		cache := genreCountsCache.counts
		genreCountsCacheMu.RUnlock()
		log.Printf("[GENRES] Using cached counts")
		return cache
	}
	genreCountsCacheMu.RUnlock()
	
	// Fetch fresh counts
	log.Printf("[GENRES] Fetching fresh counts from hot.monochrome.tf")
	fresh := fetchFreshGenreCounts(ctx)
	
	// Update cache
	genreCountsCacheMu.Lock()
	genreCountsCache = &cachedGenreCounts{
		counts:    fresh,
		timestamp: time.Now(),
	}
	genreCountsCacheMu.Unlock()
	
	return fresh
}

// fetchFreshGenreCounts fetches counts for all genres concurrently
func fetchFreshGenreCounts(ctx context.Context) map[string]genreCount {
	result := make(map[string]genreCount)
	var mu sync.Mutex
	var wg sync.WaitGroup
	
	// Semaphore to limit concurrent requests
	sem := make(chan struct{}, 5)
	
	for _, g := range hotGenres {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			
			sem <- struct{}{}
			defer func() { <-sem }()
			
			counts := fetchSingleGenreCount(ctx, id)
			mu.Lock()
			result[id] = counts
			mu.Unlock()
		}(g.ID)
	}
	
	wg.Wait()
	return result
}

// fetchSingleGenreCount fetches counts for a single genre
func fetchSingleGenreCount(ctx context.Context, genreID string) genreCount {
	reqURL := fmt.Sprintf("%s/explore/genre/?id=%s", hotMonochromeURL, url.QueryEscape(genreID))
	
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return genreCount{}
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return genreCount{}
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return genreCount{}
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return genreCount{}
	}
	
	var data hotGenreResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return genreCount{}
	}
	
	var songs, albums int
	for _, section := range data.Sections {
		switch section.Type {
		case "TRACK_LIST":
			songs += len(section.Items)
		case "ALBUM_LIST":
			albums += len(section.Items)
		}
	}
	
	return genreCount{songs: songs, albums: albums}
}

func (c *Controller) ServeGetSongsByGenre(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)
	
	genreName, err := p.Get("genre")
	if err != nil || genreName == "" {
		return spec.NewError(10, "provide a genre parameter")
	}
	
	size := p.GetOrInt("count", 20)
	if size > 50 {
		size = 50
	}
	
	// Find genre ID from name
	var genreID string
	for _, g := range hotGenres {
		if g.Name == genreName {
			genreID = g.ID
			break
		}
	}
	
	// If found in hot genres, try hot.monochrome.tf first
	if genreID != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		
		tracks, err := fetchHotGenreTracks(ctx, genreID, size)
		if err == nil && len(tracks) > 0 {
			log.Printf("[GENRE] hot.monochrome.tf returned %d tracks for %s", len(tracks), genreName)
			
			// Convert hot tracks to TrackChild
			trackList := make([]*spec.TrackChild, 0, len(tracks))
			for _, t := range tracks {
				if t.ID != 0 {
					// Fetch full track info from Tidal
					trackInfo, _ := c.proxy.GetTrackInfo(r.Context(), t.ID)
					if trackInfo != nil {
						tc := spec.NewTrackFromTidal(trackInfo)
						c.applyTrackStar(user.ID, tc)
						trackList = append(trackList, tc)
					}
				}
			}
			
			if len(trackList) > 0 {
				sub := spec.NewResponse()
				sub.SongsByGenre = &spec.SongsByGenre{
					List: trackList,
				}
				return sub
			}
		}
		
		log.Printf("[GENRE] hot.monochrome.tf failed for %s, falling back to Tidal search", genreName)
	}
	
	// Fallback: Search tracks by genre name using Tidal
	ctx := r.Context()
	tracks, err := c.proxy.SearchTracks(ctx, genreName, size, 0)
	if err != nil {
		log.Printf("[GENRE] Tidal search failed: %v", err)
		sub := spec.NewResponse()
		sub.SongsByGenre = &spec.SongsByGenre{
			List: []*spec.TrackChild{},
		}
		return sub
	}
	
	// Convert to TrackChild
	trackList := make([]*spec.TrackChild, 0, len(tracks))
	for _, t := range tracks {
		if t.ID != 0 {
			tc := spec.NewTrackFromTidal(&t)
			c.applyTrackStar(user.ID, tc)
			trackList = append(trackList, tc)
		}
	}
	
	sub := spec.NewResponse()
	sub.SongsByGenre = &spec.SongsByGenre{
		List: trackList,
	}
	return sub
}

// fetchHotGenreTracks fetches tracks for a genre from hot.monochrome.tf
func fetchHotGenreTracks(ctx context.Context, genreID string, limit int) ([]hotTrack, error) {
	url := fmt.Sprintf("%s/explore/genre/?id=%s", hotMonochromeURL, url.QueryEscape(genreID))
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hot.monochrome.tf returned status %d", resp.StatusCode)
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	
	var data hotGenreResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	
	// Extract tracks from sections
	var tracks []hotTrack
	for _, section := range data.Sections {
		if section.Type == "TRACK_LIST" {
			for _, item := range section.Items {
				if item.ID != 0 {
					tracks = append(tracks, hotTrack{
						ID:       item.ID,
						Title:    item.Title,
						Artist:   item.Artist,
						Album:    item.Album,
						Cover:    item.Cover,
						Duration: item.Duration,
					})
					if len(tracks) >= limit {
						break
					}
				}
			}
		}
		if len(tracks) >= limit {
			break
		}
	}
	
	return tracks, nil
}

func (c *Controller) ServeGetInternetRadioStations(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.InternetRadioStations = &spec.InternetRadioStations{
		List: []*spec.InternetRadioStation{},
	}
	return sub
}

func (c *Controller) ServeGetNewestPodcasts(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.NewestPodcasts = &spec.NewestPodcasts{
		List: []*spec.PodcastEpisode{},
	}
	return sub
}

func (c *Controller) ServeGetPodcasts(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.Podcasts = &spec.Podcasts{
		List: []*spec.PodcastChannel{},
	}
	return sub
}
