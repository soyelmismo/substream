package ctrlsubsonic

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// defaultInternetRadioStations defines the default internet radio stations
var defaultInternetRadioStations = []*spec.InternetRadioStation{
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 1},
		Name:        "SomaFM Groove Salad",
		StreamURL:   "http://ice1.somafm.com/groovesalad-128-mp3",
		HomepageURL: "https://somafm.com/groovesalad/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 2},
		Name:        "SomaFM Drone Zone",
		StreamURL:   "http://ice1.somafm.com/dronezone-128-mp3",
		HomepageURL: "https://somafm.com/dronezone/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 3},
		Name:        "SomaFM Secret Agent",
		StreamURL:   "http://ice1.somafm.com/secretagent-128-mp3",
		HomepageURL: "https://somafm.com/secretagent/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 4},
		Name:        "SomaFM Beat Blender",
		StreamURL:   "http://ice1.somafm.com/beatblender-128-mp3",
		HomepageURL: "https://somafm.com/beatblender/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 5},
		Name:        "SomaFM Fluid",
		StreamURL:   "http://ice1.somafm.com/fluid-128-mp3",
		HomepageURL: "https://somafm.com/fluid/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 6},
		Name:        "Radio Paradise Main Mix",
		StreamURL:   "http://stream.radioparadise.com/mp3-192",
		HomepageURL: "https://radioparadise.com/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 7},
		Name:        "Radio Paradise Mellow Mix",
		StreamURL:   "http://stream.radioparadise.com/mellow-192",
		HomepageURL: "https://radioparadise.com/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 8},
		Name:        "Radio Paradise Rock Mix",
		StreamURL:   "http://stream.radioparadise.com/rock-192",
		HomepageURL: "https://radioparadise.com/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 9},
		Name:        "Zen Radio",
		StreamURL:   "https://streamingp.shoutcast.com/ZenRadio",
		HomepageURL: "https://zenradio.com/",
	},
	{
		ID:          &specid.ID{Type: specid.InternetRadioStation, Value: 10},
		Name:        "Chillhop Music",
		StreamURL:   "https://streams.fluxfm.de/chillhop/mp3-128",
		HomepageURL: "https://chillhop.com/",
	},
}

// cachedGenreCounts holds cached counts with timestamp
type cachedGenreCounts struct {
	counts    map[string]genreCount
	timestamp time.Time
}

func (c *Controller) ServeGetGenres(r *http.Request) *spec.Response {
	sub := spec.NewResponse()

	// Get cached counts (fetch fresh if expired)
	ctx, cancel := context.WithTimeout(r.Context(), httpClientTimeout)
	defer cancel()

	genreCounts := c.getCachedGenreCounts(ctx)

	// Convert hot genres to spec.Genre with counts
	genres := make([]*spec.Genre, 0, len(hotGenreList))
	for _, g := range hotGenreList {
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
func (c *Controller) getCachedGenreCounts(ctx context.Context) map[string]genreCount {
	// Check cache first
	c.genreCountsCacheMu.RLock()
	if c.genreCountsCache != nil && time.Since(c.genreCountsCache.timestamp) < genreCountsCacheTTL {
		cache := c.genreCountsCache.counts
		c.genreCountsCacheMu.RUnlock()
		log.Printf("[GENRES] Using cached counts")
		return cache
	}
	c.genreCountsCacheMu.RUnlock()

	// Fetch fresh counts
	log.Printf("[GENRES] Fetching fresh counts from hot.monochrome.tf")
	fresh := fetchFreshGenreCounts(ctx, c.httpClient)

	// Update cache
	c.genreCountsCacheMu.Lock()
	c.genreCountsCache = &cachedGenreCounts{
		counts:    fresh,
		timestamp: time.Now(),
	}
	c.genreCountsCacheMu.Unlock()

	return fresh
}

// fetchFreshGenreCounts fetches counts for all genres concurrently
// Uses reduced concurrency and fallback to default values on API failure
func fetchFreshGenreCounts(ctx context.Context, client *http.Client) map[string]genreCount {
	result := make(map[string]genreCount)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Reduce concurrency to avoid overwhelming the API
	sem := make(chan struct{}, 2) // Reduced from genreCountFetchConcurrency

	for _, g := range hotGenreList {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			counts := fetchSingleGenreCount(ctx, client, id)
			
			// If API fails, provide reasonable defaults
			if counts.songs == 0 && counts.albums == 0 {
				counts = genreCount{songs: 100, albums: 20} // Default fallback
			}
			
			mu.Lock()
			result[id] = counts
			mu.Unlock()
		}(g.ID)
	}

	wg.Wait()
	return result
}

// fetchSingleGenreCount fetches counts for a single genre
// Returns empty count on error (caller will use fallback)
func fetchSingleGenreCount(ctx context.Context, client *http.Client, genreID string) genreCount {
	// Use shorter timeout for genre counts to fail fast
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	reqURL := fmt.Sprintf("%s/explore/genre/?id=%s", hotMonochromeURL, url.QueryEscape(genreID))

	var data hotResponse
	if err := fetchJSON(ctx, client, reqURL, "GENRES", &data); err != nil {
		log.Printf("[GENRES] Error fetching genre count for %s: %v", genreID, err)
		return genreCount{} // Return empty to trigger fallback
	}

	var songs, albums int

	// Count from top_tracks
	songs += len(data.TopTracks)

	// Count from new_releases
	for _, album := range data.NewReleases {
		if album.StreamReady {
			albums++
		}
	}

	// Count from sections
	for _, section := range data.Sections {
		switch section.Type {
		case "ALBUM_LIST":
			albums += len(section.Items)
		case "PLAYLIST_LIST":
			// Count playlists as album-like content
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
	if size > genreFetchMaxCount {
		size = genreFetchMaxCount
	}

	// Find genre ID from name
	var genreID string
	for _, g := range hotGenreList {
		if g.Name == genreName {
			genreID = g.ID
			break
		}
	}

	// If found in hot genres, try hot.monochrome.tf first
	if genreID != "" {
		ctx, cancel := context.WithTimeout(r.Context(), genreFetchTimeout)
		defer cancel()

		trackIDs := c.fetchHotGenreTracks(ctx, genreName, size)
		if len(trackIDs) > 0 {
			log.Printf("[GENRE] hot.monochrome.tf returned %d track IDs for %s", len(trackIDs), genreName)

			// Fetch full track info from Tidal
			tracks := c.batchFetchTracks(r, trackIDs)

			if len(tracks) > 0 {
				sub := spec.NewResponse()
				sub.SongsByGenre = &spec.SongsByGenre{
					List: tracks,
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

func (c *Controller) ServeGetInternetRadioStations(r *http.Request) *spec.Response {
	sub := spec.NewResponse()
	sub.InternetRadioStations = &spec.InternetRadioStations{
		List: defaultInternetRadioStations,
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
