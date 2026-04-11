package ctrlsubsonic

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
	"go.senan.xyz/gonic/tidalproxy"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (c *Controller) ServeGetIndexes(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	// Use virtual library: artists from stars + inferred from plays/playlist_tracks
	artistURIs := c.dbc.GetVirtualLibraryArtistIDs(user.ID)
	artistIDs := extractIDsFromURIs(artistURIs)

	artists := c.batchFetchArtists(r, artistIDs)
	indexes := c.buildArtistIndexes(artists)

	sub := spec.NewResponse()
	sub.Indexes = &spec.Indexes{
		Index: indexes,
	}
	return sub
}

func (c *Controller) buildArtistIndexes(artists []*spec.Artist) []*spec.Index {
	indexMap := make(map[string]*spec.Index)
	var indexes []*spec.Index

	for _, a := range artists {
		key := "#"
		if len(a.Name) > 0 {
			ch := []rune(a.Name)[0]
			if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
				key = strings.ToUpper(string(ch))
			}
		}

		if _, ok := indexMap[key]; !ok {
			idx := &spec.Index{Name: key, Artists: []*spec.Artist{}}
			indexMap[key] = idx
			indexes = append(indexes, idx)
		}
		indexMap[key].Artists = append(indexMap[key].Artists, a)
	}
	// Note: ideally sort indexes here by Name
	return indexes
}

func (c *Controller) ServeGetArtists(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	// Use virtual library: artists from stars + inferred from plays/playlist_tracks
	artistURIs := c.dbc.GetVirtualLibraryArtistIDs(user.ID)
	artistIDs := extractIDsFromURIs(artistURIs)

	artists := c.batchFetchArtists(r, artistIDs)
	indexes := c.buildArtistIndexes(artists)

	sub := spec.NewResponse()
	sub.Artists = &spec.Artists{List: indexes}
	return sub
}

func (c *Controller) ServeGetArtist(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Artist {
		return spec.NewError(10, "please provide an artist `id` parameter")
	}

	var info *tidalproxy.TidalArtistDetail
	var artistPage *tidalproxy.TidalArtistPage
	var errInfo, errPage error

	artistID := id.Value()
	done := make(chan struct{}, 2)
	go func() {
		info, errInfo = c.proxy.GetArtistInfo(r.Context(), artistID)
		done <- struct{}{}
	}()
	go func() {
		artistPage, errPage = c.proxy.GetArtistAlbums(r.Context(), artistID, true)
		done <- struct{}{}
	}()

	<-done
	<-done

	if errInfo != nil {
		return spec.NewError(0, "error fetching artist: %v", errInfo)
	}
	if errPage != nil {
		return spec.NewError(0, "error fetching artist albums: %v", errPage)
	}

	artist := spec.NewArtistFromTidal(&info.Artist)
	c.applyArtistStar(user.ID, artist)

	items := artistPage.Albums.Items
	// Deduplicate albums by title+release_date (Tidal API returns same album with different IDs)
	seenAlbums := make(map[string]bool)
	var uniqueItems []tidalproxy.TidalAlbum
	for _, item := range items {
		key := fmt.Sprintf("%s|%s", item.Title, item.ReleaseDate)
		if !seenAlbums[key] {
			seenAlbums[key] = true
			uniqueItems = append(uniqueItems, item)
		}
	}
	items = uniqueItems

	artist.AlbumCount = len(items)
	artist.Albums = make([]*spec.Album, len(items))
	for i := range items {
		artist.Albums[i] = spec.NewAlbumFromTidal(&items[i])
		c.applyAlbumStar(user.ID, artist.Albums[i])
	}

	sub := spec.NewResponse()
	sub.Artist = artist
	return sub
}

func (c *Controller) ServeGetAlbum(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Album {
		return spec.NewError(10, "please provide an album `id` parameter")
	}

	album, err := c.proxy.GetAlbumInfo(r.Context(), id.Value())
	if err != nil {
		return spec.NewError(0, "error fetching album: %v", err)
	}

	a := spec.NewAlbumFromTidal(album)
	a.TrackCount = len(album.Items)
	a.Tracks = make([]*spec.TrackChild, len(album.Items))

	totalDuration := 0
	for i := range album.Items {
		tc := spec.NewTrackFromTidal(&album.Items[i])
		// fill in album context that track might be missing
		if tc.Album == "" {
			tc.Album = album.Title
		}
		if tc.AlbumID == nil {
			tc.AlbumID = a.ID
		}
		c.applyTrackStar(user.ID, tc)
		uri := fmt.Sprintf("td:tr:%d", album.Items[i].ID)
		tc.UserRating = c.getTrackRating(user.ID, uri)
		c.applyTrackPlayCount(user.ID, tc)
		a.Tracks[i] = tc
		totalDuration += tc.Duration
	}
	if a.Duration == 0 {
		a.Duration = totalDuration
	}

	c.applyAlbumStar(user.ID, a)
	albumURI := id.String()
	a.UserRating = c.getAlbumRating(user.ID, albumURI)

	sub := spec.NewResponse()
	sub.Album = a
	return sub
}

func (c *Controller) ServeGetAlbumListTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	listType, err := p.Get("type")
	if err != nil {
		return spec.NewError(10, "please provide a `type` parameter")
	}

	size := p.GetOrInt("size", 10)
	offset := p.GetOrInt("offset", 0)

	// Normalize offset to be multiple of size to avoid duplicate items from buggy clients
	// Round UP to next multiple to avoid showing same items again
	if offset > 0 && offset%size != 0 {
		offset = ((offset / size) + 1) * size
	}

	// Hard limit for discovery endpoints to prevent infinite sync loops
	// These endpoints call external APIs (hot.monochrome/Tidal) and should be capped
	discoveryTypes := map[string]bool{
		"newest":   true,
		"random":   true,
		"recent":   true,
		"frequent": true,
		"byGenre":  true,
	}
	if discoveryTypes[listType] && offset >= 200 {
		log.Printf("[BROWSE] Hard limit reached for discovery type %s at offset %d", listType, offset)
		sub := spec.NewResponse()
		sub.AlbumsTwo = &spec.Albums{List: []*spec.Album{}}
		return sub
	}

	var albumIDs []int

	switch listType {
	case "starred", "newest":
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, extractIDFromURI(s.URI))
		}

	case "recent":
		// Recently played favorited albums (by LastPlayed first, then star_date for never played)
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("last_played DESC, star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, extractIDFromURI(s.URI))
		}
		// Fallback to hot new releases if less than threshold local albums
		if len(albumIDs) < hotFallbackThresholdRecent {
			albumIDs = c.fetchHotFallback(r.Context(), albumIDs, size-len(albumIDs), "new", "recent")
		}

	case "frequent":
		// Most played favorited albums (by PlayCount, 0 at end)
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("play_count DESC, star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, extractIDFromURI(s.URI))
		}
		// Fallback to hot trending if less than threshold local albums
		if len(albumIDs) < hotFallbackThresholdRecent {
			albumIDs = c.fetchHotFallback(r.Context(), albumIDs, size-len(albumIDs), "trending", "frequent")
		}

	case "alphabeticalByName", "alphabeticalByArtist":
		// Use virtual library: albums from stars + inferred from plays/playlist_tracks
		albumURIs := c.dbc.GetVirtualLibraryAlbumIDs(user.ID)

		if len(albumURIs) == 0 {
			break
		}

		// Fetch all album metadata to sort properly
		allAlbumIDs := extractIDsFromURIs(albumURIs)

		// Use fast fetch with timeout
		ctx, cancel := context.WithTimeout(r.Context(), hotFetchTimeout)
		allAlbums := c.batchFetchAlbumsWithContext(ctx, allAlbumIDs)
		cancel()

		// Sort by name or artist
		if listType == "alphabeticalByName" {
			sort.Slice(allAlbums, func(i, j int) bool {
				return strings.ToLower(allAlbums[i].Name) < strings.ToLower(allAlbums[j].Name)
			})
		} else {
			sort.Slice(allAlbums, func(i, j int) bool {
				return strings.ToLower(allAlbums[i].Artist) < strings.ToLower(allAlbums[j].Artist)
			})
		}

		// Apply offset/limit
		start := offset
		if start >= len(allAlbums) {
			albumIDs = nil
		} else {
			end := start + size
			if end > len(allAlbums) {
				end = len(allAlbums)
			}
			for _, a := range allAlbums[start:end] {
				if a.ID != nil {
					albumIDs = append(albumIDs, a.ID.Value())
				}
			}
		}

	case "random":
		// Fast random - limit to avoid timeout
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("RANDOM()").
			Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, extractIDFromURI(s.URI))
		}
		// Fallback to hot popular albums if less than threshold local albums
		if len(albumIDs) < hotFallbackThresholdRandom {
			albumIDs = c.fetchHotFallback(r.Context(), albumIDs, size-len(albumIDs), "popular", "random")
		}

	case "highest":
		var ratings []db.AlbumRating
		c.dbc.Where("user_id=?", user.ID).
			Order("rating DESC").
			Offset(offset).Limit(size).
			Find(&ratings)
		for _, r := range ratings {
			albumIDs = append(albumIDs, extractIDFromURI(r.URI))
		}

	case "byYear":
		// Filter albums by year range
		fromYear := p.GetOrInt("fromYear", 0)
		toYear := p.GetOrInt("toYear", 3000)
		// Determine actual min/max for filtering
		minYear, maxYear := fromYear, toYear
		if fromYear > toYear {
			minYear, maxYear = toYear, fromYear
		}
		// Get all starred albums
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).Find(&stars)
		if len(stars) == 0 {
			break
		}

		// Fetch album metadata to get years
		allAlbumIDs := make([]int, len(stars))
		for i, s := range stars {
			allAlbumIDs[i] = extractIDFromURI(s.URI)
		}
		ctx, cancel := context.WithTimeout(r.Context(), hotFetchTimeout)
		allAlbums := c.batchFetchAlbumsWithContext(ctx, allAlbumIDs)
		cancel()

		// Filter by year range
		var filtered []*spec.Album
		for _, a := range allAlbums {
			if a.Year >= minYear && a.Year <= maxYear {
				filtered = append(filtered, a)
			}
		}
		// Sort by year (ascending if fromYear < toYear, descending otherwise)
		if fromYear < toYear {
			sort.Slice(filtered, func(i, j int) bool {
				return filtered[i].Year < filtered[j].Year
			})
		} else {
			sort.Slice(filtered, func(i, j int) bool {
				return filtered[i].Year > filtered[j].Year
			})
		}
		// Apply offset/limit
		start := offset
		if start < len(filtered) {
			end := start + size
			if end > len(filtered) {
				end = len(filtered)
			}
			for _, a := range filtered[start:end] {
				if a.ID != nil {
					albumIDs = append(albumIDs, a.ID.Value())
				}
			}
		}

	case "byGenre":
		genre, _ := p.Get("genre")
		if genre == "" {
			break
		}
		// Cap max albums per genre to avoid infinite scroll issues
		maxGenreAlbums := genreFetchMaxCount
		if offset >= maxGenreAlbums {
			log.Printf("[GENRE] Max albums reached for %s at offset %d", genre, offset)
			break
		}
		// Limit size to not exceed max
		if offset+size > maxGenreAlbums {
			size = maxGenreAlbums - offset
		}
		// Map genre name to hot.monochrome.tf ID
		genreID := hotGenreMapping[genre]
		if genreID == "" {
			genreID = strings.ToLower(strings.ReplaceAll(genre, " ", "_"))
		}

		// Try hot.monochrome.tf first for discovery - use cache with deduplication
		cacheKey := fmt.Sprintf("genre_albums_%s", genreID)
		allAlbums := c.genreAlbumCache.Get(cacheKey)
		if len(allAlbums) == 0 {
			// Deduplication: only one request fetches, others wait
			lockVal, loaded := c.hotLocks.LoadOrStore(cacheKey, &hotLockPair{done: make(chan struct{})})
			lp := lockVal.(*hotLockPair)

			if loaded {
				// Another request is in flight, wait for it
				<-lp.done
				allAlbums = c.genreAlbumCache.Get(cacheKey)
			} else {
				// We are the fetcher - do the work
				allAlbums = c.fetchHotAlbumsWithFilter(r.Context(), maxGenreAlbums, "new", genreID)
				if len(allAlbums) > 0 {
					c.genreAlbumCache.Set(cacheKey, allAlbums, 10*time.Minute)
					log.Printf("[GENRE] fetched and cached %d albums for %s", len(allAlbums), cacheKey)
				}
				close(lp.done)
				// Cleanup lock after brief delay
				go func() {
					time.Sleep(100 * time.Millisecond)
					c.hotLocks.Delete(cacheKey)
				}()
			}
		}

		if len(allAlbums) > 0 {
			log.Printf("[GENRE] hot.monochrome.tf returned %d albums for %s", len(allAlbums), genre)
			// Apply offset and size locally
			if offset < len(allAlbums) {
				end := offset + size
				if end > len(allAlbums) {
					end = len(allAlbums)
				}
				albums := allAlbums[offset:end]
				// Convert to album IDs
				for _, a := range albums {
					if a.ID != 0 {
						albumIDs = append(albumIDs, a.ID)
					}
				}
			}
		} else {
			log.Printf("[GENRE] hot.monochrome.tf exhausted for %s at offset %d", genre, offset)
			// Try Tidal search as fallback
			albums, err := c.proxy.SearchAlbums(r.Context(), genre, size, offset)
			if err == nil {
				for _, a := range albums {
					if a.ID != 0 {
						albumIDs = append(albumIDs, a.ID)
					}
				}
			}
		}

	default:
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, extractIDFromURI(s.URI))
		}
	}

	// batch fetch album metadata
	albums := c.batchFetchAlbums(r, albumIDs)

	sub := spec.NewResponse()
	sub.AlbumsTwo = &spec.Albums{List: albums}
	return sub
}

func (c *Controller) ServeGetSong(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Track {
		return spec.NewError(10, "please provide a track `id` parameter")
	}

	track, err := c.proxy.GetTrackInfo(r.Context(), id.Value())
	if err != nil {
		return spec.NewError(0, "error fetching track: %v", err)
	}

	tc := spec.NewTrackFromTidal(track)
	c.applyTrackStar(user.ID, tc)
	uri := id.String()
	tc.UserRating = c.getTrackRating(user.ID, uri)

	sub := spec.NewResponse()
	sub.Track = tc
	return sub
}

func appendUnique(slice []int, val int) []int {
	for _, v := range slice {
		if v == val {
			return slice
		}
	}
	return append(slice, val)
}

// hotLockPair is used for deduplicating concurrent hot.monochrome.tf requests
type hotLockPair struct {
	mu     sync.Mutex
	done   chan struct{}
	albums []tidalproxy.TidalAlbum
	status int // 0=pending, 1=success, 2=error
}

// fetchHotFallback fetches albums from hot.monochrome.tf and appends them to existing albumIDs
// when the local library doesn't have enough albums. This provides a seamless fallback
// to external content for discovery purposes.
// Uses singleflight pattern to deduplicate concurrent requests.
func (c *Controller) fetchHotFallback(ctx context.Context, albumIDs []int, needed int, filter string, logType string) []int {
	if needed <= 0 {
		return albumIDs
	}
	log.Printf("[BROWSE] %s: only %d local albums, fetching %d %s from hot.monochrome.tf", logType, len(albumIDs), needed, filter)

	// Use cache key based on filter type
	cacheKey := fmt.Sprintf("fallback_%s", filter)

	// Fast path: check cache first
	cachedAlbums := c.genreAlbumCache.Get(cacheKey)
	if len(cachedAlbums) > 0 {
		log.Printf("[BROWSE] cache hit for %s", cacheKey)
		return appendAlbums(albumIDs, cachedAlbums, needed)
	}

	// Deduplication: only one request fetches, others wait
	lockVal, loaded := c.hotLocks.LoadOrStore(cacheKey, &hotLockPair{done: make(chan struct{})})
	lp := lockVal.(*hotLockPair)

	if loaded {
		// Another request is in flight, wait for it
		<-lp.done
		lp.mu.Lock()
		if lp.status == 1 && len(lp.albums) > 0 {
			// Use fetched results (already cached by the fetcher)
			cachedAlbums = c.genreAlbumCache.Get(cacheKey)
		}
		lp.mu.Unlock()
		return appendAlbums(albumIDs, cachedAlbums, needed)
	}

	// We are the fetcher - do the work
	// Use random genre for non-specific fallback cases
	genres := []string{"pop", "electronic", "rock", "hip_hop", "rnb"}
	genre := genres[rand.Intn(len(genres))]

	fetchedAlbums := c.fetchHotAlbumsWithFilter(ctx, 50, filter, genre)

	lp.mu.Lock()
	if len(fetchedAlbums) > 0 {
		c.genreAlbumCache.Set(cacheKey, fetchedAlbums, 15*time.Minute)
		lp.albums = fetchedAlbums
		lp.status = 1
		log.Printf("[BROWSE] fetched and cached %d albums for %s", len(fetchedAlbums), cacheKey)
	} else {
		lp.status = 2
	}
	close(lp.done)
	lp.mu.Unlock()

	// Cleanup lock after brief delay to allow other waiters to finish
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.hotLocks.Delete(cacheKey)
	}()

	// Get from cache (which now has the data)
	cachedAlbums = c.genreAlbumCache.Get(cacheKey)
	return appendAlbums(albumIDs, cachedAlbums, needed)
}

// appendAlbums appends unique albums from source to dest, up to needed count
func appendAlbums(dest []int, source []tidalproxy.TidalAlbum, needed int) []int {
	for _, album := range source {
		if len(dest) >= needed {
			break
		}
		if album.ID != 0 {
			dest = appendUnique(dest, album.ID)
		}
	}
	return dest
}

// fetchHotAlbumsWithFilter fetches albums from hot.monochrome.tf with a specific filter.
// It uses the specified genre ID to fetch albums from the API, then batch fetches
// full album metadata using concurrent goroutines with a semaphore for rate limiting.
func (c *Controller) fetchHotAlbumsWithFilter(ctx context.Context, limit int, filter string, genreID string) []tidalproxy.TidalAlbum {
	// Validate and cap limit to prevent excessive requests
	const maxFetchLimit = 50
	if limit <= 0 {
		return nil
	}
	if limit > maxFetchLimit {
		limit = maxFetchLimit
		log.Printf("[BROWSE] Limit capped to %d for filter %s", maxFetchLimit, filter)
	}

	// Use the specified genre ID
	url := fmt.Sprintf("%s/explore/genre/?id=%s", hotMonochromeURL, genreID)

	var result hotResponse

	if err := fetchJSON(ctx, c.httpClient, url, "BROWSE", &result); err != nil {
		log.Printf("[BROWSE] Error fetching albums from hot.monochrome.tf: %v", err)
		return nil
	}

	// Collect album IDs from different sources
	var albumIDs []int

	// Priority 1: new_releases (most relevant)
	for _, album := range result.NewReleases {
		if album.StreamReady {
			albumIDs = appendUnique(albumIDs, album.ID)
		}
	}

	// Priority 2: ALBUM_LIST sections
	for _, section := range result.Sections {
		if section.Type == "ALBUM_LIST" {
			for _, item := range section.Items {
				if item.ID != 0 {
					albumIDs = appendUnique(albumIDs, item.ID)
				}
			}
		}
	}

	if len(albumIDs) == 0 {
		return nil
	}

	// Limit to requested amount
	if len(albumIDs) > limit {
		albumIDs = albumIDs[:limit]
	}

	// Batch fetch albums with context awareness
	albumChan := make(chan *tidalproxy.TidalAlbum, len(albumIDs))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, hotFetchConcurrency)

	for _, id := range albumIDs {
		wg.Add(1)
		go func(albumID int) {
			defer wg.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()

				album, err := c.proxy.GetAlbumInfo(ctx, albumID)
				if err == nil && album != nil {
					albumChan <- album
				}
			case <-ctx.Done():
				// Context cancelled, skip this request
			}
		}(id)
	}

	wg.Wait()
	close(albumChan)

	var albums []tidalproxy.TidalAlbum
	for album := range albumChan {
		albums = append(albums, *album)
	}

	return albums
}

func (c *Controller) ServeGetArtistInfoTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Artist {
		return spec.NewError(10, "please provide an artist `id` parameter")
	}

	info, err := c.proxy.GetArtistInfo(r.Context(), id.Value())
	if err != nil {
		return spec.NewError(0, "error fetching artist info")
	}

	similar, _ := c.proxy.GetSimilarArtists(r.Context(), id.Value())

	artistInfo := &spec.ArtistInfo{
		Biography:      "",
		SmallImageURL:  c.proxy.GetCoverURL(info.Artist.Picture, 320),
		MediumImageURL: c.proxy.GetCoverURL(info.Artist.Picture, 640),
		LargeImageURL:  c.proxy.GetCoverURL(info.Artist.Picture, 1280),
		ArtistImageURL: c.proxy.GetCoverURL(info.Artist.Picture, 1280),
	}

	user := r.Context().Value(CtxUser).(*db.User)
	for _, a := range similar {
		sa := spec.NewArtistFromTidal(&a)
		c.applyArtistStar(user.ID, sa)
		artistInfo.Similar = append(artistInfo.Similar, sa)
	}

	sub := spec.NewResponse()
	if strings.Contains(r.URL.Path, "getArtistInfo2") {
		sub.ArtistInfoTwo = artistInfo
	} else {
		sub.ArtistInfo = artistInfo
	}
	return sub
}

func (c *Controller) ServeGetAlbumInfoTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type() != specid.Album {
		return spec.NewError(10, "please provide an album `id` parameter")
	}

	albumInfo := &spec.AlbumInfo{
		Notes:         "",
		MusicBrainzID: "",
		LastFMURL:     "",
	}

	sub := spec.NewResponse()
	sub.AlbumInfo = albumInfo
	return sub
}
