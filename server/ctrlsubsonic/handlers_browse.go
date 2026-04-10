package ctrlsubsonic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
	"go.senan.xyz/gonic/tidalproxy"
)

const hotMonochromeURL = "https://hot.monochrome.tf"

// hotAlbumItem represents an album item from hot.monochrome.tf
type hotAlbumItem struct {
	ID     int    `json:"id"`
	Title  string `json:"title"`
	Artist string `json:"artist"`
}

// hotAlbumSection represents a section from hot.monochrome.tf
type hotAlbumSection struct {
	Title string           `json:"title"`
	Type  string           `json:"type"`
	Items []hotAlbumItem   `json:"items"`
}

// hotAlbumResponse represents the response from hot.monochrome.tf/explore/genre
type hotAlbumResponse struct {
	Sections []hotAlbumSection `json:"sections"`
}

// fetchHotGenreAlbums fetches albums for a genre from hot.monochrome.tf
func fetchHotGenreAlbums(ctx context.Context, genreName string, limit int) []hotAlbumItem {
	// Map genre name to hot ID
	genreMap := map[string]string{
		"Hip-Hop": "hip_hop", "R&B / Soul": "rnb", "Blues": "blues",
		"Classical": "classical", "Country": "country", "Dance & Electronic": "dance_electronic",
		"Folk / Americana": "americana", "Global": "world", "Gospel / Christian": "gospel",
		"Jazz": "jazz", "K-Pop": "kpop", "Kids": "kids", "Latin": "latin",
		"Metal": "metal", "Pop": "pop", "Reggae / Dancehall": "reggae",
		"Legacy": "retro", "Rock / Indie": "indierock",
	}
	
	genreID, ok := genreMap[genreName]
	if !ok {
		// Try generic search if genre not in hot list
		return nil
	}
	
	reqURL := fmt.Sprintf("%s/explore/genre/?id=%s", hotMonochromeURL, url.QueryEscape(genreID))
	
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	
	var data hotAlbumResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return nil
	}
	
	// Extract albums from sections
	var albums []hotAlbumItem
	for _, section := range data.Sections {
		if section.Type == "ALBUM_LIST" {
			for _, item := range section.Items {
				if item.ID != 0 {
					albums = append(albums, item)
					if len(albums) >= limit {
						break
					}
				}
			}
		}
		if len(albums) >= limit {
			break
		}
	}
	
	return albums
}

func (c *Controller) ServeGetIndexes(r *http.Request) *spec.Response {
	user := r.Context().Value(CtxUser).(*db.User)

	var stars []db.ArtistStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Find(&stars)

	starIDs := make([]int, len(stars))
	for i, s := range stars {
		starIDs[i] = s.TidalID
	}

	artists := c.batchFetchArtists(r, starIDs)
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

	var stars []db.ArtistStar
	c.dbc.Where("user_id=?", user.ID).Order("star_date DESC").Find(&stars)

	starIDs := make([]int, len(stars))
	for i, s := range stars {
		starIDs[i] = s.TidalID
	}

	artists := c.batchFetchArtists(r, starIDs)
	indexes := c.buildArtistIndexes(artists)

	sub := spec.NewResponse()
	sub.Artists = &spec.Artists{List: indexes}
	return sub
}

func (c *Controller) ServeGetArtist(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	user := r.Context().Value(CtxUser).(*db.User)

	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Artist {
		return spec.NewError(10, "please provide an artist `id` parameter")
	}

	var info *tidalproxy.TidalArtistDetail
	var artistPage *tidalproxy.TidalArtistPage
	var errInfo, errPage error

	done := make(chan struct{}, 2)
	go func() {
		info, errInfo = c.proxy.GetArtistInfo(r.Context(), id.Value)
		done <- struct{}{}
	}()
	go func() {
		artistPage, errPage = c.proxy.GetArtistAlbums(r.Context(), id.Value, true)
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
	if err != nil || id.Type != specid.Album {
		return spec.NewError(10, "please provide an album `id` parameter")
	}

	album, err := c.proxy.GetAlbumInfo(r.Context(), id.Value)
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
		tc.UserRating = c.getTrackRating(user.ID, album.Items[i].ID)
		c.applyTrackPlayCount(user.ID, tc)
		a.Tracks[i] = tc
		totalDuration += tc.Duration
	}
	if a.Duration == 0 {
		a.Duration = totalDuration
	}

	c.applyAlbumStar(user.ID, a)
	a.UserRating = c.getAlbumRating(user.ID, id.Value)

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

	var albumIDs []int

	switch listType {
	case "starred", "newest":
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, s.TidalID)
		}

	case "recent":
		// Recently played favorited albums (by LastPlayed first, then star_date for never played)
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("last_played DESC, star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, s.TidalID)
		}

	case "frequent":
		// Most played favorited albums (by PlayCount, 0 at end)
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("play_count DESC, star_date DESC").
			Offset(offset).Limit(size).
			Find(&stars)
		for _, s := range stars {
			albumIDs = append(albumIDs, s.TidalID)
		}

	case "alphabeticalByName", "alphabeticalByArtist":
		// Get all starred albums
		var stars []db.AlbumStar
		c.dbc.Where("user_id=?", user.ID).
			Order("star_date DESC").
			Find(&stars)
		
		if len(stars) == 0 {
			break
		}
		
		// Fetch all album metadata to sort properly
		allAlbumIDs := make([]int, len(stars))
		for i, s := range stars {
			allAlbumIDs[i] = s.TidalID
		}
		
		// Use fast fetch with timeout
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
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
					albumIDs = append(albumIDs, a.ID.Value)
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
			albumIDs = append(albumIDs, s.TidalID)
		}

	case "highest":
		var ratings []db.AlbumRating
		c.dbc.Where("user_id=?", user.ID).
			Order("rating DESC").
			Offset(offset).Limit(size).
			Find(&ratings)
		for _, r := range ratings {
			albumIDs = append(albumIDs, r.TidalID)
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
			allAlbumIDs[i] = s.TidalID
		}
		
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
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
					albumIDs = append(albumIDs, a.ID.Value)
				}
			}
		}

	case "byGenre":
		genre, _ := p.Get("genre")
		if genre == "" {
			break
		}
		
		// Cap max albums per genre to avoid infinite scroll issues
		const maxGenreAlbums = 50
		if offset >= maxGenreAlbums {
			log.Printf("[GENRE] Max albums reached for %s at offset %d", genre, offset)
			break
		}
		
		// Limit size to not exceed max
		if offset+size > maxGenreAlbums {
			size = maxGenreAlbums - offset
		}
		
		// Try hot.monochrome.tf first for discovery
		hotAlbums := fetchHotGenreAlbums(r.Context(), genre, maxGenreAlbums)
		if len(hotAlbums) > offset {
			log.Printf("[GENRE] hot.monochrome.tf returned %d albums for %s (offset %d, size %d)", len(hotAlbums), genre, offset, size)
			// Apply offset/limit locally with deduplication
			start := offset
			end := offset + size
			if end > len(hotAlbums) {
				end = len(hotAlbums)
			}
			seen := make(map[int]bool)
			for _, a := range hotAlbums[start:end] {
				if a.ID != 0 && !seen[a.ID] {
					seen[a.ID] = true
					albumIDs = append(albumIDs, a.ID)
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
			albumIDs = append(albumIDs, s.TidalID)
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
	if err != nil || id.Type != specid.Track {
		return spec.NewError(10, "please provide a track `id` parameter")
	}

	track, err := c.proxy.GetTrackInfo(r.Context(), id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching track: %v", err)
	}

	tc := spec.NewTrackFromTidal(track)
	c.applyTrackStar(user.ID, tc)
	tc.UserRating = c.getTrackRating(user.ID, id.Value)

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

func (c *Controller) ServeGetArtistInfoTwo(r *http.Request) *spec.Response {
	p := r.Context().Value(CtxParams).(params.Params)
	id, err := p.GetID("id")
	if err != nil || id.Type != specid.Artist {
		return spec.NewError(10, "please provide an artist `id` parameter")
	}

	info, err := c.proxy.GetArtistInfo(r.Context(), id.Value)
	if err != nil {
		return spec.NewError(0, "error fetching artist info")
	}

	similar, _ := c.proxy.GetSimilarArtists(r.Context(), id.Value)

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
	if err != nil || id.Type != specid.Album {
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
