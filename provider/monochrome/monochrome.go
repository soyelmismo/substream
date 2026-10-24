package monochrome

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider for Monochrome
type Provider struct {
	client     *Client
	trackCache sync.Map // trackID -> *spec.TrackChild
	coverCache sync.Map // rawID -> coverURL
}

var (
	_ provider.MusicProvider = (*Provider)(nil)
	_ provider.ISRCResolver  = (*Provider)(nil)
)

// New creates a new Monochrome provider
func New(opts ...ClientOptions) *Provider {
	return &Provider{
		client: NewClient(opts...),
	}
}

func (p *Provider) ID() string {
	return "mc"
}

func (p *Provider) Name() string {
	return "Monochrome"
}

func parseYear(dateStr string) int {
	if len(dateStr) >= 4 {
		if y, err := strconv.Atoi(dateStr[:4]); err == nil {
			return y
		}
	}
	return 0
}

func (p *Provider) trackToSpec(t *MonochromeTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	rawID := t.GetTrackID()
	if rawID == "" {
		return nil
	}

	trackID := &specid.ID{URI: fmt.Sprintf("mc:tr:%s", rawID)}

	albumTitle := t.AlbumTitle
	if albumTitle == "" {
		albumTitle = t.Title
	}

	var albumID *specid.ID
	if t.ReleaseID != "" {
		albumID = &specid.ID{URI: fmt.Sprintf("mc:al:%s", t.ReleaseID)}
	}

	artistName := "Unknown Artist"
	var artistID *specid.ID
	var artistRefs []*spec.ArtistRef

	if len(t.Artists) > 0 {
		artistName = t.Artists[0].Name
		if artistName == "" {
			artistName = t.Artists[0].DisplayName
		}
		firstID := t.Artists[0].GetArtistID()
		if firstID != "" {
			artistID = &specid.ID{URI: fmt.Sprintf("mc:ar:%s", firstID)}
		}
		artistRefs = make([]*spec.ArtistRef, len(t.Artists))
		for i, a := range t.Artists {
			aid := a.GetArtistID()
			var id *specid.ID
			if aid != "" {
				id = &specid.ID{URI: fmt.Sprintf("mc:ar:%s", aid)}
			}
			aName := a.Name
			if aName == "" {
				aName = a.DisplayName
			}
			artistRefs[i] = &spec.ArtistRef{ID: id, Name: aName}
		}
	} else if len(t.ArtistNames) > 0 {
		artistName = t.ArtistNames[0]
		artistRefs = make([]*spec.ArtistRef, len(t.ArtistNames))
		for i, name := range t.ArtistNames {
			var id *specid.ID
			if i < len(t.ArtistIDs) && t.ArtistIDs[i] != "" {
				id = &specid.ID{URI: fmt.Sprintf("mc:ar:%s", t.ArtistIDs[i])}
				if artistID == nil {
					artistID = id
				}
			}
			artistRefs[i] = &spec.ArtistRef{ID: id, Name: name}
		}
	}

	coverID := trackID
	if albumID != nil {
		coverID = albumID
	}
	if t.Artwork != "" {
		p.coverCache.Store(coverID.URI, t.Artwork)
		p.coverCache.Store(trackID.URI, t.Artwork)
	}

	durationSec := int(math.Round(float64(t.Duration) / 1000.0))

	isrcSlice := []string{}
	if t.ISRC != "" {
		isrcSlice = []string{t.ISRC}
	}

	return &spec.TrackChild{
		ID:            trackID,
		ParentID:      albumID,
		Title:         t.Title,
		Album:         albumTitle,
		AlbumID:       albumID,
		Artist:        artistName,
		ArtistID:      artistID,
		Artists:       artistRefs,
		DisplayArtist: artistName,
		CoverID:       coverID,
		Duration:      durationSec,
		Bitrate:       1411,
		ContentType:   "audio/flac",
		Suffix:        "flac",
		Path:          fmt.Sprintf("audio/%s/%s - %s.flac", artistName, artistName, t.Title),
		TrackNumber:   t.TrackNumber,
		DiscNumber:    t.DiscNumber,
		Year:          parseYear(t.ReleaseDate),
		Type:          "music",
		ISRC:          isrcSlice,
	}
}

func (p *Provider) releaseToSpec(r *MonochromeRelease) *spec.Album {
	if r == nil {
		return nil
	}
	rawID := r.GetReleaseID()
	albumID := &specid.ID{URI: fmt.Sprintf("mc:al:%s", rawID)}

	artistName := "Unknown Artist"
	var artistID *specid.ID
	var artistRefs []*spec.ArtistRef

	if len(r.Artists) > 0 {
		artistName = r.Artists[0].Name
		if artistName == "" {
			artistName = r.Artists[0].DisplayName
		}
		firstID := r.Artists[0].GetArtistID()
		if firstID != "" {
			artistID = &specid.ID{URI: fmt.Sprintf("mc:ar:%s", firstID)}
		}
		artistRefs = make([]*spec.ArtistRef, len(r.Artists))
		for i, a := range r.Artists {
			aid := a.GetArtistID()
			var id *specid.ID
			if aid != "" {
				id = &specid.ID{URI: fmt.Sprintf("mc:ar:%s", aid)}
			}
			aName := a.Name
			if aName == "" {
				aName = a.DisplayName
			}
			artistRefs[i] = &spec.ArtistRef{ID: id, Name: aName}
		}
	} else if len(r.ArtistNames) > 0 {
		artistName = r.ArtistNames[0]
		artistRefs = make([]*spec.ArtistRef, len(r.ArtistNames))
		for i, name := range r.ArtistNames {
			var id *specid.ID
			if i < len(r.ArtistIDs) && r.ArtistIDs[i] != "" {
				id = &specid.ID{URI: fmt.Sprintf("mc:ar:%s", r.ArtistIDs[i])}
				if artistID == nil {
					artistID = id
				}
			}
			artistRefs[i] = &spec.ArtistRef{ID: id, Name: name}
		}
	}

	if r.Artwork != "" {
		p.coverCache.Store(albumID.URI, r.Artwork)
	}

	trackCount := r.TrackCount
	if trackCount == 0 && len(r.Tracks) > 0 {
		trackCount = len(r.Tracks)
	}

	totalDuration := 0
	tracks := make([]*spec.TrackChild, len(r.Tracks))
	for i := range r.Tracks {
		r.Tracks[i].AlbumTitle = r.Title
		r.Tracks[i].ReleaseDate = r.ReleaseDate
		if r.Tracks[i].ReleaseID == "" {
			r.Tracks[i].ReleaseID = rawID
		}
		if r.Tracks[i].Artwork == "" {
			r.Tracks[i].Artwork = r.Artwork
		}
		tc := p.trackToSpec(&r.Tracks[i])
		tracks[i] = tc
		totalDuration += tc.Duration

		// Cache individual track
		p.trackCache.Store(r.Tracks[i].GetTrackID(), tc)
	}

	return &spec.Album{
		ID:            albumID,
		ParentID:      artistID,
		Name:          r.Title,
		Artist:        artistName,
		ArtistID:      artistID,
		CoverID:       albumID,
		TrackCount:    trackCount,
		Duration:      totalDuration,
		Year:          parseYear(r.ReleaseDate),
		Artists:       artistRefs,
		DisplayArtist: artistName,
		Tracks:        tracks,
	}
}

func (p *Provider) artistToSpec(a *MonochromeArtist) *spec.Artist {
	if a == nil {
		return nil
	}
	rawID := a.GetArtistID()
	artistID := &specid.ID{URI: fmt.Sprintf("mc:ar:%s", rawID)}

	name := a.Name
	if name == "" {
		name = a.DisplayName
	}

	if a.Avatar != "" {
		p.coverCache.Store(artistID.URI, a.Avatar)
	}

	return &spec.Artist{
		ID:      artistID,
		Name:    name,
		CoverID: artistID,
	}
}

// GetTrack fetches track metadata
func (p *Provider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	if val, ok := p.trackCache.Load(rawID); ok {
		return val.(*spec.TrackChild), nil
	}

	// Monochrome does not have a dedicated /tracks/{id} metadata endpoint,
	// so we use /search/tracks?q={rawID} to find it.
	tracks, err := p.client.SearchTracks(ctx, rawID, 10)
	if err == nil {
		for i := range tracks {
			if tracks[i].GetTrackID() == rawID {
				tc := p.trackToSpec(&tracks[i])
				p.trackCache.Store(rawID, tc)
				return tc, nil
			}
		}
	}

	// Fallback: check if the audio stream itself is alive
	if _, err := p.client.CheckTrackStream(ctx, rawID); err == nil {
		tc := &spec.TrackChild{
			ID:          &specid.ID{URI: fmt.Sprintf("mc:tr:%s", rawID)},
			Title:       fmt.Sprintf("Track %s", rawID),
			Artist:      "Monochrome",
			Duration:    0,
			Bitrate:     1411,
			ContentType: "audio/flac",
			Suffix:      "flac",
			Type:        "music",
		}
		p.trackCache.Store(rawID, tc)
		return tc, nil
	}

	return nil, fmt.Errorf("monochrome track not found: %s", rawID)
}

// GetTracksBatch fetches multiple tracks concurrently
func (p *Provider) GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild {
	results := make([]*spec.TrackChild, len(rawIDs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)

	for i, rawID := range rawIDs {
		if val, ok := p.trackCache.Load(rawID); ok {
			results[i] = val.(*spec.TrackChild)
			continue
		}

		wg.Add(1)
		go func(idx int, id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if tc, err := p.GetTrack(ctx, id); err == nil && tc != nil {
				results[idx] = tc
			}
		}(i, rawID)
	}

	wg.Wait()
	return results
}

// GetAlbum fetches an album/release and its tracks
func (p *Provider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	rel, err := p.client.GetRelease(ctx, rawID)
	if err != nil {
		return nil, err
	}
	return p.releaseToSpec(rel), nil
}

// GetArtist fetches an artist and their discography
func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	art, err := p.client.GetArtist(ctx, rawID)
	if err != nil {
		return nil, err
	}

	artistID := &specid.ID{URI: fmt.Sprintf("mc:ar:%s", rawID)}
	if art.Avatar != "" {
		p.coverCache.Store(artistID.URI, art.Avatar)
	}

	name := art.Name
	if name == "" {
		name = art.DisplayName
	}

	allReleases := append([]MonochromeRelease{}, art.Albums...)
	allReleases = append(allReleases, art.Singles...)
	allReleases = append(allReleases, art.Compilations...)

	albums := make([]*spec.Album, len(allReleases))
	for i := range allReleases {
		albums[i] = p.releaseToSpec(&allReleases[i])
	}

	return &spec.Artist{
		ID:         artistID,
		Name:       name,
		CoverID:    artistID,
		AlbumCount: len(albums),
		Albums:     albums,
	}, nil
}

// GetArtistAlbums fetches an artist's discography and top tracks
func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	art, err := p.client.GetArtist(ctx, rawID)
	if err != nil {
		return nil, nil, err
	}

	allReleases := append([]MonochromeRelease{}, art.Albums...)
	allReleases = append(allReleases, art.Singles...)
	allReleases = append(allReleases, art.Compilations...)

	albums := make([]*spec.Album, len(allReleases))
	for i := range allReleases {
		albums[i] = p.releaseToSpec(&allReleases[i])
	}

	var topTracks []*spec.TrackChild
	if !skipTracks && len(art.TopTracks) > 0 {
		topTracks = make([]*spec.TrackChild, len(art.TopTracks))
		for i := range art.TopTracks {
			topTracks[i] = p.trackToSpec(&art.TopTracks[i])
		}
	}

	return albums, topTracks, nil
}

// SearchTracks searches for tracks
func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	rawTracks, err := p.client.SearchTracks(ctx, query, limit+offset)
	if err != nil {
		return nil, err
	}

	if offset >= len(rawTracks) {
		return nil, nil
	}
	rawTracks = rawTracks[offset:]
	if len(rawTracks) > limit {
		rawTracks = rawTracks[:limit]
	}

	out := make([]*spec.TrackChild, len(rawTracks))
	for i := range rawTracks {
		tc := p.trackToSpec(&rawTracks[i])
		out[i] = tc
		p.trackCache.Store(rawTracks[i].GetTrackID(), tc)
	}
	return out, nil
}

// SearchAlbums searches for albums
func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	rawReleases, err := p.client.SearchReleases(ctx, query, limit+offset)
	if err != nil {
		return nil, err
	}

	if offset >= len(rawReleases) {
		return nil, nil
	}
	rawReleases = rawReleases[offset:]
	if len(rawReleases) > limit {
		rawReleases = rawReleases[:limit]
	}

	out := make([]*spec.Album, len(rawReleases))
	for i := range rawReleases {
		out[i] = p.releaseToSpec(&rawReleases[i])
	}
	return out, nil
}

// SearchArtists searches for artists
func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	rawArtists, err := p.client.SearchArtists(ctx, query, limit+offset)
	if err != nil {
		return nil, err
	}

	if offset >= len(rawArtists) {
		return nil, nil
	}
	rawArtists = rawArtists[offset:]
	if len(rawArtists) > limit {
		rawArtists = rawArtists[:limit]
	}

	out := make([]*spec.Artist, len(rawArtists))
	for i := range rawArtists {
		out[i] = p.artistToSpec(&rawArtists[i])
	}
	return out, nil
}

// Search searches across tracks, albums, and artists
func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	res, err := p.client.SearchAll(ctx, query)
	if err != nil {
		return nil, err
	}

	tracks := make([]*spec.TrackChild, 0, len(res.Tracks))
	for i := range res.Tracks {
		tc := p.trackToSpec(&res.Tracks[i])
		tracks = append(tracks, tc)
		p.trackCache.Store(res.Tracks[i].GetTrackID(), tc)
	}

	albums := make([]*spec.Album, 0, len(res.Releases))
	for i := range res.Releases {
		albums = append(albums, p.releaseToSpec(&res.Releases[i]))
	}

	artists := make([]*spec.Artist, 0, len(res.Artists))
	for i := range res.Artists {
		artists = append(artists, p.artistToSpec(&res.Artists[i]))
	}

	return &provider.SearchResults{
		Tracks:  tracks,
		Albums:  albums,
		Artists: artists,
	}, nil
}

// GetStreamURL returns the direct lossless FLAC audio streaming URL
func (p *Provider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	return p.client.GetStreamURL(rawID), nil
}

// GetCoverURL resolves a cached artwork URL
func (p *Provider) GetCoverURL(coverID string, size int) string {
	if val, ok := p.coverCache.Load(coverID); ok {
		return val.(string)
	}
	// Fallback to proxy artwork endpoint if raw ID
	cleanID := strings.TrimPrefix(coverID, "mc:al:")
	cleanID = strings.TrimPrefix(cleanID, "mc:tr:")
	cleanID = strings.TrimPrefix(cleanID, "mc:ar:")
	return fmt.Sprintf("https://tracks.monochrome.st/proxy/mi/%s-1A01.jpg", cleanID)
}

// GetLyrics returns nil as lyrics are not served directly by this endpoint
func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, nil
}

// GetTopTracks returns top tracks from search or fallback
func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "hits", limit, 0)
}

// GetArtistTopTracks returns top tracks for an artist
func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	art, err := p.client.GetArtist(ctx, artistRawID)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, 0, len(art.TopTracks))
	for i := range art.TopTracks {
		if len(out) >= limit {
			break
		}
		out = append(out, p.trackToSpec(&art.TopTracks[i]))
	}
	return out, nil
}

// GetSimilarArtists returns empty list
func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	return nil, nil
}

// GetRecommendations returns recommendations
func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	return nil, nil
}

// GetTrackByISRC resolves a track by ISRC code
func (p *Provider) GetTrackByISRC(ctx context.Context, isrc string) (*spec.TrackChild, error) {
	cleanISRC := strings.ToUpper(strings.TrimSpace(isrc))
	if cleanISRC == "" {
		return nil, fmt.Errorf("empty isrc")
	}

	tracks, err := p.client.SearchTracks(ctx, cleanISRC, 10)
	if err != nil {
		return nil, err
	}

	for i := range tracks {
		if strings.EqualFold(tracks[i].ISRC, cleanISRC) {
			tc := p.trackToSpec(&tracks[i])
			p.trackCache.Store(tracks[i].GetTrackID(), tc)
			return tc, nil
		}
	}

	return nil, fmt.Errorf("track with isrc %s not found on monochrome", isrc)
}

// MatchTrackByMetadata matches a track by artist and title
func (p *Provider) MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error) {
	q := fmt.Sprintf("%s %s", artist, title)
	tracks, err := p.client.SearchTracks(ctx, q, 10)
	if err != nil {
		return nil, err
	}

	var best *MonochromeTrack
	bestScore := -1

	for i := range tracks {
		t := &tracks[i]
		score := 0
		if strings.Contains(strings.ToLower(t.Title), strings.ToLower(title)) {
			score += 5
		}
		for _, an := range t.ArtistNames {
			if strings.Contains(strings.ToLower(an), strings.ToLower(artist)) || strings.Contains(strings.ToLower(artist), strings.ToLower(an)) {
				score += 5
				break
			}
		}
		if expectedDuration > 0 && t.Duration > 0 {
			diff := int(math.Abs(float64(t.Duration/1000 - expectedDuration)))
			if diff <= 3 {
				score += 10
			} else if diff <= 10 {
				score += 5
			}
		}

		if score > bestScore && score >= 10 {
			bestScore = score
			best = t
		}
	}

	if best != nil {
		tc := p.trackToSpec(best)
		p.trackCache.Store(best.GetTrackID(), tc)
		return tc, nil
	}

	return nil, fmt.Errorf("no match found on monochrome for %s - %s", artist, title)
}
