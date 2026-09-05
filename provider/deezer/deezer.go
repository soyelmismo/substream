package deezer

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/internal/media/streamer"
	"go.senan.xyz/gonic/internal/media/tagger"
	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Provider implements provider.MusicProvider for Deezer
type Provider struct {
	client *Client
	worker *BackgroundWorker
}

// Ensure Provider implements interfaces
var (
	_ provider.MusicProvider    = (*Provider)(nil)
	_ provider.ISRCResolver     = (*Provider)(nil)
	_ provider.MediaDownloader  = (*Provider)(nil)
)

// New creates a new Deezer Provider
func New(opts ClientOptions) *Provider {
	client := NewClient(opts)
	var worker *BackgroundWorker
	if opts.DB != nil {
		worker = NewBackgroundWorker(opts.DB, client.Pool(), 1*time.Hour)
		worker.Start()
	}
	return &Provider{
		client: client,
		worker: worker,
	}
}

func (p *Provider) ID() string {
	return "dz"
}

func (p *Provider) Name() string {
	return "Deezer"
}

func (p *Provider) parseNumericID(raw string) (int64, error) {
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Deezer numeric ID %q: %w", raw, err)
	}
	return val, nil
}

func sanitizeFilename(s string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "\"", "_", "<", "_", ">", "_", "|", "_")
	return replacer.Replace(s)
}

func parseYear(dateStr string) int {
	parts := strings.Split(dateStr, "-")
	if len(parts) > 0 && len(parts[0]) == 4 {
		y, _ := strconv.Atoi(parts[0])
		return y
	}
	return 0
}

func (p *Provider) trackToSpec(t *DeezerTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	rawID := strconv.FormatInt(t.ID, 10)
	albumRawID := strconv.FormatInt(t.Album.ID, 10)
	artistRawID := strconv.FormatInt(t.Artist.ID, 10)

	trackID := &specid.ID{URI: fmt.Sprintf("dz:tr:%s", rawID)}
	albumID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", albumRawID)}
	artistID := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", albumRawID)}

	safeArtist := sanitizeFilename(t.Artist.Name)
	safeTitle := sanitizeFilename(t.Title)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.flac", safeArtist, t.TrackPosition, safeArtist, safeTitle)

	var isrcs []string
	if strings.TrimSpace(t.ISRC) != "" {
		isrcs = []string{strings.TrimSpace(t.ISRC)}
	}

	return &spec.TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       t.Album.Title,
		AlbumID:     albumID,
		Artist:      t.Artist.Name,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: t.TrackPosition,
		DiscNumber:  t.DiskNumber,
		Duration:    t.Duration,
		Bitrate:     1411,
		ContentType: "audio/flac",
		Suffix:      "flac",
		Size:        t.Duration * 176400,
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        parseYear(t.ReleaseDate),
		ISRC:        isrcs,
	}
}

func (p *Provider) albumToSpec(a *DeezerAlbum) *spec.Album {
	if a == nil {
		return nil
	}
	rawID := strconv.FormatInt(a.ID, 10)
	artistRawID := strconv.FormatInt(a.Artist.ID, 10)
	albumID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", rawID)}
	artistID := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", artistRawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("dz:al:%s", rawID)}

	var tracks []*spec.TrackChild
	for i := range a.Tracks.Data {
		t := &a.Tracks.Data[i]
		if t.Album.ID == 0 {
			t.Album.ID = a.ID
			t.Album.Title = a.Title
		}
		tracks = append(tracks, p.trackToSpec(t))
	}

	return &spec.Album{
		ID:         albumID,
		ParentID:   artistID,
		Name:       a.Title,
		Artist:     a.Artist.Name,
		ArtistID:   artistID,
		CoverID:    coverID,
		TrackCount: a.NbTracks,
		Duration:   a.Duration,
		Year:       parseYear(a.ReleaseDate),
		Tracks:     tracks,
	}
}

func (p *Provider) artistToSpec(a *DeezerArtist) *spec.Artist {
	if a == nil {
		return nil
	}
	rawID := strconv.FormatInt(a.ID, 10)
	id := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", rawID)}
	coverID := &specid.ID{URI: fmt.Sprintf("dz:ar:%s", rawID)}
	return &spec.Artist{
		ID:         id,
		Name:       a.Name,
		CoverID:    coverID,
		AlbumCount: a.NbAlbum,
	}
}

func (p *Provider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	t, err := p.client.GetTrack(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}

func (p *Provider) GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild {
	res := make([]*spec.TrackChild, len(rawIDs))
	if len(rawIDs) == 0 {
		return res
	}

	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup

	for i, raw := range rawIDs {
		wg.Add(1)
		go func(idx int, rawID string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			t, err := p.GetTrack(ctx, rawID)
			if err == nil && t != nil {
				res[idx] = t
			}
		}(i, raw)
	}
	wg.Wait()

	var clean []*spec.TrackChild
	for _, t := range res {
		if t != nil {
			clean = append(clean, t)
		}
	}
	return clean
}

func (p *Provider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	a, err := p.client.GetAlbum(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.albumToSpec(a), nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	a, err := p.client.GetArtist(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.artistToSpec(a), nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, nil, err
	}
	albums, err := p.client.GetArtistAlbums(ctx, id, 50)
	if err != nil {
		return nil, nil, err
	}

	resAlbums := make([]*spec.Album, 0, len(albums))
	for _, a := range albums {
		resAlbums = append(resAlbums, p.albumToSpec(&a))
	}

	var tracks []*spec.TrackChild
	if !skipTracks {
		top, err := p.client.GetArtistTopTracks(ctx, id, 50)
		if err == nil {
			tracks = make([]*spec.TrackChild, 0, len(top))
			for _, t := range top {
				tracks = append(tracks, p.trackToSpec(&t))
			}
		}
	}

	return resAlbums, tracks, nil
}

func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	items, err := p.client.SearchTracks(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.TrackChild, 0, len(items))
	for _, t := range items {
		res = append(res, p.trackToSpec(&t))
	}
	return res, nil
}

func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	items, err := p.client.SearchAlbums(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.Album, 0, len(items))
	for _, a := range items {
		res = append(res, p.albumToSpec(&a))
	}
	return res, nil
}

func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	items, err := p.client.SearchArtists(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.Artist, 0, len(items))
	for _, a := range items {
		res = append(res, p.artistToSpec(&a))
	}
	return res, nil
}

func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	tracks, _ := p.SearchTracks(ctx, query, limit, offset)
	albums, _ := p.SearchAlbums(ctx, query, limit, offset)
	artists, _ := p.SearchArtists(ctx, query, limit, offset)

	return &provider.SearchResults{
		Tracks:  tracks,
		Albums:  albums,
		Artists: artists,
	}, nil
}

func (p *Provider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return "", err
	}
	t, err := p.client.GetTrack(ctx, id)
	if err != nil {
		return "", err
	}
	return p.client.GetStreamURL(ctx, t, quality)
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	coverID = strings.TrimSpace(coverID)
	if strings.HasPrefix(coverID, "http") {
		return coverID
	}
	// If 32-character hex MD5 hash, serve from CDN image storage
	if len(coverID) == 32 {
		return fmt.Sprintf("https://cdn-images.dzcdn.net/images/cover/%s/500x500.jpg", coverID)
	}
	// For numeric IDs (album or artist), Deezer provides 302 image redirects
	sizeParam := "big"
	if size > 0 && size <= 250 {
		sizeParam = "medium"
	} else if size > 600 {
		sizeParam = "xl"
	}
	return fmt.Sprintf("https://api.deezer.com/album/%s/image?size=%s", coverID, sizeParam)
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return nil, fmt.Errorf("deezer lyrics not supported")
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return p.SearchTracks(ctx, "top", limit, 0)
}

func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	id, err := p.parseNumericID(artistRawID)
	if err != nil {
		return nil, err
	}
	tracks, err := p.client.GetArtistTopTracks(ctx, id, limit)
	if err != nil {
		return nil, err
	}
	res := make([]*spec.TrackChild, 0, len(tracks))
	for _, t := range tracks {
		res = append(res, p.trackToSpec(&t))
	}
	return res, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	return nil, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	return nil, nil
}

// GetTrackByISRC looks up a Deezer track by its international standard recording code
func (p *Provider) GetTrackByISRC(ctx context.Context, isrc string) (*spec.TrackChild, error) {
	t, err := p.client.SearchByISRC(ctx, isrc)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}

func cleanMatchString(s string) string {
	s = strings.ToLower(s)
	var out strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == ' ' {
			out.WriteRune(r)
		} else {
			out.WriteRune(' ')
		}
	}
	return strings.Join(strings.Fields(out.String()), " ")
}

// MatchTrackByMetadata matches a track by artist, title, and duration for cross-provider fallback
func (p *Provider) MatchTrackByMetadata(ctx context.Context, artist, title string, expectedDuration int) (*spec.TrackChild, error) {
	q := strings.TrimSpace(fmt.Sprintf("%s %s", artist, title))
	tracks, err := p.SearchTracks(ctx, q, 15, 0)
	if (err != nil || len(tracks) == 0) && title != "" {
		tracks, err = p.SearchTracks(ctx, title, 15, 0)
	}
	if err != nil || len(tracks) == 0 {
		return nil, fmt.Errorf("no deezer tracks found for %q: %v", q, err)
	}

	normTargetArtist := cleanMatchString(artist)
	normTargetTitle := cleanMatchString(title)
	isSnippet := expectedDuration > 0 && expectedDuration <= 35

	var bestTrack *spec.TrackChild
	bestScore := -1

	for _, t := range tracks {
		if t == nil {
			continue
		}
		normCandArtist := cleanMatchString(t.Artist)
		normCandTitle := cleanMatchString(t.Title)

		titleMatches := normCandTitle == normTargetTitle ||
			(normTargetTitle != "" && strings.Contains(normCandTitle, normTargetTitle)) ||
			(normCandTitle != "" && strings.Contains(normTargetTitle, normCandTitle))

		artistMatches := normCandArtist == normTargetArtist ||
			(normTargetArtist != "" && strings.Contains(normCandArtist, normTargetArtist)) ||
			(normCandArtist != "" && strings.Contains(normTargetArtist, normCandArtist)) ||
			normTargetArtist == ""

		score := 0
		if titleMatches {
			score += 50
			if normCandTitle == normTargetTitle {
				score += 20
			}
		}
		if artistMatches {
			score += 30
		}

		if isSnippet {
			// Expected duration is a preview/snippet (e.g. 30s). Full audio (>40s) is desired.
			if t.Duration >= expectedDuration && titleMatches {
				score += 40
				if t.Duration > 40 {
					score += 10
				}
			}
		} else if expectedDuration > 0 {
			diff := int(math.Abs(float64(t.Duration - expectedDuration)))
			if diff <= 5 {
				score += 40
			} else if diff <= 12 {
				score += 25
			} else if diff <= 25 {
				score += 10
			} else if !titleMatches {
				score -= 20
			}
		} else {
			score += 20 // duration ignored
		}

		if score > bestScore {
			bestScore = score
			bestTrack = t
		}
	}

	if bestScore >= 50 && bestTrack != nil {
		return bestTrack, nil
	}

	if len(tracks) > 0 && tracks[0] != nil {
		normFirstTitle := cleanMatchString(tracks[0].Title)
		if normFirstTitle == normTargetTitle || strings.Contains(normFirstTitle, normTargetTitle) {
			return tracks[0], nil
		}
	}

	return nil, fmt.Errorf("no matching Deezer track found for %q with duration %d", q, expectedDuration)
}

// DownloadTrack implements provider.MediaDownloader for Deezer tracks
func (p *Provider) DownloadTrack(ctx context.Context, req provider.DownloadRequest, w io.Writer) error {
	filePath, err := p.GetStreamURL(ctx, req.TrackID, req.Quality, req.ClientIP)
	if err != nil {
		return err
	}
	if strings.HasPrefix(filePath, "/") {
		f, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open cached stream: %w", err)
		}
		defer f.Close()
		if req.Metadata != nil {
			return tagger.TagFLAC(ctx, f, w, req.Metadata, nil)
		}
		_, err = io.Copy(w, f)
		return err
	}
	// Fallback if remote HTTP stream URL
	return streamer.Download(ctx, filePath, w, req.Metadata, req.ClientIP, nil)
}

// SyncAccounts updates active accounts on the underlying client
func (p *Provider) SyncAccounts(accounts []*db.DeezerAccount) {
	p.client.SyncAccounts(accounts)
}

// SetDB updates the database reference on the underlying client
func (p *Provider) SetDB(dbc *db.DB) {
	p.client.SetDB(dbc)
}

// SetCacheDir sets the cache directory for storing decrypted streams
func (p *Provider) SetCacheDir(dir string) {
	p.client.SetCacheDir(dir)
}

// Client returns the underlying Deezer client
func (p *Provider) Client() *Client {
	return p.client
}

// StartBackgroundMaintenance starts the periodic account validation worker with a custom interval
func (p *Provider) StartBackgroundMaintenance(interval time.Duration) {
	if p.worker != nil {
		p.worker.Stop()
	}
	if p.client != nil && p.client.pool != nil && p.client.pool.db != nil {
		p.worker = NewBackgroundWorker(p.client.pool.db, p.client.Pool(), interval)
		p.worker.Start()
	}
}

// Close terminates any background workers
func (p *Provider) Close() {
	if p.worker != nil {
		p.worker.Stop()
	}
}


