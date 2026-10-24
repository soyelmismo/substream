package tidal

import (
	"context"
	"fmt"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

var nonAsciiUnsafe = regexp.MustCompile(`[^\p{L}\p{N} ._-]+`)
var lrcRegex = regexp.MustCompile(`^\[(\d{2}):(\d{2}\.\d{2,3})\]\s*(.*)$`)

// DefaultSeeds contains verified active regional mirrors for Tidal
var DefaultSeeds = []string{
	"https://ohio-1.monochrome.tf",
	"https://frankfurt-1.monochrome.tf",
	"https://monochrome-api.samidy.com",
	"https://singapore-1.monochrome.tf",
}

// DefaultTrackers contains public status trackers for Tidal mirrors
var DefaultTrackers = []string{
	"https://tidal-uptime.props-76styles.workers.dev",
}

func sanitizeFilename(s string) string {
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
	s = replacer.Replace(s)
	s = nonAsciiUnsafe.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func parseYear(date string) int {
	if date == "" {
		return 0
	}
	parts := strings.Split(date, "-")
	if len(parts) > 0 {
		y, _ := strconv.Atoi(parts[0])
		return y
	}
	return 0
}

// Provider implements provider.MusicProvider and provider.SeedProvider using tidalproxy backend.
type Provider struct {
	proxy TidalProxy
}

// New creates a new Tidal music provider wrapping
func New(proxy TidalProxy) *Provider {
	return &Provider{proxy: proxy}
}

// Bootstrap creates and initializes a fully autonomous Tidal provider:
// configuring the mirror pool, auto-seeding SQLite if empty, and launching mirror discovery.
func Bootstrap(dbc *db.DB, cliURLs []string, userToken string) *Provider {
	pool := NewPool(cliURLs, PoolConfig{
		HealthInterval: 30 * time.Second,
		Timeout:        10 * time.Second,
		UserToken:      userToken,
	})
	setupTokenManager(dbc, pool)
	cachedProxy := NewCachedProxy(pool, dbc, 30*24*time.Hour)

	// Seed mirrors if SQLite table is empty
	dbProxies, _ := dbc.GetProxies()
	if len(dbProxies) == 0 {
		log.Printf("[TIDAL] No mirrors found in database. Auto-seeding default regional mirrors")
		for _, u := range DefaultSeeds {
			dbc.AddProxy(u, "Community", "auto-seed")
		}
		for _, u := range cliURLs {
			if u != "" && u != "http://localhost:8000" {
				dbc.AddProxy(u, "CLI Default", "cli")
			}
		}
		dbProxies, _ = dbc.GetProxies()
	}

	if len(dbProxies) > 0 {
		var dbURLs []string
		for _, p := range dbProxies {
			dbURLs = append(dbURLs, p.URL)
		}
		pool.SetInstances(dbURLs)
	}

	// Start background auto-discovery from trackers
	pool.StartDiscovery(DefaultTrackers, 30*time.Minute, dbc)

	return New(cachedProxy)
}

func (p *Provider) ID() string {
	return "td"
}

func (p *Provider) Name() string {
	return "Tidal"
}

func (p *Provider) DefaultSeeds() []string {
	return DefaultSeeds
}

func (p *Provider) DefaultTrackers() []string {
	return DefaultTrackers
}

// Proxy returns the underlying TidalProxy if direct access is needed
func (p *Provider) Proxy() TidalProxy {
	return p.proxy
}

func (p *Provider) parseNumericID(raw string) (int, error) {
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid tidal ID %q: must be integer", raw)
	}
	return val, nil
}

func (p *Provider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	t, err := p.proxy.GetTrackInfo(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.trackToSpec(t), nil
}

func (p *Provider) GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild {
	intIDs := make([]int, 0, len(rawIDs))
	indexMap := make(map[int]int) // intID -> original index

	for i, raw := range rawIDs {
		if id, err := strconv.Atoi(raw); err == nil {
			intIDs = append(intIDs, id)
			indexMap[id] = i
		}
	}

	results := make([]*spec.TrackChild, len(rawIDs))
	if len(intIDs) == 0 {
		return results
	}

	trackMap := p.proxy.GetTracksInfoBatch(ctx, intIDs)
	if trackMap != nil {
		for tid, t := range trackMap {
			if idx, ok := indexMap[tid]; ok && t != nil {
				results[idx] = p.trackToSpec(t)
			}
		}
	}

	return results
}

func (p *Provider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	album, err := p.proxy.GetAlbumInfo(ctx, id)
	if err != nil {
		return nil, err
	}

	a := p.albumToSpec(album)
	a.TrackCount = len(album.Items)
	a.Tracks = make([]*spec.TrackChild, len(album.Items))

	totalDuration := 0
	for i := range album.Items {
		tc := p.trackToSpec(&album.Items[i])
		if tc.Album == "" {
			tc.Album = album.Title
		}
		if tc.AlbumID == nil {
			tc.AlbumID = a.ID
		}
		a.Tracks[i] = tc
		totalDuration += tc.Duration
	}
	if a.Duration == 0 {
		a.Duration = totalDuration
	}

	return a, nil
}

func (p *Provider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	info, err := p.proxy.GetArtistInfo(ctx, id)
	if err != nil {
		return nil, err
	}
	return p.artistToSpec(&info.Artist), nil
}

func (p *Provider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, nil, err
	}
	page, err := p.proxy.GetArtistAlbums(ctx, id, skipTracks)
	if err != nil {
		return nil, nil, err
	}

	albums := make([]*spec.Album, len(page.Albums.Items))
	for i := range page.Albums.Items {
		albums[i] = p.albumToSpec(&page.Albums.Items[i])
	}

	tracks := make([]*spec.TrackChild, len(page.Tracks))
	for i := range page.Tracks {
		tracks[i] = p.trackToSpec(&page.Tracks[i])
	}

	return albums, tracks, nil
}

func (p *Provider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	rawTracks, err := p.proxy.SearchTracks(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(rawTracks))
	for i := range rawTracks {
		out[i] = p.trackToSpec(&rawTracks[i])
	}
	return out, nil
}

func (p *Provider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	rawAlbums, err := p.proxy.SearchAlbums(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Album, len(rawAlbums))
	for i := range rawAlbums {
		out[i] = p.albumToSpec(&rawAlbums[i])
	}
	return out, nil
}

func (p *Provider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	rawArtists, err := p.proxy.SearchArtists(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Artist, len(rawArtists))
	for i := range rawArtists {
		out[i] = p.artistToSpec(&rawArtists[i])
	}
	return out, nil
}

func (p *Provider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	var wg sync.WaitGroup
	wg.Add(3)

	var tracks []*spec.TrackChild
	var albums []*spec.Album
	var artists []*spec.Artist

	go func() {
		defer wg.Done()
		tracks, _ = p.SearchTracks(ctx, query, limit, offset)
	}()
	go func() {
		defer wg.Done()
		albums, _ = p.SearchAlbums(ctx, query, limit, offset)
	}()
	go func() {
		defer wg.Done()
		artists, _ = p.SearchArtists(ctx, query, limit, offset)
	}()

	wg.Wait()

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
	return p.proxy.GetStreamURL(ctx, id, quality, clientIP)
}

func (p *Provider) GetCoverURL(coverID string, size int) string {
	return p.proxy.GetCoverURL(coverID, size)
}

func (p *Provider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	id, err := p.parseNumericID(rawID)
	if err != nil {
		return nil, err
	}
	rawLyrics, err := p.proxy.GetLyrics(ctx, id)
	if err != nil {
		return nil, err
	}

	var specLines []spec.Lyric
	if rawLyrics.Subtitles != "" {
		for _, line := range strings.Split(rawLyrics.Subtitles, "\n") {
			matches := lrcRegex.FindStringSubmatch(line)
			if len(matches) == 4 {
				min, _ := strconv.Atoi(matches[1])
				sec, _ := strconv.ParseFloat(matches[2], 64)
				startMs := int64(min*60*1000 + int(sec*1000))
				specLines = append(specLines, spec.Lyric{
					Start: &startMs,
					Value: matches[3],
				})
			} else if line != "" {
				specLines = append(specLines, spec.Lyric{Value: line})
			}
		}
	} else {
		for _, l := range strings.Split(rawLyrics.Lyrics, "\n") {
			specLines = append(specLines, spec.Lyric{Value: l})
		}
	}

	return &spec.StructuredLyrics{
		Lang:   "und",
		Synced: rawLyrics.Subtitles != "",
		Lines:  specLines,
	}, nil
}

func (p *Provider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	tracks, err := p.proxy.GetTopTracks(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(tracks))
	for i := range tracks {
		out[i] = p.trackToSpec(&tracks[i])
	}
	return out, nil
}

func (p *Provider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	id, err := p.parseNumericID(artistRawID)
	if err != nil {
		return nil, err
	}
	tracks, err := p.proxy.GetArtistTopTracks(ctx, id, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(tracks))
	for i := range tracks {
		out[i] = p.trackToSpec(&tracks[i])
	}
	return out, nil
}

func (p *Provider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	id, err := p.parseNumericID(artistRawID)
	if err != nil {
		return nil, err
	}
	artists, err := p.proxy.GetSimilarArtists(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.Artist, len(artists))
	for i := range artists {
		out[i] = p.artistToSpec(&artists[i])
	}
	return out, nil
}

func (p *Provider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	id, err := p.parseNumericID(trackRawID)
	if err != nil {
		return nil, err
	}
	tracks, err := p.proxy.GetRecommendations(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]*spec.TrackChild, len(tracks))
	for i := range tracks {
		out[i] = p.trackToSpec(&tracks[i])
	}
	return out, nil
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
		return nil, fmt.Errorf("no tidal tracks found for %q: %v", q, err)
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
			score += 20
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

	return nil, fmt.Errorf("no matching Tidal track found for %q with duration %d", q, expectedDuration)
}

// --- Spec Conversion Helpers ---

func (p *Provider) trackToSpec(t *TidalTrack) *spec.TrackChild {
	if t == nil {
		return nil
	}
	artistName := t.Artist.Name
	if len(t.Artists) > 0 && t.Artists[0].Name != "" {
		artistName = t.Artists[0].Name
	}
	if artistName == "" {
		artistName = t.Artist.Name
	}
	if len(t.Artists) == 0 && t.Artist.ID != 0 {
		t.Artists = []TidalArtist{t.Artist}
	}

	trackID := &specid.ID{URI: fmt.Sprintf("td:tr:%d", t.ID)}
	albumID := &specid.ID{URI: fmt.Sprintf("td:al:%d", t.Album.ID)}
	artistID := &specid.ID{URI: fmt.Sprintf("td:ar:%d", t.Artist.ID)}

	coverIDValue := t.Album.ID
	if coverIDValue == 0 {
		coverIDValue = t.ID
	}
	coverID := &specid.ID{URI: fmt.Sprintf("td:al:%d", coverIDValue)}

	suffix := "flac"
	bitrate := 1411
	contentType := "audio/flac"
	if !t.IsLossless() {
		suffix = "m4a"
		bitrate = 320
		if t.AudioQuality == "LOW" {
			bitrate = 96
		}
		contentType = "audio/mp4"
	} else if t.AudioQuality == "HI_RES_LOSSLESS" {
		bitrate = 2822
	}

	safeArtist := sanitizeFilename(artistName)
	safeTitle := sanitizeFilename(t.Title)
	path := fmt.Sprintf("audio/%s/%02d. %s - %s.%s", safeArtist, t.TrackNumber, safeArtist, safeTitle, suffix)

	tc := &spec.TrackChild{
		ID:          trackID,
		Title:       t.Title,
		Album:       t.Album.Title,
		AlbumID:     albumID,
		Artist:      artistName,
		ArtistID:    artistID,
		ParentID:    albumID,
		CoverID:     coverID,
		TrackNumber: t.TrackNumber,
		DiscNumber:  t.VolumeNumber,
		Duration:    t.Duration,
		Bitrate:     bitrate,
		ContentType: contentType,
		Suffix:      suffix,
		Size:        t.Duration * 176400,
		IsDir:       false,
		Type:        "music",
		Path:        path,
		Year:        parseYear(t.Album.ReleaseDate),
	}

	if len(t.Artists) > 0 {
		tc.Artists = make([]*spec.ArtistRef, len(t.Artists))
		names := make([]string, len(t.Artists))
		for i, a := range t.Artists {
			id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}
			tc.Artists[i] = &spec.ArtistRef{ID: id, Name: a.Name}
			names[i] = a.Name
		}
		tc.DisplayArtist = strings.Join(names, ", ")
	}

	return tc
}

func (p *Provider) albumToSpec(a *TidalAlbum) *spec.Album {
	albumID := &specid.ID{URI: fmt.Sprintf("td:al:%d", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("td:al:%d", a.ID)}

	artistName := ""
	var artistID *specid.ID
	var artistRefs []*spec.ArtistRef
	if len(a.Artists) > 0 {
		artistName = a.Artists[0].Name
		artistID = &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.Artists[0].ID)}
		artistRefs = make([]*spec.ArtistRef, len(a.Artists))
		for i, ar := range a.Artists {
			id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", ar.ID)}
			artistRefs[i] = &spec.ArtistRef{ID: id, Name: ar.Name}
		}
	}

	names := make([]string, len(a.Artists))
	for i, ar := range a.Artists {
		names[i] = ar.Name
	}

	return &spec.Album{
		ID:            albumID,
		ParentID:      artistID,
		Name:          a.Title,
		Artist:        artistName,
		ArtistID:      artistID,
		CoverID:       coverID,
		TrackCount:    a.NumberOfTracks,
		Duration:      a.Duration,
		Year:          parseYear(a.ReleaseDate),
		Artists:       artistRefs,
		DisplayArtist: strings.Join(names, ", "),
	}
}

func (p *Provider) artistToSpec(a *TidalArtist) *spec.Artist {
	id := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}
	coverID := &specid.ID{URI: fmt.Sprintf("td:ar:%d", a.ID)}

	return &spec.Artist{
		ID:      id,
		Name:    a.Name,
		CoverID: coverID,
	}
}

// setupTokenManager dynamically provides and auto-refreshes active Tidal OAuth accounts from SQLite with quality-awareness
func setupTokenManager(dbc *db.DB, pool *Pool) {
	var tokenIdx uint64
	var refreshMu sync.Mutex

	getTokenForAccounts := func(accounts []*db.TidalAccount) string {
		if len(accounts) == 0 {
			return ""
		}

		idx := atomic.AddUint64(&tokenIdx, 1) - 1
		acc := accounts[idx%uint64(len(accounts))]

		// Auto-refresh if token expires in less than 5 minutes or is empty
		if acc.AccessToken == "" || time.Now().After(acc.ExpiresAt.Add(-5*time.Minute)) {
			refreshMu.Lock()
			freshAccs, _ := dbc.GetActiveTidalAccounts()
			for _, fa := range freshAccs {
				if fa.ID == acc.ID {
					acc = fa
					break
				}
			}
			if acc.AccessToken == "" || time.Now().After(acc.ExpiresAt.Add(-5*time.Minute)) {
				log.Printf("[TIDAL-OAUTH] 🔄 Auto-refreshing access token for account %s (User ID: %s)...", acc.Username, acc.UserID)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				tokResp, err := RefreshAccessToken(ctx, acc.RefreshToken)
				cancel()
				if err != nil {
					log.Printf("[TIDAL-OAUTH] ❌ Refresh failed for account %s: %v", acc.Username, err)
					refreshMu.Unlock()
					return acc.AccessToken
				}
				expiresAt := time.Now().Add(time.Duration(tokResp.ExpiresIn) * time.Second)
				if err := dbc.UpdateTidalAccountTokens(acc.ID, tokResp.AccessToken, expiresAt); err != nil {
					log.Printf("[TIDAL-OAUTH] ⚠️ Failed to persist refreshed token to DB: %v", err)
				} else {
					log.Printf("[TIDAL-OAUTH] ✅ Token refreshed successfully for %s! Valid until %s", acc.Username, expiresAt.Format("15:04:05 MST"))
				}
				acc.AccessToken = tokResp.AccessToken

				// Also check and update subscription if not yet detected
				if acc.SubscriptionType == "" || acc.QualityTier == 0 {
					subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
					subType, highestQuality, tier, subErr := FetchAccountSubscription(subCtx, acc.UserID, acc.AccessToken)
					subCancel()
					if subErr == nil {
						_ = dbc.UpdateTidalAccountSubscription(acc.ID, subType, highestQuality, tier)
						log.Printf("[TIDAL-OAUTH] 🏷️ Detected subscription for %s: Type=%s Quality=%s (Tier=%d)", acc.Username, subType, highestQuality, tier)
					}
				}
			}
			refreshMu.Unlock()
		}

		// Make sure the pool's token→accountID map is up-to-date so the
		// subscription-degraded handler in client.go can pass an
		// authoritative accountID instead of a (possibly stale) token.
		if acc.AccessToken != "" {
			pool.RegisterTokenAccountID(acc.AccessToken, acc.ID)
		}

		return acc.AccessToken
	}

	pool.SetTokenGetter(func() string {
		accounts, err := dbc.GetActiveTidalAccounts()
		if err != nil || len(accounts) == 0 {
			return ""
		}
		return getTokenForAccounts(accounts)
	})

	pool.SetQualityTokenGetter(func(quality string) string {
		var accounts []*db.TidalAccount
		var err error

		if strings.ToUpper(quality) == "LOSSLESS" || strings.ToUpper(quality) == "HI_RES_LOSSLESS" {
			// Prioritize HiFi/Lossless accounts
			accounts, err = dbc.GetActiveTidalAccountsForQuality(db.QualityTierLossless)
			if err == nil && len(accounts) > 0 {
				return getTokenForAccounts(accounts)
			}
		}

		// Fallback to any active account (e.g. Premium 320k)
		accounts, err = dbc.GetActiveTidalAccounts()
		if err != nil || len(accounts) == 0 {
			return ""
		}
		return getTokenForAccounts(accounts)
	})

	// [FIX C] Register a snapshot lister so GetStreamURL can iterate
	// every active account before giving up on paid quality. Mirrors the
	// same account selection as SetQualityTokenGetter (Lossless-priority
	// for paid qualities, all-active for lossy).
	pool.SetQualityTokenLister(func(quality string) []string {
		var accounts []*db.TidalAccount
		var err error

		if strings.ToUpper(quality) == "LOSSLESS" || strings.ToUpper(quality) == "HI_RES_LOSSLESS" || strings.ToUpper(quality) == "HI_RES" {
			accounts, err = dbc.GetActiveTidalAccountsForQuality(db.QualityTierLossless)
			if err != nil || len(accounts) == 0 {
				accounts, err = dbc.GetActiveTidalAccounts()
			}
		} else {
			accounts, err = dbc.GetActiveTidalAccounts()
		}
		if err != nil || len(accounts) == 0 {
			return nil
		}

		out := make([]string, 0, len(accounts))
		seen := make(map[string]bool)
		for _, acc := range accounts {
			tok := getTokenForAccounts([]*db.TidalAccount{acc})
			if tok == "" || seen[tok] {
				continue
			}
			seen[tok] = true
			out = append(out, tok)
		}
		return out
	})

	// Background worker: proactively auto-refreshes all active Tidal accounts
	// before they expire (every 5 minutes, refreshes anything expiring in < 30 minutes)
	//
	// Also wires the subscription-degraded handler: when the Tidal API
	// rejects a token with subStatus 5003 ("Requested quality is not
	// allowed in user's subscription") or returns a PREVIEW-only asset,
	// the corresponding account row in the DB is demoted to FREE so
	// future requests naturally rotate to the next paid account. This
	// fixes the "Supersonic hears Opus instead of FLAC" symptom caused
	// by an account flagged HI_RES in the DB that Tidal actually treats
	// as FREE.
	pool.SetAccountDegradedHandler(func(accountID int, reason string) {
		// Look up the DB row by its authoritative accountID. This is
		// race-free: the pool knows which account the failed token
		// came from at registration time, so we don't depend on the
		// (possibly stale) access_token still being present in the DB.
		allAccs, err := dbc.GetTidalAccounts()
		if err != nil {
			log.Printf("[TIDAL-OAUTH] ⚠️ Degraded handler could not load accounts: %v", err)
			return
		}
		var matchedID int
		var matchedUser string
		for _, a := range allAccs {
			if a.ID == accountID {
				matchedID = a.ID
				matchedUser = a.Username
				if matchedUser == "" {
					matchedUser = a.UserID
				}
				if matchedUser == "" {
					matchedUser = fmt.Sprintf("id=%d", matchedID)
				}
				break
			}
		}
		if matchedID == 0 {
			log.Printf("[TIDAL-OAUTH] ⚠️ Degraded handler: no DB row matches accountID=%d (reason: %s)", accountID, reason)
			return
		}
		log.Printf("[TIDAL-OAUTH] ⛔ Demoting Tidal account %s (id=%d) to FREE — reason: %s", matchedUser, matchedID, reason)
		if err := dbc.MarkTidalAccountAsFree(matchedID); err != nil {
			log.Printf("[TIDAL-OAUTH] ❌ Failed to persist FREE demotion for account %d: %v", matchedID, err)
			return
		}
		log.Printf("[TIDAL-OAUTH] ✅ Account %s (id=%d) is now FREE and will be skipped in paid-quality rotation", matchedUser, matchedID)
	})

	go func() {
		time.Sleep(3 * time.Second)
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		refreshAll := func() {
			accounts, err := dbc.GetTidalAccounts()
			if err != nil || len(accounts) == 0 {
				return
			}
			now := time.Now()
			for _, acc := range accounts {
				if !acc.IsActive {
					continue
				}
				if acc.AccessToken == "" || now.After(acc.ExpiresAt.Add(-30*time.Minute)) {
					refreshMu.Lock()
					freshAccs, _ := dbc.GetTidalAccounts()
					for _, fa := range freshAccs {
						if fa.ID == acc.ID {
							acc = fa
							break
						}
					}
					if !acc.IsActive {
						refreshMu.Unlock()
						continue
					}
					if acc.AccessToken == "" || time.Now().After(acc.ExpiresAt.Add(-30*time.Minute)) {
						log.Printf("[TIDAL-OAUTH] 🔄 Proactive background refresh for account %s (%s)...", acc.Username, acc.UserID)
						ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
						tokResp, err := RefreshAccessToken(ctx, acc.RefreshToken)
						cancel()
						if err != nil {
							log.Printf("[TIDAL-OAUTH] ❌ Background refresh failed for %s: %v", acc.Username, err)
							refreshMu.Unlock()
							continue
						}
						expiresAt := time.Now().Add(time.Duration(tokResp.ExpiresIn) * time.Second)
						if err := dbc.UpdateTidalAccountTokens(acc.ID, tokResp.AccessToken, expiresAt); err != nil {
							log.Printf("[TIDAL-OAUTH] ⚠️ Failed to persist refreshed token for %s: %v", acc.Username, err)
						} else {
							log.Printf("[TIDAL-OAUTH] ✅ Background refreshed token for %s! Valid until %s", acc.Username, expiresAt.Format("15:04:05 MST"))
						}

						// Refresh subscription status (in case account was upgraded or changed)
						subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
						subType, highestQuality, tier, subErr := FetchAccountSubscription(subCtx, acc.UserID, tokResp.AccessToken)
						subCancel()
						if subErr == nil {
							_ = dbc.UpdateTidalAccountSubscription(acc.ID, subType, highestQuality, tier)
						}
					}
					refreshMu.Unlock()
				}
			}
		}

		refreshAll()

		for range ticker.C {
			refreshAll()
		}
	}()
}
