package ctrlsubsonic

import (
	"context"
	"crypto/md5"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/handlerutil"
	"go.senan.xyz/gonic/internal/cache"
	"go.senan.xyz/gonic/internal/importer"
	lfmclient "go.senan.xyz/gonic/lastfm"
	lastfmprovider "go.senan.xyz/gonic/providers/lastfm"
	"go.senan.xyz/gonic/recommendations"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/tidalproxy"
	"golang.org/x/crypto/bcrypt"
)

type CtxKey int

const (
	CtxUser CtxKey = iota
	CtxSession
	CtxParams
	CtxRequestID
)

type Controller struct {
	*http.ServeMux

	dbc              *db.DB
	proxy            tidalproxy.TidalProxy
	scrobblers       []scrobble.Scrobbler
	cachePath        string
	searchCache      *cache.Cache[cachedSearch]
	proxySem         chan struct{}
	coverLocks       sync.Map                              // dedup concurrent cover requests
	hotLocks         sync.Map                              // dedup concurrent hot.monochrome.tf requests
	httpClient       *http.Client                          // HTTP client for external requests
	streamClient     *http.Client                          // Dedicated client for audio streaming with optimized settings
	streamBufPool    sync.Pool                             // Pool of buffers for streaming (reduces GC pressure)
	genreCache       *cache.Cache[[]int]                   // Cache for genre tracks with LRU eviction
	genreAlbumCache  *cache.Cache[[]tidalproxy.TidalAlbum] // Cache for genre albums
	genreCountsCache *cache.Cache[map[string]genreCount]   // Cache for genre counts with TTL
	negCoverCache    *cache.Cache[bool]                    // Negative cache for missing covers
	settingsCache    *cache.Cache[string]                  // Cache for DB settings (proxy_streams, etc)
	streamURLCache   *cache.Cache[string]                  // Cache for stream URLs (TTL 30s)
	hydratedCache    *cache.Cache[bool]                    // Prevent duplicate background hydrations
	streamURLLocks   sync.Map                              // dedup concurrent stream URL requests
	streamLocks      sync.Map                              // dedup concurrent stream serving per track+client
	importer         *importer.JobManager                  // Background playlist import manager
	userStreamSem    chan struct{}                         // limit total concurrent streams across all users
	userStreamLimits sync.Map                              // per-user stream limiting (userID -> chan struct{})
	recEngine        *recommendations.Engine               // Recommendation engine for external providers

	// User preferences cache (L3) - warms on activity to eliminate repeated SQLite queries
	userPrefsCache *cache.Cache[*userPreferences] // userID -> cached preferences
}

// userPreferences holds all user metadata (stars, ratings, plays) in memory
type userPreferences struct {
	Stars    map[string]time.Time // uri -> starDate
	Ratings  map[string]int       // uri -> rating (1-5)
	Plays    map[string]int       // uri -> playCount
	LoadedAt time.Time            // for TTL checking
}

func New(dbc *db.DB, proxy tidalproxy.TidalProxy, scrobblers []scrobble.Scrobbler, cachePath string) *Controller {
	// Load configuration from environment variables
	initConfig()

	c := &Controller{
		ServeMux:      http.NewServeMux(),
		dbc:           dbc,
		proxy:         proxy,
		scrobblers:    scrobblers,
		cachePath:     cachePath,
		proxySem:      make(chan struct{}, 30),  // limit total concurrent proxy calls
		userStreamSem: make(chan struct{}, 100), // limit total concurrent streams (100 = ~500MB RAM max with 64KB buffers)
		importer:      importer.NewJobManager(dbc, proxy, cachePath),
		httpClient: &http.Client{
			Timeout: httpClientTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        httpMaxIdleConns,
				MaxIdleConnsPerHost: httpMaxIdleConnsPerHost,
				IdleConnTimeout:     httpIdleConnTimeout,
			},
		},
		streamClient: &http.Client{
			Timeout: 0, // No timeout for streaming - managed by context
			Transport: &http.Transport{
				MaxIdleConns:        200,               // More idle connections for high concurrency
				MaxIdleConnsPerHost: 50,                // Tidal uses few CDN hosts, increase per-host limit
				IdleConnTimeout:     120 * time.Second, // Longer timeout for streaming connections
				DisableCompression:  true,              // Audio already compressed
				ForceAttemptHTTP2:   false,             // HTTP/1.1 is more reliable for long streams
			},
		},
		streamBufPool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 64*1024) // 64KB optimal for audio streaming
				return &buf
			},
		},
		genreCache: cache.New[[]int](cache.Config{
			Name:            "genre-tracks",
			MaxSize:         maxGenreCacheSize,
			DefaultTTL:      genreCacheTTL,
			CleanupInterval: 5 * time.Minute,
		}),
		genreAlbumCache: cache.New[[]tidalproxy.TidalAlbum](cache.Config{
			Name:            "genre-albums",
			MaxSize:         20,
			DefaultTTL:      genreAlbumCacheTTL,
			CleanupInterval: 5 * time.Minute,
		}),
		genreCountsCache: cache.New[map[string]genreCount](cache.Config{
			Name:            "genre-counts",
			MaxSize:         10,
			DefaultTTL:      genreCountsCacheTTL,
			CleanupInterval: 5 * time.Minute,
		}),
		searchCache: cache.New[cachedSearch](cache.Config{
			Name:            "search",
			MaxSize:         100,
			DefaultTTL:      searchCacheTTL,
			CleanupInterval: 5 * time.Minute,
		}),
		negCoverCache: cache.New[bool](cache.Config{
			Name:            "neg-covers",
			MaxSize:         1000,
			DefaultTTL:      time.Hour,
			CleanupInterval: 10 * time.Minute,
		}),
		settingsCache: cache.New[string](cache.Config{
			Name:            "settings",
			MaxSize:         50,
			DefaultTTL:      5 * time.Second, // Short TTL for settings
			CleanupInterval: 30 * time.Second,
		}),
		streamURLCache: cache.New[string](cache.Config{
			Name:            "stream-urls",
			MaxSize:         100,
			DefaultTTL:      30 * time.Second, // URLs expire after 30s
			CleanupInterval: 1 * time.Minute,
		}),
		hydratedCache: cache.New[bool](cache.Config{
			Name:            "hydrated-items",
			MaxSize:         10000,
			DefaultTTL:      24 * time.Hour, // 24 hours cooldown
			CleanupInterval: 1 * time.Hour,
		}),
		userPrefsCache: cache.New[*userPreferences](cache.Config{
			Name:            "user-prefs",
			MaxSize:         100,              // 100 users
			DefaultTTL:      10 * time.Minute, // Warm for 10 min of inactivity
			CleanupInterval: 5 * time.Minute,
		}),
	}

	// Initialize recommendation engine and register external providers
	c.recEngine = recommendations.NewEngine(dbc)

	// Register Last.fm provider if a lastfm client is available in scrobblers
	for _, s := range scrobblers {
		if lfmClient, ok := s.(*lfmclient.Client); ok {
			c.recEngine.Register(lastfmprovider.New(lfmClient, proxy))
			log.Println("[CTRL] Last.fm recommendation provider registered")
			break
		}
	}

	chain := handlerutil.Chain(
		withRequestID, // Generate unique ID for each request for tracing
		withLogging,
		withRecovery,
		withParams,
		withRequiredParams,
		withUser(dbc),
		c.withWarmUserPrefs(), // Warm user preferences cache after auth (L3 cache)
	)
	chainRaw := handlerutil.Chain(
		chain,
		slow,
	)

	// Core
	c.Handle("/ping", chain(resp(c.ServePing)))
	c.Handle("/getLicense", chain(resp(c.ServeGetLicence)))
	c.Handle("/getMusicFolders", chain(resp(c.ServeGetMusicFolders)))
	c.Handle("/getUser", chain(resp(c.ServeGetUser)))
	c.Handle("/getUsers", chain(resp(c.ServeGetUsers)))
	c.Handle("/createUser", chain(resp(c.ServeCreateUser)))
	c.Handle("/deleteUser", chain(resp(c.ServeDeleteUser)))
	c.Handle("/changePassword", chain(resp(c.ServeChangePassword)))
	c.Handle("/getScanStatus", chain(resp(c.ServeGetScanStatus)))
	c.Handle("/startScan", chain(resp(c.ServeGetScanStatus)))
	c.Handle("/getOpenSubsonicExtensions", chain(resp(c.ServeGetOpenSubsonicExtensions)))

	// Search
	c.Handle("/search3", chain(resp(c.ServeSearchThree)))
	c.Handle("/search2", chain(resp(c.ServeSearchThree)))

	// Browse (proxy Tidal)
	c.Handle("/getArtists", chain(resp(c.ServeGetArtists)))
	c.Handle("/getArtist", chain(resp(c.ServeGetArtist)))
	c.Handle("/getIndexes", chain(resp(c.ServeGetIndexes)))
	c.Handle("/getArtistInfo", chain(resp(c.ServeGetArtistInfoTwo)))
	c.Handle("/getArtistInfo2", chain(resp(c.ServeGetArtistInfoTwo)))
	c.Handle("/getAlbum", chain(resp(c.ServeGetAlbum)))
	c.Handle("/getAlbumInfo", chain(resp(c.ServeGetAlbumInfoTwo)))
	c.Handle("/getAlbumInfo2", chain(resp(c.ServeGetAlbumInfoTwo)))
	c.Handle("/getAlbumList", chain(resp(c.ServeGetAlbumListTwo)))
	c.Handle("/getAlbumList2", chain(resp(c.ServeGetAlbumListTwo)))
	c.Handle("/getSong", chain(resp(c.ServeGetSong)))

	// Streaming
	c.Handle("/stream", chainRaw(respRaw(c.ServeStream)))
	c.Handle("/download", chainRaw(respRaw(c.ServeDownload)))
	c.Handle("/getCoverArt", chainRaw(respRaw(c.ServeGetCoverArt)))

	// Stars & Ratings
	c.Handle("/star", chain(resp(c.ServeStar)))
	c.Handle("/unstar", chain(resp(c.ServeUnstar)))
	c.Handle("/setRating", chain(resp(c.ServeSetRating)))
	c.Handle("/getStarred2", chain(resp(c.ServeGetStarredTwo)))
	c.Handle("/getStarred", chain(resp(c.ServeGetStarredTwo))) // map v1 to v2

	// Playlists
	c.Handle("/getPlaylists", chain(resp(c.ServeGetPlaylists)))
	c.Handle("/getPlaylist", chain(resp(c.ServeGetPlaylist)))
	c.Handle("/createPlaylist", chain(resp(c.ServeCreatePlaylist)))
	c.Handle("/updatePlaylist", chain(resp(c.ServeUpdatePlaylist)))
	c.Handle("/deletePlaylist", chain(resp(c.ServeDeletePlaylist)))

	// Bookmarks
	c.Handle("/getBookmarks", chain(resp(c.ServeGetBookmarks)))
	c.Handle("/createBookmark", chain(resp(c.ServeCreateBookmark)))
	c.Handle("/deleteBookmark", chain(resp(c.ServeDeleteBookmark)))

	// Play queue & Scrobble
	c.Handle("/savePlayQueue", chain(resp(c.ServeSavePlayQueue)))
	c.Handle("/getPlayQueue", chain(resp(c.ServeGetPlayQueue)))
	c.Handle("/scrobble", chain(resp(c.ServeScrobble)))

	// Discovery
	c.Handle("/getRandomSongs", chain(resp(c.ServeGetRandomSongs)))
	c.Handle("/getSimilarSongs2", chain(resp(c.ServeGetSimilarSongsTwo)))
	c.Handle("/getSimilarSongs", chain(resp(c.ServeGetSimilarSongs)))
	c.Handle("/getTopSongs", chain(resp(c.ServeGetTopSongs)))

	// Browsing / Empty placeholders for mobile compatibility
	c.Handle("/getGenres", chain(resp(c.ServeGetGenres)))
	c.Handle("/getSongsByGenre", chain(resp(c.ServeGetSongsByGenre)))
	c.Handle("/getInternetRadioStations", chain(resp(c.ServeGetInternetRadioStations)))
	c.Handle("/getNewestPodcasts", chain(resp(c.ServeGetNewestPodcasts)))
	c.Handle("/getPodcasts", chain(resp(c.ServeGetPodcasts)))

	// Lyrics
	c.Handle("/getLyricsBySongId", chain(resp(c.ServeGetLyricsBySongID)))

	c.Handle("/", chain(resp(c.ServeNotFound)))

	return c
}

// Close stops all cache cleanup goroutines and releases resources.
// Should be called when shutting down the controller.
func (c *Controller) Close() {
	c.genreCache.Stop()
	c.genreAlbumCache.Stop()
	c.genreCountsCache.Stop()
	c.searchCache.Stop()
	c.negCoverCache.Stop()
	c.hydratedCache.Stop()
}

type (
	handlerSubsonic    func(r *http.Request) *spec.Response
	handlerSubsonicRaw func(w http.ResponseWriter, r *http.Request) *spec.Response
)

func resp(h handlerSubsonic) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := writeResp(w, r, h(r)); err != nil {
			log.Printf("error writing subsonic response: %v\n", err)
		}
	})
}

func respRaw(h handlerSubsonicRaw) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := writeResp(w, r, h(w, r)); err != nil {
			log.Printf("error writing raw subsonic response: %v\n", err)
		}
	})
}

// request colors for visual tracing (rotating palette)
var requestColors = []string{
	"\033[31m", // Red
	"\033[32m", // Green
	"\033[33m", // Yellow
	"\033[34m", // Blue
	"\033[35m", // Magenta
	"\033[36m", // Cyan
}

// colorIndex tracks next color to assign (atomic for thread safety)
var colorIndex int32 = 0

// generateRequestID creates a short unique ID (6 chars) for request tracing
func generateRequestID() string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 6)
	for i := range b {
		b[i] = chars[time.Now().UnixNano()%int64(len(chars))]
		time.Sleep(1 * time.Nanosecond) // Ensure different nano for each char
	}
	return string(b)
}

// getNextRequestColor returns next color from rotating palette
func getNextRequestColor() string {
	idx := atomic.AddInt32(&colorIndex, 1)
	return requestColors[int(idx)%len(requestColors)]
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := generateRequestID()
		color := getNextRequestColor()
		ctx := context.WithValue(r.Context(), CtxRequestID, reqID+":"+color)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := ""
		reqColor := ""
		if val := r.Context().Value(CtxRequestID); val != nil {
			parts := strings.Split(val.(string), ":")
			if len(parts) == 2 {
				reqID = parts[0]
				reqColor = parts[1]
			}
		}

		// Check if running under systemd (no colors)
		runningUnderSystemd := os.Getenv("JOURNAL_STREAM") != "" || os.Getenv("INVOCATION_ID") != ""

		if runningUnderSystemd {
			log.Printf("[SUBS:%s] IN  %s %s", reqID, r.Method, r.URL.RequestURI())
			next.ServeHTTP(w, r)
			log.Printf("[SUBS:%s] OUT %s %s (%v)", reqID, r.Method, r.URL.Path, time.Since(start))
		} else {
			// Color the request ID with unique rotating color for visual tracing
			reset := "\033[0m"
			colorID := reqColor + reqID + reset
			log.Printf("[SUBS:%s] IN  %s %s", colorID, r.Method, r.URL.RequestURI())
			next.ServeHTTP(w, r)
			log.Printf("[SUBS:%s] OUT %s %s (%v)", colorID, r.Method, r.URL.Path, time.Since(start))
		}
	})
}

func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[CRITICAL] Panic in handler %s: %v\nStack: %s", r.URL.Path, err, debug.Stack())
				_ = writeResp(w, r, spec.NewError(0, "internal server error (panic)"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func withParams(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := params.New(r)
		withP := context.WithValue(r.Context(), CtxParams, p)
		next.ServeHTTP(w, r.WithContext(withP))
	})
}

func withRequiredParams(next http.Handler) http.Handler {
	required := []string{"u", "c"}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.Context().Value(CtxParams).(params.Params)
		for _, req := range required {
			if _, err := p.Get(req); err != nil {
				_ = writeResp(w, r, spec.NewError(10, "please provide a %q parameter", req))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func withUser(dbc *db.DB) handlerutil.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := r.Context().Value(CtxParams).(params.Params)
			username, _ := p.Get("u")
			password, _ := p.Get("p")
			token, _ := p.Get("t")
			salt, _ := p.Get("s")

			passwordAuth := token == "" && salt == ""
			tokenAuth := password == ""
			if tokenAuth == passwordAuth {
				_ = writeResp(w, r, spec.NewError(10, "please provide auth"))
				return
			}
			user := dbc.GetUserByName(username)
			if user == nil {
				// Check auto_register setting
				autoRegister := dbc.GetSetting("auto_register", "false")
				if autoRegister != "true" {
					_ = writeResp(w, r, spec.NewError(40, "invalid username"))
					return
				}
				// Auto-register requires password auth (not token auth) to set the initial password
				if tokenAuth || password == "" {
					log.Printf("[AUTH] auto-register rejected: user=%s token_auth=%v pass_len=%d (password required for auto-register)", username, tokenAuth, len(password))
					if tokenAuth {
						_ = writeResp(w, r, spec.NewError(40, "auto-register requires password auth. Please register via web panel first"))
					} else {
						_ = writeResp(w, r, spec.NewError(40, "invalid username"))
					}
					return
				}
				// Auto-create user with the provided password
				// Hash with bcrypt for secure auth, store plaintext for Subsonic token auth
				hashedPass, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
				if err != nil {
					log.Printf("[AUTH] auto-register failed for %s: bcrypt error: %v", username, err)
					_ = writeResp(w, r, spec.NewError(40, "invalid username"))
					return
				}
				newUser := &db.User{
					Name:             username,
					Password:         string(hashedPass), // Bcrypt hash for secure auth
					SubsonicPassword: password,           // Plaintext for Subsonic token auth
					IsAdmin:          false,
				}
				log.Printf("[AUTH] auto-registering user=%s with pass_len=%d", username, len(password))
				if err := dbc.Create(newUser).Error; err != nil {
					log.Printf("[AUTH] auto-register failed for %s: %v", username, err)
					_ = writeResp(w, r, spec.NewError(40, "invalid username"))
					return
				}
				log.Printf("[AUTH] auto-registered new user: %s", username)
				user = newUser
			}
			var credsOk bool
			if tokenAuth {
				credsOk = checkCredsToken(user, token, salt)
			} else {
				credsOk = checkCredsBasic(user.Password, password)
			}
			if !credsOk {
				_ = writeResp(w, r, spec.NewError(40, "invalid password"))
				return
			}
			withU := context.WithValue(r.Context(), CtxUser, user)
			next.ServeHTTP(w, r.WithContext(withU))
		})
	}
}

func slow(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		_ = rc.SetWriteDeadline(time.Time{})
		_ = rc.SetReadDeadline(time.Time{})
		next.ServeHTTP(w, r)
	})
}

func checkCredsToken(user *db.User, token, salt string) bool {
	// Token auth requires plaintext password to compute expected token
	// Use SubsonicPassword if available (set when user has bcrypt Password)
	plainPassword := user.SubsonicPassword
	if plainPassword == "" {
		// Fallback: if Password field is not bcrypt, use it directly (legacy plaintext users)
		if !isBcryptHash(user.Password) {
			plainPassword = user.Password
		} else {
			log.Printf("[AUTH] token auth rejected: user has bcrypt password but no SubsonicPassword set")
			return false
		}
	}
	toHash := fmt.Sprintf("%s%s", plainPassword, salt)
	hash := md5.Sum([]byte(toHash))
	expToken := hex.EncodeToString(hash[:])
	match := token == expToken
	if !match {
		log.Printf("[AUTH] token mismatch: expected=%s got=%s salt=%s pass_len=%d", expToken[:16], token[:16], salt, len(plainPassword))
	}
	return match
}

func checkCredsBasic(password, given string) bool {
	if len(given) >= 4 && given[:4] == "enc:" {
		b, _ := hex.DecodeString(given[4:])
		given = string(b)
	}
	// Check if stored password is a bcrypt hash
	if isBcryptHash(password) {
		err := bcrypt.CompareHashAndPassword([]byte(password), []byte(given))
		return err == nil
	}
	// Legacy: plaintext comparison using constant-time to prevent timing attacks
	return subtle.ConstantTimeCompare([]byte(password), []byte(given)) == 1
}

// isBcryptHash checks if the password is stored as a bcrypt hash
func isBcryptHash(password string) bool {
	return strings.HasPrefix(password, "$2a$") ||
		strings.HasPrefix(password, "$2b$") ||
		strings.HasPrefix(password, "$2y$")
}

func writeResp(w http.ResponseWriter, r *http.Request, resp *spec.Response) error {
	if resp == nil {
		log.Printf("[SUBS] Writing nil response")
		return nil
	}

	status := "ok"
	if resp.Error != nil {
		status = fmt.Sprintf("error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	log.Printf("[SUBS] Writing response status=%s", status)

	var res struct {
		XMLName        xml.Name `xml:"subsonic-response" json:"-"`
		*spec.Response `json:"subsonic-response"`
	}
	res.Response = resp

	p := r.Context().Value(CtxParams).(params.Params)

	switch v, _ := p.Get("f"); v {
	case "json":
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		return enc.Encode(res)

	case "jsonp":
		w.Header().Set("Content-Type", "application/javascript")
		pCall := p.GetOr("callback", "cb")
		if _, err := fmt.Fprintf(w, "%s(", pCall); err != nil {
			return err
		}
		enc := json.NewEncoder(w)
		if err := enc.Encode(res); err != nil {
			return err
		}
		_, err := fmt.Fprint(w, ");")
		return err

	default:
		w.Header().Set("Content-Type", "application/xml")
		if _, err := io.WriteString(w, xml.Header); err != nil {
			return err
		}
		enc := xml.NewEncoder(w)
		enc.Indent("", "    ")
		if err := enc.Encode(res); err != nil {
			return err
		}
		return nil
	}
}

// warmUserPreferences loads all user metadata (stars, ratings, plays) into memory cache
// Call this on any user activity to "warm" the cache for subsequent requests
func (c *Controller) warmUserPreferences(userID int) {
	key := fmt.Sprintf("%d", userID)
	if prefs := c.userPrefsCache.Get(key); prefs != nil {
		return // Already warmed, TTL will refresh on access
	}

	// Load all preferences in 3 batch queries
	prefs := userPreferences{
		Stars:    make(map[string]time.Time),
		Ratings:  make(map[string]int),
		Plays:    make(map[string]int),
		LoadedAt: time.Now(),
	}

	// Get all stars for this user
	var stars []db.TrackStar
	if err := c.dbc.Where("user_id = ?", userID).Find(&stars).Error; err == nil {
		for _, s := range stars {
			prefs.Stars[s.URI] = s.StarDate
		}
	}

	// Get all ratings for this user
	var ratings []db.TrackRating
	if err := c.dbc.Where("user_id = ?", userID).Find(&ratings).Error; err == nil {
		for _, r := range ratings {
			prefs.Ratings[r.URI] = r.Rating
		}
	}

	// Get all plays for this user
	var plays []db.Play
	if err := c.dbc.Where("user_id = ?", userID).Find(&plays).Error; err == nil {
		for _, p := range plays {
			prefs.Plays[p.URI] = p.Count
		}
	}

	c.userPrefsCache.Set(key, &prefs, 0)
}

// getUserPreferences retrieves cached preferences or loads them if missing
func (c *Controller) getUserPreferences(userID int) *userPreferences {
	c.warmUserPreferences(userID)
	key := fmt.Sprintf("%d", userID)
	return c.userPrefsCache.Get(key)
}

// withWarmUserPrefs is a middleware that warms user preferences after authentication
// This ensures any authenticated endpoint gets snappy L3 cache performance
func (c *Controller) withWarmUserPrefs() handlerutil.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get user from context (set by withUser middleware which runs before this)
			if user, ok := r.Context().Value(CtxUser).(*db.User); ok && user != nil {
				// Fire-and-forget warm in background to not block the request
				// If already warmed, this returns immediately
				c.warmUserPreferences(user.ID)
			}
			next.ServeHTTP(w, r)
		})
	}
}
