package ctrlsubsonic

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/handlerutil"
	"go.senan.xyz/gonic/internal/cache"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrlsubsonic/params"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/tidalproxy"
)

type CtxKey int

const (
	CtxUser CtxKey = iota
	CtxSession
	CtxParams
)

type Controller struct {
	*http.ServeMux

	dbc          *db.DB
	proxy        tidalproxy.TidalProxy
	scrobblers   []scrobble.Scrobbler
	cachePath    string
	searchCache  *cache.Cache[cachedSearch]
	proxySem     chan struct{}
	coverLocks   sync.Map // dedup concurrent cover requests
	hotLocks     sync.Map // dedup concurrent hot.monochrome.tf requests
	httpClient   *http.Client // HTTP client for external requests
	genreCache   *cache.Cache[[]int] // Cache for genre tracks with LRU eviction
	genreAlbumCache *cache.Cache[[]tidalproxy.TidalAlbum] // Cache for genre albums
	genreCountsCache *cache.Cache[map[string]genreCount] // Cache for genre counts with TTL
	negCoverCache *cache.Cache[bool] // Negative cache for missing covers
	settingsCache *cache.Cache[string] // Cache for DB settings (proxy_streams, etc)
	streamURLCache *cache.Cache[string] // Cache for stream URLs (TTL 30s)
	streamURLLocks sync.Map // dedup concurrent stream URL requests
}

func New(dbc *db.DB, proxy tidalproxy.TidalProxy, scrobblers []scrobble.Scrobbler, cachePath string) *Controller {
	// Load configuration from environment variables
	initConfig()

	c := &Controller{
		ServeMux:   http.NewServeMux(),
		dbc:        dbc,
		proxy:      proxy,
		scrobblers: scrobblers,
		cachePath:  cachePath,
		proxySem:   make(chan struct{}, 30), // limit total concurrent proxy calls
		httpClient: &http.Client{
			Timeout: httpClientTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        httpMaxIdleConns,
				MaxIdleConnsPerHost: httpMaxIdleConnsPerHost,
				IdleConnTimeout:     httpIdleConnTimeout,
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
	}

	chain := handlerutil.Chain(
		withLogging,
		withRecovery,
		withParams,
		withRequiredParams,
		withUser(dbc),
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
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("[SUBS] IN  %s %s", r.Method, r.URL.RequestURI())
		next.ServeHTTP(w, r)
		log.Printf("[SUBS] OUT %s %s (%v)", r.Method, r.URL.Path, time.Since(start))
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
					_ = writeResp(w, r, spec.NewError(40, "invalid username"))
					return
				}
				// Auto-create user with the provided password
				newUser := &db.User{
					Name:     username,
					Password: password, // Stored as plaintext (gonic convention)
					IsAdmin:  false,
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
				credsOk = checkCredsToken(user.Password, token, salt)
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

func checkCredsToken(password, token, salt string) bool {
	toHash := fmt.Sprintf("%s%s", password, salt)
	hash := md5.Sum([]byte(toHash))
	expToken := hex.EncodeToString(hash[:])
	match := token == expToken
	if !match {
		log.Printf("[AUTH] token mismatch: expected=%s got=%s salt=%s pass_len=%d", expToken[:16], token[:16], salt, len(password))
	}
	return match
}

func checkCredsBasic(password, given string) bool {
	if len(given) >= 4 && given[:4] == "enc:" {
		b, _ := hex.DecodeString(given[4:])
		given = string(b)
	}
	return password == given
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
