package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sentriz/gormstore"
	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/lastfm"
	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/provider/amazon"
	"go.senan.xyz/gonic/provider/deezer"
	"go.senan.xyz/gonic/provider/qobuz"
	"go.senan.xyz/gonic/provider/soundcloud"
	"go.senan.xyz/gonic/provider/spotify"
	"go.senan.xyz/gonic/provider/tidal"
	"go.senan.xyz/gonic/provider/ytmusic"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrladmin"
	"go.senan.xyz/gonic/server/ctrlsubsonic"
)

// compactLogWriter wraps log output with compact timestamp format
type compactLogWriter struct{}

func (w *compactLogWriter) Write(p []byte) (n int, err error) {
	msg := string(p)
	// Remove trailing newline if present
	msg = strings.TrimSuffix(msg, "\n")

	// Check if running under systemd/journald (it adds its own timestamp, no colors)
	runningUnderSystemd := os.Getenv("JOURNAL_STREAM") != "" || os.Getenv("INVOCATION_ID") != ""

	// Determine level
	level := "INF"
	if strings.Contains(msg, "error") || strings.Contains(msg, "Error") || strings.Contains(msg, "fatal") {
		level = "ERR"
	} else if strings.Contains(msg, "warn") || strings.Contains(msg, "Warn") {
		level = "WRN"
	}

	// Always show compact timestamp
	now := time.Now()
	timestamp := fmt.Sprintf("%02d/%02d %02d:%02d:%02d", now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())

	if runningUnderSystemd {
		// Under journald: plain text with timestamp (for --output=cat)
		fmt.Printf("%s [%s] %s\n", timestamp, level, msg)
	} else {
		// Direct terminal: colors + timestamp
		color := "\033[36m" // Cyan for INF
		if level == "ERR" {
			color = "\033[31m" // Red
		} else if level == "WRN" {
			color = "\033[33m" // Yellow
		}
		reset := "\033[0m"
		fmt.Printf("%s %s%s%s %s\n", timestamp, color, level, reset, msg)
	}
	return len(p), nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func main() {
	confListenAddr := flag.String("listen-addr", getEnv("SUBSTREAM_LISTEN_ADDR", "0.0.0.0:4533"), "listen address")
	confDBPath := flag.String("db-path", getEnv("SUBSTREAM_DB_PATH", "substream.db"), "database path")
	confCachePath := flag.String("cache-path", getEnv("SUBSTREAM_CACHE_PATH", "./cache"), "cache directory")
	confProxyURLs := flag.String("proxy-urls", getEnv("SUBSTREAM_PROXY_URLS", "http://localhost:8000"), "comma-separated hifi-api URLs")
	confProxyPrefix := flag.String("proxy-prefix", getEnv("SUBSTREAM_PROXY_PREFIX", ""), "URL path prefix")
	confCertPath := flag.String("cert-path", getEnv("SUBSTREAM_CERT_PATH", ""), "path to SSL certificate")
	confKeyPath := flag.String("key-path", getEnv("SUBSTREAM_KEY_PATH", ""), "path to SSL key")

	// Provider Configuration Flags
	confTidalToken := flag.String("tidal-token", getEnv("SUBSTREAM_TIDAL_TOKEN", getEnv("SUBSTREAM_TIDAL_TOKENS", "")), "optional Tidal subscriber session token(s), comma-separated for multi-account rotation")
	confQobuzToken := flag.String("qobuz-token", getEnv("SUBSTREAM_QOBUZ_TOKEN", ""), "optional Qobuz user auth token for full Hi-Res FLAC")
	confQobuzURL := flag.String("qobuz-url", getEnv("SUBSTREAM_QOBUZ_URL", ""), "optional custom Qobuz proxy/mirror URL")
	confDeezerURL := flag.String("deezer-url", getEnv("SUBSTREAM_DEEZER_URL", "https://dzr.tabs-vs-spaces.wtf"), "Deezer proxy/mirror URL")
	confDeezerARL := flag.String("deezer-arl", getEnv("SUBSTREAM_DEEZER_ARL", ""), "optional Deezer ARL cookie")
	confDeezerStreamFilter := flag.String("deezer-stream-filter", getEnv("SUBSTREAM_DEEZER_STREAM_FILTER", getEnv("DEEZER_STREAM_FILTER", "")), "optional stream filter activation token/spec for direct native stream decryption")
	confAmazonURL := flag.String("amazon-url", getEnv("SUBSTREAM_AMAZON_URL", "https://amz.geeked.wtf"), "Amazon Music proxy/mirror URL")
	confSpotifySPDC := flag.String("spotify-sp-dc", getEnv("SUBSTREAM_SPOTIFY_SP_DC", getEnv("SPOTIFY_SP_DC", "")), "optional Spotify sp_dc session cookie for Widevine audio playback")
	confSpotifyWVD := flag.String("spotify-wvd-path", getEnv("SUBSTREAM_SPOTIFY_WVD_PATH", getEnv("SPOTIFY_WVD_PATH", "spotify.wvd")), "path to Widevine .wvd device file for Spotify audio decryption")

	flag.Parse()

	// Configure compact colored logging
	log.SetFlags(0) // Remove standard date/time flags
	log.SetOutput(&compactLogWriter{})

	log.Printf("Starting SubStream on %s", *confListenAddr)

	os.MkdirAll(*confCachePath, 0755)

	// DB
	dbc, err := db.New(*confDBPath)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	if err := dbc.Migrate(); err != nil {
		log.Fatalf("Error migrating database schema: %v", err)
	}
	log.Printf("Database ready at %s", *confDBPath)

	// Start background cache maintenance (cleanup expired/old entries every hour, max 100k entries)
	dbc.StartCacheMaintenance(1*time.Hour, 100000)

	if dbc.UserCount() == 0 {
		log.Printf("No users found. Creating default 'admin' user with password 'admin'")
		admin := db.User{
			Name:     "admin",
			Password: "admin",
			IsAdmin:  true,
		}
		if err := dbc.Create(&admin).Error; err != nil {
			log.Fatalf("Error creating default admin user: %v", err)
		}
	}

	// Multi-Provider Registry
	providers := provider.NewRegistry()

	// 1. Tidal Provider (self-contained mirror pool, default seeds, and auto-discovery)
	cliURLs := strings.Split(*confProxyURLs, ",")
	tidalProvider := tidal.Bootstrap(dbc, cliURLs, *confTidalToken)
	providers.Register(tidalProvider)

	// 2. Qobuz Provider (Hi-Res Lossless FLAC)
	qobuzProvider := qobuz.New(qobuz.ClientOptions{
		UserAuthToken: *confQobuzToken,
		CustomURL:     *confQobuzURL,
	})
	providers.Register(qobuzProvider)
	log.Printf("[PROVIDER] Registered Qobuz provider (qb)")

	// 3. Deezer Provider (Lossless FLAC)
	deezerProvider := deezer.New(deezer.ClientOptions{
		CustomURL: *confDeezerURL,
		ARL:       *confDeezerARL,
		StreamFilter: *confDeezerStreamFilter,
		CacheDir:  filepath.Join(*confCachePath, "deezer"),
		DB:        dbc,
	})
	if dzAccounts, err := dbc.GetActiveDeezerAccounts(); err == nil && len(dzAccounts) > 0 {
		deezerProvider.SyncAccounts(dzAccounts)
	}
	providers.Register(deezerProvider)
	log.Printf("[PROVIDER] Registered Deezer provider (dz) -> URL: %s", *confDeezerURL)

	// 4. Amazon Music Provider
	amazonProvider := amazon.New(amazon.ClientOptions{
		CustomURL: *confAmazonURL,
	})
	providers.Register(amazonProvider)
	log.Printf("[PROVIDER] Registered Amazon Music provider (am) -> URL: %s", *confAmazonURL)

	// 5. YouTube Music Provider (Universal Fallback & Opus 160kbps streaming)
	ytProvider := ytmusic.New()
	if ytAccounts, err := dbc.GetActiveYTMusicAccounts(); err == nil && len(ytAccounts) > 0 {
		ytProvider.SyncAccounts(ytAccounts)
	}
	providers.Register(ytProvider)
	log.Printf("[PROVIDER] Registered YouTube Music provider (yt)")

	// 6. Spotify Provider (Universal Metadata, ISRC Fallback & Native Widevine MP4_128 AAC)
	spotifyProvider := spotify.New(providers, spotify.Options{
		SPDC:     *confSpotifySPDC,
		WVDPath:  *confSpotifyWVD,
		CacheDir: filepath.Join(*confCachePath, "spotify"),
	})
	providers.Register(spotifyProvider)
	log.Printf("[PROVIDER] Registered Spotify provider (sp)")

	// 7. SoundCloud Provider (Independent tracks, sets, remixes & progressive MP3)
	scProvider := soundcloud.New()
	providers.Register(scProvider)
	log.Printf("[PROVIDER] Registered SoundCloud provider (sc)")

	// Load persistent provider toggle settings (metadata & audio)
	providers.LoadDBSettings(dbc.GetSetting)

	// Scrobblers (Per-user Last.fm scrobbling)
	lastfmClient := lastfm.NewClient("Substream/1.0", func() (string, string, error) {
		k := dbc.GetSetting("lastfm_api_key", "")
		s := dbc.GetSetting("lastfm_api_secret", "")
		if k == "" || s == "" {
			return "", "", errors.New("last.fm api key/secret not set")
		}
		return k, s, nil
	})
	scrobblers := []scrobble.Scrobbler{lastfmClient}

	// Sessions
	sessDB := gormstore.New(dbc.DB, []byte("substream-secret-change-me"))
	go sessDB.PeriodicCleanup(1*time.Hour, make(chan struct{}))

	// Controllers
	ctrlSubsonic := ctrlsubsonic.New(dbc, tidalProvider.Proxy(), providers, scrobblers, *confCachePath)
	resolveProxyPath := func(in string) string {
		if *confProxyPrefix == "" {
			return in
		}
		return *confProxyPrefix + in
	}
	ctrlAdmin, err := ctrladmin.New(dbc, sessDB, tidalProvider.Proxy(), resolveProxyPath)
	if err != nil {
		log.Fatalf("Error initializing admin controller: %v", err)
	}
	ctrlAdmin.SetProviders(providers)

	// Routes
	mux := http.NewServeMux()

	// Add prefix if given
	restPath := "/rest/"
	adminPath := "/admin/"
	if *confProxyPrefix != "" {
		restPath = *confProxyPrefix + restPath
		adminPath = *confProxyPrefix + adminPath
	}

	mux.Handle(restPath, http.StripPrefix(strings.TrimRight(restPath, "/"), ctrlSubsonic))
	mux.Handle(adminPath, http.StripPrefix(strings.TrimRight(adminPath, "/"), ctrlAdmin))

	// Root redirect: redirect "/" directly to web panel / login
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, adminPath, http.StatusFound)
	})

	// Convenience "/login" shortcut -> "/admin/login"
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, adminPath+"login", http.StatusFound)
	})

	// CORS & Path Cleaning Middleware
	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Range")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Clean extension for Subsonic compatibility (e.g., ping.view -> ping)
		path := r.URL.Path
		if strings.HasSuffix(path, ".view") {
			r.URL.Path = strings.TrimSuffix(path, ".view")
		} else if strings.HasSuffix(path, ".jsp") {
			r.URL.Path = strings.TrimSuffix(path, ".jsp")
		}

		mux.ServeHTTP(w, r)
	})

	// Serve
	server := &http.Server{
		Addr:         *confListenAddr,
		Handler:      corsHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Printf("Shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}

		// Close controller and stop cache cleanup goroutines
		ctrlSubsonic.Close()
		log.Printf("Server stopped gracefully")
	}()

	if *confCertPath != "" && *confKeyPath != "" {
		log.Printf("Starting HTTPS server on %s", *confListenAddr)
		if err := server.ListenAndServeTLS(*confCertPath, *confKeyPath); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTPS Server stopped: %v", err)
		}
	} else {
		log.Printf("Starting HTTP server on %s", *confListenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP Server stopped: %v", err)
		}
	}
}
