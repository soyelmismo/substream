package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sentriz/gormstore"
	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrladmin"
	"go.senan.xyz/gonic/server/ctrlsubsonic"
	"go.senan.xyz/gonic/tidalproxy"
)

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

	flag.Parse()

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

	// Tidal Proxy Pool
	urls := strings.Split(*confProxyURLs, ",")
	proxyPool := tidalproxy.NewPool(urls, tidalproxy.PoolConfig{
		HealthInterval: 30 * time.Second,
		Timeout:        10 * time.Second,
	})
	proxy := tidalproxy.NewCachedProxy(proxyPool, dbc, 5*time.Minute)

	// Load proxies from DB if any
	dbProxies, _ := dbc.GetProxies()
	if len(dbProxies) == 0 {
		log.Printf("No proxies in database. Seeding with community defaults")
		seeds := []string{
			"https://monochrome-api.samidy.com",
			"https://api.monochrome.tf",
			"https://hifi.geeked.wtf",
			"https://wolf.qqdl.site",
			"https://maus.qqdl.site",
			"https://vogel.qqdl.site",
			"https://katze.qqdl.site",
		}
		for _, u := range seeds {
			dbc.AddProxy(u, "Community", "auto-seed")
		}
		// also add CLI defaults if not already present
		for _, u := range urls {
			dbc.AddProxy(u, "CLI Default", "cli")
		}

		dbProxies, _ = dbc.GetProxies()
	}

	if len(dbProxies) > 0 {
		var dbURLs []string
		for _, p := range dbProxies {
			dbURLs = append(dbURLs, p.URL)
		}
		proxyPool.SetInstances(dbURLs)
	}

	// Background Auto-Discovery (Trackers)
	trackers := []string{
		"https://tidal-uptime.jiffy-puffs-1j.workers.dev",
		"https://tidal-uptime.props-76styles.workers.dev",
	}
	proxyPool.StartDiscovery(trackers, 30*time.Minute, dbc)

	// Scrobblers (Keep empty for Phase 1 MVP, can add ListenBrainz here)
	var scrobblers []scrobble.Scrobbler

	// Sessions
	sessDB := gormstore.New(dbc.DB, []byte("substream-secret-change-me"))
	go sessDB.PeriodicCleanup(1*time.Hour, make(chan struct{}))

	// Controllers
	ctrlSubsonic := ctrlsubsonic.New(dbc, proxy, scrobblers, *confCachePath)
	resolveProxyPath := func(in string) string {
		if *confProxyPrefix == "" {
			return in
		}
		return *confProxyPrefix + in
	}
	ctrlAdmin, err := ctrladmin.New(dbc, sessDB, proxy, resolveProxyPath)

	if err != nil {
		log.Fatalf("Error initializing admin controller: %v", err)
	}

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
