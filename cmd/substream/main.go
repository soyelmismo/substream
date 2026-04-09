package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"
	"os"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/scrobble"
	"go.senan.xyz/gonic/server/ctrladmin"
	"go.senan.xyz/gonic/server/ctrlsubsonic"
	"go.senan.xyz/gonic/tidalproxy"
	"github.com/sentriz/gormstore"
)

func main() {
	confListenAddr := flag.String("listen-addr", "0.0.0.0:4533", "listen address")
	confDBPath := flag.String("db-path", "substream.db", "database path")
	confCachePath := flag.String("cache-path", "./cache", "cache directory (covers)")
	confProxyURLs := flag.String("proxy-urls", "http://localhost:8000", "comma-separated hifi-api URLs")
	confProxyPrefix := flag.String("proxy-prefix", "", "URL path prefix if behind reverse proxy")

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

	if dbc.UserCount() == 0 {
		log.Printf("No users found. Creating default 'admin' user with password 'admin'")
		admin := db.User{
			Name:    "admin",
			Password: "admin",
			IsAdmin: true,
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
	proxy := tidalproxy.NewCachedProxy(proxyPool, 5*time.Minute)

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

	// Serve
	server := &http.Server{
		Addr:         *confListenAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server stopped: %v", err)
	}
}
