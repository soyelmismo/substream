package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"go.senan.xyz/gonic/db"
	//"go.senan.xyz/gonic/lastfm"
	//"go.senan.xyz/gonic/listenbrainz"
	//"go.senan.xyz/gonic/scrobble"
	//"go.senan.xyz/gonic/server/ctrladmin"
	//"go.senan.xyz/gonic/server/ctrlsubsonic"
	//"go.senan.xyz/gonic/tidalproxy"
)

func main() {
	// Flags mínimos (sin music-path, sin scan, sin transcode, sin jukebox, sin podcast)
	confListenAddr := flag.String("listen-addr", "0.0.0.0:4533", "listen address")
	confDBPath := flag.String("db-path", "substream.db", "database path")
	confCachePath := flag.String("cache-path", "./cache", "cache directory (covers)")
	confProxyURLs := flag.String("proxy-urls", "http://localhost:8000", "comma-separated hifi-api URLs")
	//confProxyPrefix := flag.String("proxy-prefix", "", "URL path prefix if behind reverse proxy")

	flag.Parse()

	log.Printf("Starting SubStream on %s", *confListenAddr)

	// DB
	dbc, err := db.New(*confDBPath)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	if err := dbc.Migrate(); err != nil {
		log.Fatalf("Error migrating database schema: %v", err)
	}
	log.Printf("Database loaded from %s", *confDBPath)

	// Tidal Proxy Pool (TODO: Uncomment when tidalproxy is implemented)
	_ = strings.Split(*confProxyURLs, ",")
	_ = confCachePath
	/*
		proxy := tidalproxy.NewPool(urls, tidalproxy.PoolConfig{
			HealthInterval: 30 * time.Second,
			Timeout:        10 * time.Second,
		})

		// Scrobblers
		lastfmClient := lastfm.NewClient(...)
		lbClient := listenbrainz.NewClient()
		scrobblers := []scrobble.Scrobbler{lastfmClient, lbClient}

		// Controllers
		ctrlSubsonic := ctrlsubsonic.New(dbc, proxy, scrobblers, *confCachePath)
		ctrlAdmin := ctrladmin.New(dbc, proxy)

		// Routes
		mux := http.NewServeMux()
		mux.Handle("/rest/", http.StripPrefix("/rest", ctrlSubsonic))
		mux.Handle("/admin/", http.StripPrefix("/admin", ctrlAdmin))

		// Serve
		server := &http.Server{
			Addr: *confListenAddr,
			Handler: mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Server stopped: %v", err)
		}
	*/

	// Temporary mock server until controllers are updated
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})

	server := &http.Server{
		Addr:         *confListenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server stopped: %v", err)
	}
}
