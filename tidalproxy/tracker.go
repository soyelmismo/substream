package tidalproxy

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"


	"go.senan.xyz/gonic/db"
)

type trackerJSON struct {
	API []struct {
		URL string `json:"url"`
	} `json:"api"`
}

func (p *Pool) StartDiscovery(trackers []string, interval time.Duration, dbc *db.DB) {
	if len(trackers) == 0 {
		return
	}

	log.Printf("tidalproxy: starting discovery worker (trackers: %d, interval: %v)", len(trackers), interval)
	
	fetch := func() {
		allURLs := make(map[string]bool)
		for _, tURL := range trackers {
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Get(tURL)
			if err != nil {
				log.Printf("tidalproxy: discovery fetch error (%s): %v", tURL, err)
				continue
			}
			var data trackerJSON
			err = json.NewDecoder(resp.Body).Decode(&data)
			resp.Body.Close()
			if err != nil {
				log.Printf("tidalproxy: discovery decode error (%s): %v", tURL, err)
				continue
			}

			found := 0
			for _, item := range data.API {
				u := strings.TrimRight(item.URL, "/")
				if u == "" {
					continue
				}
				allURLs[u] = true
				found++

				// Sync with DB
				var inst db.ProxyInstance
				dbc.Where("url = ?", u).First(&inst)
				if inst.ID == 0 {
					// New automatic proxy
					inst.URL = u
					inst.Name = "Auto-discovered"
					inst.Source = tURL
					inst.IsHealthy = true
					if err := dbc.Create(&inst).Error; err != nil {
						log.Printf("tidalproxy: failed to save auto-proxy %s: %v", u, err)
					}
				} else if inst.Source != "manual" {
					// Update source if it's already an auto proxy
					inst.Source = tURL
					dbc.Save(&inst)
				}
			}
			// log.Printf("tidalproxy: found %d proxies from %s", found, tURL)
		}

		// Reload pool from DB to include manual + new auto proxies
		var dbProxies []db.ProxyInstance
		dbc.Find(&dbProxies)
		
		urls := make([]string, len(dbProxies))
		for i, dp := range dbProxies {
			urls[i] = dp.URL
		}
		p.SetInstances(urls)
	}

	// Initial fetch
	fetch()

	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			fetch()
		}
	}()
}
