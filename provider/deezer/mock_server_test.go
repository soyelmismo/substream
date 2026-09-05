package deezer

import (
	"bytes"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/blowfish"
)

const (
	dummyTestARL  = "dummy_arl_test_token_0123456789abcdef0123456789abcdef0123456789abcdef"
	dummyTestSpec = "bf:2048:3:0:0001020304050607:md5x:dummy_secret_key_16_bytes_long"
)

// generateMockEncryptedStream creates a synthetic audio stream where blocks are encrypted
// matching the test StreamFilterProfile specifications.
func generateMockEncryptedStream(trackID string, totalChunks int, magicHeader []byte) []byte {
	profile, err := ParseStreamFilterSpec(dummyTestSpec)
	if err != nil {
		panic(err)
	}
	key, err := profile.DeriveKey(trackID)
	if err != nil {
		panic(err)
	}
	block, err := blowfish.NewCipher(key)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, totalChunks*profile.ChunkSize)
	for c := 0; c < totalChunks; c++ {
		offset := c * profile.ChunkSize
		chunk := buf[offset : offset+profile.ChunkSize]
		for i := range chunk {
			chunk[i] = byte((c*17 + i) % 256)
		}
		if c == 0 && len(magicHeader) > 0 {
			copy(chunk[0:len(magicHeader)], magicHeader)
		}
		if c%profile.Stride == profile.Offset {
			mode := cipher.NewCBCEncrypter(block, profile.IV)
			mode.CryptBlocks(chunk, chunk)
		}
	}
	return buf
}

func generateMockEncryptedFLACStream(trackID string, totalChunks int) []byte {
	return generateMockEncryptedStream(trackID, totalChunks, []byte("fLaC"))
}

// newMockDeezerServer starts a local httptest.Server that mocks:
// 1. /ajax/gw-light.php (deezer.getUserData and song.getData)
// 2. /v1/get_url (media URL resolver)
// 3. /media/track-{id}.flac and .mp3 (encrypted audio streams)
// 4. /track/{id} (public Deezer track metadata API)
// 5. /search/track (search endpoint)
func newMockDeezerServer(t testing.TB) *httptest.Server {
	t.Helper()

	var srv *httptest.Server
	streamDataFLAC := generateMockEncryptedFLACStream("3135556", 520) // ~1.06 MB > 1MB
	streamDataMP3 := generateMockEncryptedStream("3135556", 520, []byte("ID3\x03"))

	startTime := time.Now()

	mux := http.NewServeMux()

	// 1. Gateway light mock
	mux.HandleFunc("/ajax/gw-light.php", func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Query().Get("method")
		w.Header().Set("Content-Type", "application/json")

		switch method {
		case "deezer.getUserData":
			// Verify arl cookie is present and matches the dummy test token
			c, _ := r.Cookie("arl")
			if c == nil || strings.TrimSpace(c.Value) == "" || c.Value != dummyTestARL {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "AUTH_REQUIRED",
					"results": map[string]interface{}{
						"USER": map[string]interface{}{
							"USER_ID": "0",
						},
					},
				})
				return
			}

			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"results": map[string]interface{}{
					"USER": map[string]interface{}{
						"USER_ID":   "6426929883",
						"BLOG_NAME": "CEO The Producer",
						"OPTIONS": map[string]interface{}{
							"license_token":   "mock_license_token_test",
							"web_lossless":    true,
							"web_hq":          true,
							"expiration_date": "2099-12-31",
						},
					},
					"checkForm": "mock_check_form_test_csrf",
				},
			})

		case "song.getData":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"results": map[string]interface{}{
					"SNG_ID":      "3135556",
					"TRACK_TOKEN": "mock_track_token_3135556",
				},
			})

		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"unknown method"}`))
		}
	})

	// 2. Media get_url mock
	mux.HandleFunc("/v1/get_url", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req MediaGetURLRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		chosenFormat := "FLAC"
		for _, m := range req.Media {
			for _, f := range m.Formats {
				if f.Format == "FLAC" || f.Format == "MP3_320" || f.Format == "MP3_128" {
					chosenFormat = f.Format
					break
				}
			}
		}

		ext := "flac"
		if strings.HasPrefix(chosenFormat, "MP3") {
			ext = "mp3"
		}
		cdnURL := fmt.Sprintf("%s/media/track-3135556.%s", srv.URL, ext)

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"media": []map[string]interface{}{
						{
							"format": chosenFormat,
							"sources": []map[string]interface{}{
								{
									"url":      cdnURL,
									"provider": "mock",
								},
							},
						},
					},
				},
			},
		})
	})

	// 3. Encrypted audio CDN stream mock (supports Range and full GET)
	mux.HandleFunc("/media/track-3135556.flac", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "track-3135556.flac", startTime, bytes.NewReader(streamDataFLAC))
	})
	mux.HandleFunc("/media/track-3135556.mp3", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "track-3135556.mp3", startTime, bytes.NewReader(streamDataMP3))
	})

	// 4. Deezer public track metadata API mock
	mux.HandleFunc("/track/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		trackRef := strings.TrimPrefix(r.URL.Path, "/track/")
		switch trackRef {
		case "3135556":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id":          3135556,
				"readable":    true,
				"title":       "One More Time",
				"title_short": "One More Time",
				"duration":    320,
				"artist": map[string]interface{}{
					"id":   27,
					"name": "Daft Punk",
				},
				"album": map[string]interface{}{
					"id":    302127,
					"title": "Discovery",
				},
			})
		case "isrc:USAT21400808":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id":          7654321,
				"readable":    true,
				"title":       "Fire Away (with Kid Harpoon)",
				"title_short": "Fire Away",
				"duration":    240,
				"artist": map[string]interface{}{
					"id":   123,
					"name": "Skrillex",
				},
				"album": map[string]interface{}{
					"id":    456,
					"title": "Recess",
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"type":    "DataException",
					"message": "no data",
					"code":    800,
				},
			})
		}
	})

	// 5. Track search API mock
	mux.HandleFunc("/search/track", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query().Get("q")
		if strings.Contains(strings.ToLower(q), "daft punk") || strings.Contains(strings.ToLower(q), "one more time") {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":          3135556,
						"readable":    true,
						"title":       "One More Time",
						"title_short": "One More Time",
						"duration":    320,
						"artist": map[string]interface{}{
							"id":   27,
							"name": "Daft Punk",
						},
						"album": map[string]interface{}{
							"id":    302127,
							"title": "Discovery",
						},
					},
				},
				"total": 1,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data":  []map[string]interface{}{},
			"total": 0,
		})
	})

	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Route internal Deezer gateway and media calls to this mock server
	resetURLs := SetGatewayURLsForTest(srv.URL+"/ajax/gw-light.php", srv.URL+"/v1/get_url")
	t.Cleanup(resetURLs)

	return srv
}
