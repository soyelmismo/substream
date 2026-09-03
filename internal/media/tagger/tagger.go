package tagger

import (
	"context"
	"io"
	"net/http"
	"time"
)

// Metadata contains normalized audio tags and cover art information
// shared across all providers (Tidal, YouTube Music, Qobuz, Deezer, etc.)
type Metadata struct {
	Title       string
	Artist      string
	Album       string
	AlbumArtist string
	TrackNumber int
	DiscNumber  int
	Date        string
	Year        string
	ISRC        string
	Comment     string
	CoverURL    string
	CoverBytes  []byte
}

// FetchCover downloads cover art if CoverBytes is nil and CoverURL is populated
func (m *Metadata) FetchCover(ctx context.Context, client *http.Client) []byte {
	if len(m.CoverBytes) > 0 {
		return m.CoverBytes
	}
	if m.CoverURL == "" {
		return nil
	}

	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}

	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", m.CoverURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil || len(data) == 0 {
		return nil
	}

	m.CoverBytes = data
	return data
}
