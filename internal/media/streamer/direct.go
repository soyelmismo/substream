package streamer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"go.senan.xyz/gonic/internal/media/tagger"
)

// Download streams audio from a direct progressive URL and tags it if applicable
func Download(ctx context.Context, streamURL string, w io.Writer, meta *tagger.Metadata, clientIP string, client *http.Client) error {
	if strings.HasPrefix(streamURL, "/") {
		f, err := os.Open(streamURL)
		if err != nil {
			return fmt.Errorf("open local stream file: %w", err)
		}
		defer f.Close()

		if meta != nil {
			return tagger.TagFLAC(ctx, f, w, meta, client)
		}

		_, err = io.Copy(w, f)
		return err
	}
	if client == nil {
		client = &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", streamURL, nil)
	if err != nil {
		return fmt.Errorf("create direct stream request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("execute direct stream request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("direct stream returned HTTP %d", resp.StatusCode)
	}

	if meta != nil {
		return tagger.TagFLAC(ctx, resp.Body, w, meta, client)
	}

	_, err = io.Copy(w, resp.Body)
	return err
}
