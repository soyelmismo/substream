package hls

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// SegmentInfo contains URL, duration and whether this segment is an fMP4 init segment
type SegmentInfo struct {
	URL      string
	Duration float64
	IsInit   bool
}

// ParseManifest fetches and parses an HLS manifest (supports HTTP URLs and data: URIs).
// If the playlist is a master playlist, it returns the variantURL.
// Otherwise, it returns the list of media and init segments.
func ParseManifest(ctx context.Context, manifestURL string, clientIP string, client *http.Client) (segments []SegmentInfo, variantURL string, err error) {
	var manifestReader io.Reader
	if strings.HasPrefix(manifestURL, "data:") {
		commaIdx := strings.Index(manifestURL, ",")
		if commaIdx == -1 {
			return nil, "", fmt.Errorf("invalid data URL for manifest")
		}
		dataPart := manifestURL[commaIdx+1:]
		if strings.Contains(manifestURL[:commaIdx], ";base64") {
			decoded, err := base64.StdEncoding.DecodeString(dataPart)
			if err != nil {
				return nil, "", fmt.Errorf("decode base64 manifest: %w", err)
			}
			manifestReader = bytes.NewReader(decoded)
		} else {
			manifestReader = strings.NewReader(dataPart)
		}
	} else {
		if client == nil {
			client = http.DefaultClient
		}
		req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
		if err != nil {
			return nil, "", err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		if clientIP != "" {
			req.Header.Set("X-Forwarded-For", clientIP)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, "", fmt.Errorf("manifest fetch failed: %d", resp.StatusCode)
		}
		manifestBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, "", err
		}
		manifestReader = bytes.NewReader(manifestBytes)
	}

	scanner := bufio.NewScanner(manifestReader)
	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	baseURL := manifestURL
	if lastSlash := strings.LastIndex(baseURL, "/"); lastSlash != -1 {
		baseURL = baseURL[:lastSlash+1]
	}

	isMaster := false
	var currentDuration float64
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-STREAM-INF") {
			isMaster = true
			continue
		}
		if strings.HasPrefix(line, "#EXTINF:") {
			durStr := strings.TrimPrefix(line, "#EXTINF:")
			durStr = strings.TrimSuffix(durStr, ",")
			_, _ = fmt.Sscanf(durStr, "%f", &currentDuration)
			continue
		}
		if strings.HasPrefix(line, "#") {
			if strings.HasPrefix(line, "#EXT-X-MAP:URI=") {
				mapURL := strings.Trim(strings.TrimPrefix(line, "#EXT-X-MAP:URI="), "\"")
				if !strings.HasPrefix(mapURL, "http") {
					mapURL = baseURL + mapURL
				}
				segments = append(segments, SegmentInfo{URL: mapURL, Duration: 0, IsInit: true})
			}
			continue
		}

		fullURL := line
		if !strings.HasPrefix(fullURL, "http") {
			fullURL = baseURL + fullURL
		}

		if isMaster {
			variantURL = fullURL
			break
		}
		segments = append(segments, SegmentInfo{URL: fullURL, Duration: currentDuration, IsInit: false})
		currentDuration = 0
	}

	if err := scanner.Err(); err != nil {
		return nil, "", fmt.Errorf("manifest scanner error: %w", err)
	}

	return segments, variantURL, nil
}
