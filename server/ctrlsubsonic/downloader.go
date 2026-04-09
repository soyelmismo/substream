package ctrlsubsonic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"go.senan.xyz/gonic/tidalproxy"
)

// downloadAndStitchHLS fetches an M3U8 manifest and streams all its segments to w
func (c *Controller) downloadAndStitchHLS(ctx context.Context, manifestURL string, w io.Writer, clientIP string, track *tidalproxy.TidalTrack) error {
	// 1. Fetch the manifest
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("manifest fetch failed: %d", resp.StatusCode)
	}

	// 2. Parse segments
	var segments []string
	var variantURL string
	scanner := bufio.NewScanner(resp.Body)
	baseURL := manifestURL
	if lastSlash := strings.LastIndex(baseURL, "/"); lastSlash != -1 {
		baseURL = baseURL[:lastSlash+1]
	}

	isMaster := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-STREAM-INF") {
			isMaster = true
			continue
		}
		if strings.HasPrefix(line, "#") {
			if strings.HasPrefix(line, "#EXT-X-MAP:URI=") {
				mapURL := strings.Trim(strings.TrimPrefix(line, "#EXT-X-MAP:URI="), "\"")
				if !strings.HasPrefix(mapURL, "http") {
					mapURL = baseURL + mapURL
				}
				segments = append(segments, mapURL)
			}
			continue
		}

		// URL line
		fullURL := line
		if !strings.HasPrefix(fullURL, "http") {
			fullURL = baseURL + fullURL
		}

		if isMaster {
			variantURL = fullURL
			break
		}
		segments = append(segments, fullURL)
	}

	if isMaster && variantURL != "" {
		log.Printf("[DOWNLOAD] HLS Master detected, following variant: %s", variantURL)
		return c.downloadAndStitchHLS(ctx, variantURL, w, clientIP, track)
	}

	log.Printf("[DOWNLOAD] HLS Media playlist parsed: %d segments found", len(segments))
	if len(segments) > 0 {
		log.Printf("[DOWNLOAD] First segment URL: %s", segments[0])
	}
	if len(segments) == 0 {
		return fmt.Errorf("no segments found in manifest")
	}

	// 3. Download and stream segments with tagging
	tagger := &flacTagger{w: w, track: track}
	for i, segmentURL := range segments {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// fetch segment
		sReq, _ := http.NewRequestWithContext(ctx, "GET", segmentURL, nil)
		sReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
		if clientIP != "" {
			sReq.Header.Set("X-Forwarded-For", clientIP)
		}
		sResp, err := http.DefaultClient.Do(sReq)
		if err != nil {
			return err
		}

		if i == 0 {
			// First segment might need tagging if it's FLAC
			if err := tagger.process(sResp.Body, c); err != nil {
				log.Printf("[DOWNLOAD] tagging failed, falling back to raw: %v", err)
				_, err = io.Copy(w, sResp.Body)
				if err != nil {
					sResp.Body.Close()
					return err
				}
			}
		} else {
			_, err := io.Copy(w, sResp.Body)
			if err != nil {
				sResp.Body.Close()
				return err
			}
		}
		sResp.Body.Close()
	}

	return nil
}

func (c *Controller) downloadAndStitchDASH(ctx context.Context, manifestURL string, w io.Writer, clientIP string, track *tidalproxy.TidalTrack) error {
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
	if clientIP != "" {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	content := string(body)
	baseURL := manifestURL
	if lastSlash := strings.LastIndex(baseURL, "/"); lastSlash != -1 {
		baseURL = baseURL[:lastSlash+1]
	}

	var segments []string
	parts := strings.Split(content, "<BaseURL>")
	for _, part := range parts[1:] {
		end := strings.Index(part, "</BaseURL>")
		if end > 0 {
			u := strings.ReplaceAll(part[:end], "&amp;", "&")
			if !strings.HasPrefix(u, "http") {
				u = baseURL + u
			}
			segments = append(segments, u)
		}
	}

	if len(segments) == 0 {
		return fmt.Errorf("no segments found in DASH")
	}

	tagger := &flacTagger{w: w, track: track}
	for i, segmentURL := range segments {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sReq, _ := http.NewRequestWithContext(ctx, "GET", segmentURL, nil)
		sReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
		if clientIP != "" {
			sReq.Header.Set("X-Forwarded-For", clientIP)
		}
		sResp, err := http.DefaultClient.Do(sReq)
		if err == nil {
			if i == 0 {
				if err := tagger.process(sResp.Body, c); err != nil {
					_, err = io.Copy(w, sResp.Body)
					if err != nil {
						sResp.Body.Close()
						return err
					}
				}
			} else {
				_, err := io.Copy(w, sResp.Body)
				if err != nil {
					sResp.Body.Close()
					return err
				}
			}
			sResp.Body.Close()
		}
	}

	return nil
}

type flacTagger struct {
	w     io.Writer
	track *tidalproxy.TidalTrack
}

func (t *flacTagger) process(r io.Reader, c *Controller) error {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	rw, isResponse := t.w.(http.ResponseWriter)
	isFlac := string(header) == "fLaC"

	// Resolve the headers dynamically before flushing any bytes
	if isResponse {
		cleanName := strings.ReplaceAll(fmt.Sprintf("%s - %s", t.track.Artist.Name, t.track.Title), "/", "_")
		if isFlac {
			rw.Header().Set("Content-Type", "audio/flac")
			rw.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", cleanName+".flac"))
		} else {
			rw.Header().Set("Content-Type", "audio/mp4")
			rw.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", cleanName+".m4a"))
		}
		rw.Header().Set("Transfer-Encoding", "chunked")
	}

	if !isFlac {
		// Not a FLAC file
		t.w.Write(header)
		_, err := io.Copy(t.w, r)
		return err
	}

	// We have a FLAC header
	t.w.Write(header)

	var blocks [][]byte

	// Read all original metadata blocks
	for {
		blockHead := make([]byte, 4)
		if _, err := io.ReadFull(r, blockHead); err != nil {
			return err
		}

		last := blockHead[0]&0x80 != 0
		typ := blockHead[0] & 0x7F
		length := int(blockHead[1])<<16 | int(blockHead[2])<<8 | int(blockHead[3])

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}

		// Keep blocks except existing VORBIS_COMMENT (4) and PICTURE (6)
		if typ != 4 && typ != 6 {
			blocks = append(blocks, append(blockHead, data...))
		}

		if last {
			break
		}
	}

	// Inject new blocks
	commentBlock := t.makeVorbisComment()
	pictureBlock := t.makePictureBlock(c)

	blocks = append(blocks, commentBlock)
	if pictureBlock != nil {
		blocks = append(blocks, pictureBlock)
	}

	// Write reconstructed blocks
	for i, b := range blocks {
		if i == len(blocks)-1 {
			b[0] |= 0x80 // Set last block flag
		} else {
			b[0] &= 0x7F // Clear last block flag
		}
		t.w.Write(b)
	}

	// Copy the audio frames
	_, err := io.Copy(t.w, r)
	return err
}

func (t *flacTagger) makeVorbisComment() []byte {
	vendor := "Substream"

	tags := map[string]string{
		"TITLE": t.track.Title,
	}

	if t.track.Artist.Name != "" {
		tags["ARTIST"] = t.track.Artist.Name
	}
	if t.track.Album.Title != "" {
		tags["ALBUM"] = t.track.Album.Title
	}
	if t.track.TrackNumber > 0 {
		tags["TRACKNUMBER"] = fmt.Sprint(t.track.TrackNumber)
	}
	if t.track.VolumeNumber > 0 {
		tags["DISCNUMBER"] = fmt.Sprint(t.track.VolumeNumber)
	}
	if t.track.Album.ReleaseDate != "" {
		tags["DATE"] = t.track.Album.ReleaseDate
	}
	if t.track.ISRC != "" {
		tags["ISRC"] = t.track.ISRC
	}
	if len(t.track.Artists) > 0 {
		tags["ALBUMARTIST"] = t.track.Artists[0].Name
	} else if t.track.Artist.Name != "" {
		tags["ALBUMARTIST"] = t.track.Artist.Name
	}

	buf := new(bytes.Buffer)
	// vendor length (4 bytes le)
	binary.Write(buf, binary.LittleEndian, uint32(len(vendor)))
	buf.WriteString(vendor)

	// list length (4 bytes le)
	binary.Write(buf, binary.LittleEndian, uint32(len(tags)))
	for k, v := range tags {
		entry := k + "=" + v
		binary.Write(buf, binary.LittleEndian, uint32(len(entry)))
		buf.WriteString(entry)
	}

	data := buf.Bytes()
	header := make([]byte, 4)
	header[0] = 4 // type VORBIS_COMMENT
	header[1] = byte(len(data) >> 16)
	header[2] = byte(len(data) >> 8)
	header[3] = byte(len(data))

	return append(header, data...)
}

func (t *flacTagger) makePictureBlock(c *Controller) []byte {
	coverUUID := t.track.Album.Cover
	if coverUUID == "" {
		return nil
	}

	coverURL := c.proxy.GetCoverURL(coverUUID, 640)
	resp, err := http.Get(coverURL)
	if err != nil || resp.StatusCode != 200 {
		return nil
	}
	defer resp.Body.Close()
	imgData, _ := io.ReadAll(resp.Body)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(3)) // type: front cover

	mime := "image/jpeg"
	binary.Write(buf, binary.BigEndian, uint32(len(mime)))
	buf.WriteString(mime)

	desc := "Cover"
	binary.Write(buf, binary.BigEndian, uint32(len(desc)))
	buf.WriteString(desc)

	// width, height, depth, colors (dummy values for now)
	binary.Write(buf, binary.BigEndian, uint32(640))
	binary.Write(buf, binary.BigEndian, uint32(640))
	binary.Write(buf, binary.BigEndian, uint32(24))
	binary.Write(buf, binary.BigEndian, uint32(0))

	binary.Write(buf, binary.BigEndian, uint32(len(imgData)))
	buf.Write(imgData)

	data := buf.Bytes()
	header := make([]byte, 4)
	header[0] = 6 // type PICTURE
	header[1] = byte(len(data) >> 16)
	header[2] = byte(len(data) >> 8)
	header[3] = byte(len(data))

	return append(header, data...)
}
