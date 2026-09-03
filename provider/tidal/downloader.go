package tidal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.senan.xyz/gonic/internal/media/hls"
	"go.senan.xyz/gonic/internal/media/remux"
	"go.senan.xyz/gonic/internal/media/tagger"
	"go.senan.xyz/gonic/provider"
)

// DownloadTrack implements provider.MediaDownloader for Tidal tracks.
// It fetches the HLS manifest, stitches segments in parallel, remuxes fMP4 FLAC losslessly,
// and embeds Vorbis comments with cover art into the output stream.
func (p *Provider) DownloadTrack(ctx context.Context, req provider.DownloadRequest, w io.Writer) error {
	streamURL, err := p.GetStreamURL(ctx, req.TrackID, req.Quality, req.ClientIP)
	if err != nil {
		return fmt.Errorf("tidal stream url: %w", err)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}

	segments, variantURL, err := hls.ParseManifest(ctx, streamURL, req.ClientIP, client)
	if err != nil {
		return fmt.Errorf("tidal parse manifest: %w", err)
	}
	if variantURL != "" {
		segments, _, err = hls.ParseManifest(ctx, variantURL, req.ClientIP, client)
		if err != nil {
			return fmt.Errorf("tidal parse variant manifest: %w", err)
		}
	}

	opts := hls.Options{
		ClientIP:    req.ClientIP,
		HTTPClient:  client,
		WorkerCount: 4,
	}

	var remuxSession *remux.Session
	var taggerDone chan struct{}

	defer func() {
		if remuxSession != nil {
			_ = remuxSession.Close()
			_ = remuxSession.Wait()
		}
		if taggerDone != nil {
			<-taggerDone
		}
	}()

	err = hls.Stitch(ctx, segments, 0, w, opts, func(initData, firstMediaData []byte) (io.Writer, error) {
		isFMP4 := len(initData) >= 8 && string(initData[4:8]) == "ftyp"
		isFLACInFMP4 := isFMP4 && bytes.Contains(initData, []byte("fLaC"))

		if isFLACInFMP4 {
			session, err := remux.StartLosslessCopy(ctx, "mp4", "flac")
			if err != nil {
				return w, nil
			}
			remuxSession = session
			taggerDone = make(chan struct{})

			go func() {
				defer close(taggerDone)
				_ = tagger.TagFLAC(ctx, session.Stdout(), w, req.Metadata, client)
			}()

			return session.Stdin(), nil
		}

		return w, nil
	})

	return err
}
