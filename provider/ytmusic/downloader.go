package ytmusic

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"time"

	"go.senan.xyz/gonic/internal/media/tagger"
	"go.senan.xyz/gonic/provider"
)

// DownloadTrack implements provider.MediaDownloader for YouTube Music.
// It resolves the stream URL via fast persistent worker or yt-dlp CLI fallback,
// converts/remuxes the stream to FLAC, and tags it with complete metadata and cover art.
func (p *Provider) DownloadTrack(ctx context.Context, req provider.DownloadRequest, w io.Writer) error {
	streamURL, err := p.client.GetStreamURL(ctx, req.TrackID)
	if err != nil {
		return fmt.Errorf("ytmusic get stream url: %w", err)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}

	// 1. Fetch audio stream from Google Video CDN
	httpReq, err := http.NewRequestWithContext(ctx, "GET", streamURL, nil)
	if err != nil {
		return fmt.Errorf("create stream request: %w", err)
	}
	httpReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := httpClient.Do(httpReq)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		// Fallback: direct pipe from yt-dlp CLI
		return p.downloadViaYTDLPPipe(ctx, req, w, httpClient)
	}
	defer resp.Body.Close()

	// 2. Remux/Convert audio stream to FLAC with ffmpeg so clients receive standard tagged lossless format
	ffmpegCmd := exec.CommandContext(ctx, "ffmpeg",
		"-loglevel", "error",
		"-i", "pipe:0",
		"-f", "flac",
		"pipe:1",
	)
	ffmpegCmd.Stdin = resp.Body
	ffmpegOut, err := ffmpegCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg stdout pipe: %w", err)
	}

	if err := ffmpegCmd.Start(); err != nil {
		return fmt.Errorf("ffmpeg start: %w", err)
	}

	taggerDone := make(chan error, 1)
	go func() {
		defer close(taggerDone)
		taggerDone <- tagger.TagFLAC(ctx, ffmpegOut, w, req.Metadata, httpClient)
	}()

	tagErr := <-taggerDone
	waitErr := ffmpegCmd.Wait()
	if tagErr != nil {
		return tagErr
	}
	return waitErr
}

func (p *Provider) downloadViaYTDLPPipe(ctx context.Context, req provider.DownloadRequest, w io.Writer, client *http.Client) error {
	targetURL := fmt.Sprintf("https://www.youtube.com/watch?v=%s", req.TrackID)
	ytdlpCmd := exec.CommandContext(ctx, "yt-dlp",
		"--no-warnings",
		"--no-playlist",
		"-f", "ba/b",
		"-o", "-",
		targetURL,
	)

	ytdlpOut, err := ytdlpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("yt-dlp pipe: %w", err)
	}

	ffmpegCmd := exec.CommandContext(ctx, "ffmpeg",
		"-loglevel", "error",
		"-i", "pipe:0",
		"-f", "flac",
		"pipe:1",
	)
	ffmpegCmd.Stdin = ytdlpOut
	ffmpegOut, err := ffmpegCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("ffmpeg pipe: %w", err)
	}

	if err := ytdlpCmd.Start(); err != nil {
		return fmt.Errorf("yt-dlp start: %w", err)
	}
	if err := ffmpegCmd.Start(); err != nil {
		_ = ytdlpCmd.Process.Kill()
		return fmt.Errorf("ffmpeg start: %w", err)
	}

	taggerDone := make(chan error, 1)
	go func() {
		defer close(taggerDone)
		taggerDone <- tagger.TagFLAC(ctx, ffmpegOut, w, req.Metadata, client)
	}()

	tagErr := <-taggerDone
	_ = ytdlpCmd.Wait()
	waitErr := ffmpegCmd.Wait()
	if tagErr != nil {
		return tagErr
	}
	return waitErr
}
