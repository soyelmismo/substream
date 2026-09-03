package remux

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
)

// Session represents an active stream-copy remuxing process
type Session struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  bytes.Buffer
	closed  bool
}

// Stdin returns the stream where input chunks (e.g. fMP4) should be written
func (s *Session) Stdin() io.WriteCloser {
	return s.stdin
}

// Stdout returns the stream where demuxed audio (e.g. FLAC) can be read
func (s *Session) Stdout() io.Reader {
	return s.stdout
}

// Close closes the stdin pipe to signal EOF to ffmpeg
func (s *Session) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.stdin != nil {
		return s.stdin.Close()
	}
	return nil
}

// Wait waits for the ffmpeg process to finish and returns any error
func (s *Session) Wait() error {
	_ = s.Close()
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	if err := s.cmd.Wait(); err != nil {
		if s.cmd.ProcessState != nil && s.cmd.ProcessState.ExitCode() != 0 {
			return fmt.Errorf("ffmpeg exit code %d (stderr: %s): %w", s.cmd.ProcessState.ExitCode(), s.stderr.String(), err)
		}
		return err
	}
	return nil
}

// StartLosslessCopy starts an ffmpeg process to demux and remux audio without re-encoding (-c:a copy).
// Runs at >8000x real-time with negligible CPU usage.
func StartLosslessCopy(ctx context.Context, inputFormat, outputFormat string) (*Session, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return nil, fmt.Errorf("ffmpeg not available: %w", err)
	}

	args := []string{
		"-loglevel", "error",
	}
	if inputFormat != "" {
		args = append(args, "-f", inputFormat)
	}
	args = append(args,
		"-i", "pipe:0",
		"-c:a", "copy",
	)
	if outputFormat != "" {
		args = append(args, "-f", outputFormat)
	}
	args = append(args, "pipe:1")

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("ffmpeg stdout pipe: %w", err)
	}

	session := &Session{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
	}
	cmd.Stderr = &session.stderr

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("ffmpeg start: %w", err)
	}

	return session, nil
}
