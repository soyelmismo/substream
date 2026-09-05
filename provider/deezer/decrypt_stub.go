//go:build !deezer_native

package deezer

import (
	"errors"
	"io"
	"os"
	"strings"
)

var (
	// ErrStreamFilterNotConfigured is returned when attempting stream decryption without an active stream filter profile.
	ErrStreamFilterNotConfigured = errors.New("stream filter engine not enabled in this build; configure DEEZER_CUSTOM_URL or compile with -tags deezer_native")
)

// StreamFilterProfile defines generic parameters for stream block filtering/decryption.
type StreamFilterProfile struct {
	Algo      string
	ChunkSize int
	Stride    int
	Offset    int
	IV        []byte
	KDF       string
	Key       string
}

// ResolveStreamFilterSpec resolves the stream filter specification string from an explicit argument or environment.
func ResolveStreamFilterSpec(explicitSpec string) string {
	if s := strings.TrimSpace(explicitSpec); s != "" {
		return s
	}
	if s := strings.TrimSpace(os.Getenv("SUBSTREAM_DEEZER_STREAM_FILTER")); s != "" {
		return s
	}
	if s := strings.TrimSpace(os.Getenv("DEEZER_STREAM_FILTER")); s != "" {
		return s
	}
	if s := strings.TrimSpace(os.Getenv("SUBSTREAM_DEEZER_CIPHER_SPEC")); s != "" {
		return s
	}
	return strings.TrimSpace(os.Getenv("DEEZER_CIPHER_SPEC"))
}

// ParseStreamFilterSpec parses an activation token string into a StreamFilterProfile.
func ParseStreamFilterSpec(raw string) (*StreamFilterProfile, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrStreamFilterNotConfigured
	}
	return &StreamFilterProfile{}, nil
}

// DeriveKey returns ErrStreamFilterNotConfigured in non-native builds.
func (p *StreamFilterProfile) DeriveKey(trackID string) ([]byte, error) {
	return nil, ErrStreamFilterNotConfigured
}

// DecryptStream returns ErrStreamFilterNotConfigured in non-native builds.
func (p *StreamFilterProfile) DecryptStream(dst io.Writer, src io.Reader, trackID string) error {
	return ErrStreamFilterNotConfigured
}

// DecryptStream decrypts an encrypted stream using an explicit spec string or resolved environment spec.
func DecryptStream(dst io.Writer, src io.Reader, trackID string, spec string) error {
	return ErrStreamFilterNotConfigured
}

// DeriveStreamKey derives the decryption key for a track using an explicit spec or environment spec.
func DeriveStreamKey(trackID string, spec string) ([]byte, error) {
	return nil, ErrStreamFilterNotConfigured
}
