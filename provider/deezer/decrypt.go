package deezer

import (
	"crypto/cipher"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/crypto/blowfish"
)

var (
	// ErrStreamFilterNotConfigured is returned when attempting stream decryption without an active stream filter profile.
	ErrStreamFilterNotConfigured = errors.New("stream filter engine not configured; set DEEZER_STREAM_FILTER in environment")
)

// StreamFilterProfile defines generic parameters for stream block filtering/decryption.
// In the public repository, no provider-specific constants (chunk size, stride, IV, key)
// are hardcoded. All parameters are dynamically parsed from the activation token.
type StreamFilterProfile struct {
	Algo      string // "bf", "blowfish", "aes", etc.
	ChunkSize int    // block size in bytes
	Stride    int    // block stride interval
	Offset    int    // block stride offset
	IV        []byte // initialization vector
	KDF       string // key derivation function: "md5x", "raw", etc.
	Key       string // cipher secret key
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
// The token can be in raw colon-separated format (algo:chunk:stride:offset:iv:kdf:key)
// or encoded as a Base64 string.
func ParseStreamFilterSpec(raw string) (*StreamFilterProfile, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, ErrStreamFilterNotConfigured
	}

	candidate := raw
	// Try Base64 decoding if the string doesn't contain colon separators or decodes cleanly to spec format
	if !strings.Contains(raw, ":") {
		if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil && strings.Contains(string(decoded), ":") {
			candidate = strings.TrimSpace(string(decoded))
		} else if decodedURL, err := base64.URLEncoding.DecodeString(raw); err == nil && strings.Contains(string(decodedURL), ":") {
			candidate = strings.TrimSpace(string(decodedURL))
		}
	} else if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil && strings.Count(string(decoded), ":") >= 6 {
		candidate = strings.TrimSpace(string(decoded))
	}

	parts := strings.SplitN(candidate, ":", 7)
	if len(parts) < 7 {
		return nil, fmt.Errorf("invalid stream filter spec: expected 7 colon-separated tokens (algo:chunk:stride:offset:iv:kdf:key), got %d", len(parts))
	}

	algo := strings.ToLower(strings.TrimSpace(parts[0]))
	chunkSize, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || chunkSize <= 0 {
		return nil, fmt.Errorf("invalid stream filter chunkSize: %w", err)
	}

	stride, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil || stride <= 0 {
		return nil, fmt.Errorf("invalid stream filter stride: %w", err)
	}

	offset, err := strconv.Atoi(strings.TrimSpace(parts[3]))
	if err != nil || offset < 0 {
		return nil, fmt.Errorf("invalid stream filter offset: %w", err)
	}

	ivHex := strings.TrimSpace(parts[4])
	iv, err := hex.DecodeString(ivHex)
	if err != nil {
		return nil, fmt.Errorf("invalid stream filter IV hex: %w", err)
	}

	kdf := strings.ToLower(strings.TrimSpace(parts[5]))
	key := parts[6]
	if key == "" {
		return nil, errors.New("invalid stream filter: key cannot be empty")
	}

	return &StreamFilterProfile{
		Algo:      algo,
		ChunkSize: chunkSize,
		Stride:    stride,
		Offset:    offset,
		IV:        iv,
		KDF:       kdf,
		Key:       key,
	}, nil
}

// DeriveKey derives the symmetric cipher key for a given track identifier using the profile's KDF.
func (p *StreamFilterProfile) DeriveKey(trackID string) ([]byte, error) {
	if p == nil || p.ChunkSize == 0 || p.Stride == 0 {
		return nil, ErrStreamFilterNotConfigured
	}

	switch p.KDF {
	case "md5x":
		if len(p.Key) < 16 {
			return nil, fmt.Errorf("invalid key length for md5x (%d): must be at least 16 characters", len(p.Key))
		}
		h := md5.Sum([]byte(trackID))
		hexHash := hex.EncodeToString(h[:])
		keyLen := len(p.Key)
		if keyLen > 16 {
			keyLen = 16
		}
		derived := make([]byte, keyLen)
		for i := 0; i < keyLen; i++ {
			derived[i] = hexHash[i] ^ hexHash[i+16] ^ p.Key[i]
		}
		return derived, nil
	case "raw":
		return []byte(p.Key), nil
	default:
		return nil, fmt.Errorf("unsupported stream filter KDF: %s", p.KDF)
	}
}

// DecryptStream decrypts an encrypted stream according to the profile's parameters.
// If no profile is configured or block dimensions are zero, it returns ErrStreamFilterNotConfigured.
func (p *StreamFilterProfile) DecryptStream(dst io.Writer, src io.Reader, trackID string) error {
	if p == nil || p.ChunkSize == 0 || p.Stride == 0 {
		return ErrStreamFilterNotConfigured
	}

	key, err := p.DeriveKey(trackID)
	if err != nil {
		return err
	}

	var block cipher.Block
	switch p.Algo {
	case "bf", "blowfish", "blowfish-cbc":
		b, cErr := blowfish.NewCipher(key)
		if cErr != nil {
			return fmt.Errorf("init blowfish cipher: %w", cErr)
		}
		block = b
	default:
		return fmt.Errorf("unsupported stream filter algorithm: %s", p.Algo)
	}

	buf := make([]byte, p.ChunkSize)
	chunkIdx := 0
	var totalBytes int64

	for {
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			chunk := buf[:n]
			// Only full chunkSize chunks where (chunkIdx % stride == offset) are filtered/decrypted
			if n == p.ChunkSize && (chunkIdx%p.Stride == p.Offset) {
				mode := cipher.NewCBCDecrypter(block, p.IV)
				mode.CryptBlocks(chunk, chunk)
			}
			if _, wErr := dst.Write(chunk); wErr != nil {
				return fmt.Errorf("write filtered chunk: %w", wErr)
			}
			totalBytes += int64(n)
			chunkIdx++
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("read stream chunk: %w", err)
		}
	}

	if totalBytes < int64(p.ChunkSize) {
		return fmt.Errorf("stream too short: %d bytes (minimum required: %d)", totalBytes, p.ChunkSize)
	}
	return nil
}

// DecryptStream decrypts an encrypted stream using an explicit spec string or resolved environment spec.
func DecryptStream(dst io.Writer, src io.Reader, trackID string, spec string) error {
	resolved := ResolveStreamFilterSpec(spec)
	profile, err := ParseStreamFilterSpec(resolved)
	if err != nil {
		return err
	}
	return profile.DecryptStream(dst, src, trackID)
}

// DeriveStreamKey derives the decryption key for a track using an explicit spec or environment spec.
func DeriveStreamKey(trackID string, spec string) ([]byte, error) {
	resolved := ResolveStreamFilterSpec(spec)
	profile, err := ParseStreamFilterSpec(resolved)
	if err != nil {
		return nil, err
	}
	return profile.DeriveKey(trackID)
}
