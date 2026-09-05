package deezer

import (
	"bytes"
	"crypto/cipher"
	"io"
	"net/http"
	"testing"

	"go.senan.xyz/gonic/db"
	"golang.org/x/crypto/blowfish"
)

func TestStreamFilterParsingAndDerivation(t *testing.T) {
	// 1. Unconfigured / empty spec returns ErrStreamFilterNotConfigured
	if _, err := ParseStreamFilterSpec(""); err != ErrStreamFilterNotConfigured {
		t.Fatalf("expected ErrStreamFilterNotConfigured, got %v", err)
	}

	var nilProfile *StreamFilterProfile
	if _, err := nilProfile.DeriveKey("3135556"); err != ErrStreamFilterNotConfigured {
		t.Fatalf("expected ErrStreamFilterNotConfigured on nil profile, got %v", err)
	}
	var emptyBuf bytes.Buffer
	if err := nilProfile.DecryptStream(&emptyBuf, bytes.NewReader([]byte{}), "3135556"); err != ErrStreamFilterNotConfigured {
		t.Fatalf("expected ErrStreamFilterNotConfigured on nil DecryptStream, got %v", err)
	}

	// 2. Invalid spec formats
	if _, err := ParseStreamFilterSpec("invalid:format"); err == nil {
		t.Fatalf("expected error for malformed spec, got nil")
	}

	// 3. Valid plaintext spec
	profile, err := ParseStreamFilterSpec(dummyTestSpec)
	if err != nil {
		t.Fatalf("failed to parse valid dummy spec: %v", err)
	}
	if profile.Algo != "bf" || profile.ChunkSize != 2048 || profile.Stride != 3 || profile.Offset != 0 {
		t.Fatalf("unexpected profile parameters: %+v", profile)
	}
	if len(profile.IV) != 8 {
		t.Fatalf("expected 8-byte IV, got %d bytes", len(profile.IV))
	}

	// 4. Derive key
	key, err := profile.DeriveKey("3135556")
	if err != nil {
		t.Fatalf("unexpected error deriving key: %v", err)
	}
	if len(key) != 16 {
		t.Fatalf("expected 16-byte key, got %d bytes", len(key))
	}

	// Calling again should yield identical deterministic key
	key2, err := profile.DeriveKey("3135556")
	if err != nil || !bytes.Equal(key, key2) {
		t.Fatalf("key derivation is non-deterministic")
	}

	// Different track ID should yield different key
	keyDiff, err := profile.DeriveKey("1234567")
	if err != nil || bytes.Equal(key, keyDiff) {
		t.Fatalf("different track IDs produced identical keys")
	}
}

func TestBase64TokenActivation(t *testing.T) {
	// Base64 token encoding: bf:2048:3:0:0001020304050607:md5x:dummy_secret_key_16_bytes_long
	b64Token := "YmY6MjA0ODozOjA6MDAwMTAyMDMwNDA1MDYwNzptZDV4OmR1bW15X3NlY3JldF9rZXlfMTZfYnl0ZXNfbG9uZw=="
	profile, err := ParseStreamFilterSpec(b64Token)
	if err != nil {
		t.Fatalf("failed to parse Base64 stream filter token: %v", err)
	}

	if profile.Algo != "bf" {
		t.Errorf("expected Algo 'bf', got %q", profile.Algo)
	}
	if profile.ChunkSize != 2048 {
		t.Errorf("expected ChunkSize 2048, got %d", profile.ChunkSize)
	}
	if profile.Stride != 3 {
		t.Errorf("expected Stride 3, got %d", profile.Stride)
	}
	if profile.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", profile.Offset)
	}
	expectedIV := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	if !bytes.Equal(profile.IV, expectedIV) {
		t.Errorf("expected IV %x, got %x", expectedIV, profile.IV)
	}
	if profile.KDF != "md5x" {
		t.Errorf("expected KDF 'md5x', got %q", profile.KDF)
	}
	if profile.Key != "dummy_secret_key_16_bytes_long" {
		t.Errorf("expected Key 'dummy_secret_key_16_bytes_long', got %q", profile.Key)
	}
}

func TestDecryptStreamRoundTrip(t *testing.T) {
	profile, err := ParseStreamFilterSpec(dummyTestSpec)
	if err != nil {
		t.Fatalf("failed to parse dummy spec: %v", err)
	}

	trackID := "3135556"
	key, err := profile.DeriveKey(trackID)
	if err != nil {
		t.Fatalf("failed to derive key: %v", err)
	}
	block, err := blowfish.NewCipher(key)
	if err != nil {
		t.Fatalf("failed to create blowfish cipher: %v", err)
	}

	// Create a synthetic stream of 3 chunks matching profile parameters
	origChunk0 := make([]byte, profile.ChunkSize)
	for i := range origChunk0 {
		origChunk0[i] = byte(i % 256)
	}
	origChunk1 := make([]byte, profile.ChunkSize)
	for i := range origChunk1 {
		origChunk1[i] = byte((i + 42) % 256)
	}
	origChunk2 := make([]byte, 500)
	for i := range origChunk2 {
		origChunk2[i] = byte((i + 99) % 256)
	}

	// Encrypt chunk 0 (since 0 % stride == offset)
	encryptedChunk0 := make([]byte, profile.ChunkSize)
	copy(encryptedChunk0, origChunk0)
	encMode := cipher.NewCBCEncrypter(block, profile.IV)
	encMode.CryptBlocks(encryptedChunk0, encryptedChunk0)

	// Combine into raw stream
	var rawStream bytes.Buffer
	rawStream.Write(encryptedChunk0)
	rawStream.Write(origChunk1)
	rawStream.Write(origChunk2)

	// Decrypt using profile.DecryptStream
	var decryptedStream bytes.Buffer
	err = profile.DecryptStream(&decryptedStream, &rawStream, trackID)
	if err != nil {
		t.Fatalf("DecryptStream failed: %v", err)
	}

	decryptedBytes := decryptedStream.Bytes()
	expectedTotal := profile.ChunkSize*2 + 500
	if len(decryptedBytes) != expectedTotal {
		t.Fatalf("expected %d bytes decrypted, got %d", expectedTotal, len(decryptedBytes))
	}

	if !bytes.Equal(decryptedBytes[:profile.ChunkSize], origChunk0) {
		t.Errorf("chunk 0 was not properly decrypted to original data")
	}
	if !bytes.Equal(decryptedBytes[profile.ChunkSize:profile.ChunkSize*2], origChunk1) {
		t.Errorf("chunk 1 (unencrypted) was corrupted during decryption")
	}
	if !bytes.Equal(decryptedBytes[profile.ChunkSize*2:], origChunk2) {
		t.Errorf("chunk 2 (remainder) was corrupted during decryption")
	}
}

func TestDecryptStreamShortStreamRejection(t *testing.T) {
	profile, err := ParseStreamFilterSpec(dummyTestSpec)
	if err != nil {
		t.Fatalf("failed to parse spec: %v", err)
	}

	// 1. Zero bytes stream
	var emptyBuf bytes.Buffer
	err = profile.DecryptStream(&emptyBuf, bytes.NewReader([]byte{}), "3135556")
	if err == nil {
		t.Errorf("expected error for empty stream, got nil")
	}

	// 2. Truncated stream (< ChunkSize)
	var shortBuf bytes.Buffer
	err = profile.DecryptStream(&shortBuf, bytes.NewReader(make([]byte, 500)), "3135556")
	if err == nil {
		t.Errorf("expected error for short stream (<ChunkSize), got nil")
	}
}

func TestLiveDeezerStreamDecryption(t *testing.T) {
	_ = newMockDeezerServer(t)

	dummyARL := dummyTestARL
	ctx := t.Context()

	// 1. Validate ARL
	info, err := ValidateARL(ctx, dummyARL)
	if err != nil {
		t.Fatalf("ValidateARL failed with dummy ARL: %v", err)
	}
	if info.UserID != "6426929883" {
		t.Errorf("expected UserID '6426929883', got %q", info.UserID)
	}
	if !info.WebLossless {
		t.Errorf("expected WebLossless true")
	}
	if info.HighestQuality != "FLAC" {
		t.Errorf("expected HighestQuality 'FLAC', got %q", info.HighestQuality)
	}
	if info.QualityTier != 80 {
		t.Errorf("expected QualityTier 80, got %d", info.QualityTier)
	}

	// 2. Fetch track token and media stream URL for Daft Punk - One More Time (ID: 3135556)
	sess := NewAccountSession(&db.DeezerAccount{
		ID:               1,
		ARL:              dummyARL,
		Username:         info.Username,
		UserID:           info.UserID,
		SubscriptionType: info.SubscriptionType,
		HighestQuality:   info.HighestQuality,
		QualityTier:      info.QualityTier,
		IsActive:         true,
	})

	tokenRes, err := sess.GetTrackToken(ctx, 3135556)
	if err != nil {
		t.Fatalf("GetTrackToken failed: %v", err)
	}
	if tokenRes.Results.TrackToken == "" {
		t.Fatalf("expected non-empty track token")
	}

	cdnURL, format, err := sess.GetMediaURL(ctx, tokenRes.Results.TrackToken, "LOSSLESS")
	if err != nil {
		t.Fatalf("GetMediaURL failed: %v", err)
	}
	if format != "FLAC" {
		t.Errorf("expected format 'FLAC', got %q", format)
	}
	if cdnURL == "" {
		t.Fatalf("empty CDN URL")
	}

	// 3. Download the first 2048 bytes of the encrypted stream
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cdnURL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Range", "bytes=0-2047")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch encrypted chunk from CDN: %v", err)
	}
	defer resp.Body.Close()

	encData := make([]byte, 2048)
	n, err := io.ReadFull(resp.Body, encData)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		t.Fatalf("read encrypted chunk: %v", err)
	}
	if n != 2048 {
		t.Fatalf("expected 2048 bytes from CDN, got %d", n)
	}

	// Before decryption, bytes should NOT start with "fLaC"
	if string(encData[:4]) == "fLaC" {
		t.Fatalf("expected encrypted stream bytes, but found plaintext fLaC header!")
	}

	// 4. Decrypt using DecryptStream with dummyTestSpec
	var decryptedBuf bytes.Buffer
	err = DecryptStream(&decryptedBuf, bytes.NewReader(encData), "3135556", dummyTestSpec)
	if err != nil {
		t.Fatalf("DecryptStream failed: %v", err)
	}

	decBytes := decryptedBuf.Bytes()
	if len(decBytes) != 2048 {
		t.Fatalf("expected 2048 decrypted bytes, got %d", len(decBytes))
	}

	// The first 4 bytes of any valid FLAC file MUST be 'fLaC' (0x66, 0x4c, 0x61, 0x43)
	magicHeader := string(decBytes[:4])
	if magicHeader != "fLaC" {
		t.Fatalf("FLAC magic header mismatch! expected 'fLaC', got %q (bytes: %x)", magicHeader, decBytes[:4])
	}

	t.Logf("SUCCESS! Successfully fetched and decrypted authentic 16-bit FLAC audio stream header: %s (%x)", magicHeader, decBytes[:4])
}

func TestValidateARLValidationErrors(t *testing.T) {
	_ = newMockDeezerServer(t)
	ctx := t.Context()

	// 1. Empty ARL
	if _, err := ValidateARL(ctx, ""); err == nil {
		t.Errorf("expected error for empty ARL, got nil")
	}

	// 2. Whitespace-only ARL
	if _, err := ValidateARL(ctx, "   \t\n  "); err == nil {
		t.Errorf("expected error for whitespace ARL, got nil")
	}

	// 3. Invalid / Expired ARL rejected by Deezer
	if _, err := ValidateARL(ctx, "invalid_or_expired_arl_token_9999"); err == nil {
		t.Errorf("expected error for invalid ARL, got nil")
	}
}
