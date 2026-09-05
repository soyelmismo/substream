package deezer

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestDeezerProviderBasic(t *testing.T) {
	prov := New(ClientOptions{})
	if prov.ID() != "dz" {
		t.Errorf("expected ID 'dz', got %s", prov.ID())
	}
	if prov.Name() != "Deezer" {
		t.Errorf("expected Name 'Deezer', got %s", prov.Name())
	}
}

func TestDeezerSearchLive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live test in short mode")
	}

	prov := New(ClientOptions{})
	// Test ISRC lookup for Skrillex - Fire Away (USAT21400808)
	track, err := prov.GetTrackByISRC(context.Background(), "USAT21400808")
	if err != nil {
		t.Logf("Live Deezer ISRC lookup error (network dependent): %v", err)
		return
	}
	if track == nil {
		t.Fatalf("expected track, got nil")
	}
	if track.Title != "Fire Away (with Kid Harpoon)" {
		t.Errorf("unexpected track title: %s", track.Title)
	}
	if track.ID.Provider() != "dz" {
		t.Errorf("expected provider 'dz', got %s", track.ID.Provider())
	}
}

func TestDeezerGetStreamURLAndCache(t *testing.T) {
	mockSrv := newMockDeezerServer(t)

	dummyARL := dummyTestARL
	tmpDir := t.TempDir()

	prov := New(ClientOptions{
		ARL:          dummyARL,
		CacheDir:     tmpDir,
		BaseURL:      mockSrv.URL,
		StreamFilter: dummyTestSpec,
	})

	ctx := t.Context()
	filePath, err := prov.GetStreamURL(ctx, "3135556", "LOSSLESS", "")
	if err != nil {
		t.Fatalf("GetStreamURL failed: %v", err)
	}

	if filePath == "" {
		t.Fatalf("empty file path returned")
	}

	// Should be a .flac file
	if !strings.HasSuffix(filePath, ".flac") {
		t.Errorf("expected .flac extension, got %s", filePath)
	}

	// Verify file exists and has valid size
	fi, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat cached file failed: %v", err)
	}
	if fi.Size() < 1000000 {
		t.Errorf("expected file size > 1MB, got %d bytes", fi.Size())
	}

	// Second call should return same path directly from cache
	secondPath, err := prov.GetStreamURL(ctx, "3135556", "LOSSLESS", "")
	if err != nil {
		t.Fatalf("second GetStreamURL failed: %v", err)
	}
	if secondPath != filePath {
		t.Errorf("expected cached path %s, got %s", filePath, secondPath)
	}

	t.Logf("Cached FLAC verified at: %s (size: %d bytes)", filePath, fi.Size())
}

func TestDeezerGetStreamURLMissingStreamFilter(t *testing.T) {
	mockSrv := newMockDeezerServer(t)
	tmpDir := t.TempDir()

	// Clear environment during test
	t.Setenv("SUBSTREAM_DEEZER_STREAM_FILTER", "")
	t.Setenv("DEEZER_STREAM_FILTER", "")
	t.Setenv("SUBSTREAM_DEEZER_CIPHER_SPEC", "")
	t.Setenv("DEEZER_CIPHER_SPEC", "")

	prov := New(ClientOptions{
		ARL:          dummyTestARL,
		CacheDir:     tmpDir,
		BaseURL:      mockSrv.URL,
		StreamFilter: "", // explicit empty spec
	})

	ctx := t.Context()
	_, err := prov.GetStreamURL(ctx, "3135556", "LOSSLESS", "")
	if err == nil {
		t.Fatalf("expected error when stream filter is missing and no mirror configured, got nil")
	}
}

func TestDeezerGetStreamURLMP3(t *testing.T) {
	mockSrv := newMockDeezerServer(t)

	dummyARL := dummyTestARL
	tmpDir := t.TempDir()

	prov := New(ClientOptions{
		ARL:          dummyARL,
		CacheDir:     tmpDir,
		BaseURL:      mockSrv.URL,
		StreamFilter: dummyTestSpec,
	})

	ctx := t.Context()
	filePath, err := prov.GetStreamURL(ctx, "3135556", "HIGH", "")
	if err != nil {
		t.Fatalf("GetStreamURL failed: %v", err)
	}

	if !strings.HasSuffix(filePath, ".mp3") {
		t.Errorf("expected .mp3 extension, got %s", filePath)
	}

	fi, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat cached file failed: %v", err)
	}
	if fi.Size() < 1000000 {
		t.Errorf("expected file size > 1MB, got %d bytes", fi.Size())
	}
}

func TestDeezerMatchTrackByMetadataMocked(t *testing.T) {
	mockSrv := newMockDeezerServer(t)

	prov := New(ClientOptions{
		BaseURL: mockSrv.URL,
	})
	ctx := t.Context()

	matched, err := prov.MatchTrackByMetadata(ctx, "Daft Punk", "One More Time", 320)
	if err != nil {
		t.Fatalf("MatchTrackByMetadata failed: %v", err)
	}
	if matched == nil {
		t.Fatalf("expected matched track, got nil")
	}
	if !strings.Contains(strings.ToLower(matched.Title), "one more time") {
		t.Errorf("expected 'One More Time', got %s", matched.Title)
	}
}

func TestDeezerMatchTrackByMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live test in short mode")
	}

	prov := New(ClientOptions{})
	ctx := t.Context()

	matched, err := prov.MatchTrackByMetadata(ctx, "Daft Punk", "One More Time", 320)
	if err != nil {
		t.Fatalf("MatchTrackByMetadata failed: %v", err)
	}
	if matched == nil {
		t.Fatalf("expected matched track, got nil")
	}
	if !strings.Contains(strings.ToLower(matched.Title), "one more time") {
		t.Errorf("expected 'One More Time', got %s", matched.Title)
	}
	if matched.Duration < 300 || matched.Duration > 340 {
		t.Errorf("expected duration around 320s, got %d", matched.Duration)
	}
}

func TestDeezerMatchTrackByMetadataSnippet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live test in short mode")
	}

	prov := New(ClientOptions{})
	ctx := t.Context()

	// "onderwaterwereld (Z1)" has 30s preview on SoundCloud, but is 73s on Deezer
	matched, err := prov.MatchTrackByMetadata(ctx, "gladde paling", "onderwaterwereld (Z1)", 30)
	if err != nil {
		t.Fatalf("MatchTrackByMetadata snippet failed: %v", err)
	}
	if matched == nil {
		t.Fatalf("expected matched track, got nil")
	}
	t.Logf("Matched track: ID=%s Title=%q Artist=%q Duration=%ds", matched.ID.String(), matched.Title, matched.Artist, matched.Duration)
	if !strings.Contains(strings.ToLower(matched.Title), "onderwaterwereld") {
		t.Errorf("expected 'onderwaterwereld', got %s", matched.Title)
	}
	if matched.Duration < 60 {
		t.Errorf("expected full duration (>60s), got %d", matched.Duration)
	}
}

