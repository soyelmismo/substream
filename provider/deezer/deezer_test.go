package deezer

import (
	"context"
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
