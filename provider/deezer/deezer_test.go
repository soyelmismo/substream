package deezer

import (
	"context"
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

