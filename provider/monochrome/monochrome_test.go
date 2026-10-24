package monochrome

import (
	"context"
	"testing"
)

func TestMonochromeLiveSearchAndStream(t *testing.T) {
	prov := New()

	ctx := context.Background()
	results, err := prov.Search(ctx, "Skrillex", 5, 0)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results.Tracks) == 0 {
		t.Fatalf("Expected tracks, got 0")
	}

	firstTrack := results.Tracks[0]
	t.Logf("Found track: %s by %s (ID: %s)", firstTrack.Title, firstTrack.Artist, firstTrack.ID.String())

	if firstTrack.ID.Provider() != "mc" {
		t.Fatalf("Expected provider 'mc', got %s", firstTrack.ID.Provider())
	}

	streamURL, err := prov.GetStreamURL(ctx, firstTrack.ID.RawID(), "LOSSLESS", "")
	if err != nil {
		t.Fatalf("GetStreamURL failed: %v", err)
	}

	t.Logf("Resolved Stream URL: %s", streamURL)
	if streamURL == "" {
		t.Fatalf("Expected non-empty stream URL")
	}
}
