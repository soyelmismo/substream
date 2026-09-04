package soundcloud

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestSoundCloudClient(t *testing.T) {
	client := NewClient(ClientOptions{Timeout: 10 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Test search tracks
	tracks, err := client.SearchTracks(ctx, "william crooks", 3, 0)
	if err != nil {
		t.Fatalf("SearchTracks failed: %v", err)
	}
	if len(tracks) == 0 {
		t.Fatalf("expected at least 1 track, got 0")
	}
	t.Logf("Found %d tracks. First: ID=%d Title=%s Artist=%s", len(tracks), tracks[0].ID, tracks[0].Title, tracks[0].User.Username)

	// 2. Test GetTrack
	firstID := strconv.FormatInt(tracks[0].ID, 10)
	track, err := client.GetTrack(ctx, firstID)
	if err != nil {
		t.Fatalf("GetTrack failed: %v", err)
	}
	t.Logf("GetTrack: ID=%d Title=%s Duration=%dms", track.ID, track.Title, track.Duration)

	// 3. Test GetStreamURL
	streamURL, err := client.GetStreamURL(ctx, firstID)
	if err != nil {
		t.Fatalf("GetStreamURL failed: %v", err)
	}
	if streamURL == "" {
		t.Fatalf("expected non-empty stream URL")
	}
	t.Logf("Resolved stream URL: %s", streamURL)
}
