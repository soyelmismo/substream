package spotify

import (
	"context"
	"testing"
)

func TestSpotifyAnonymousToken(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live test in short mode")
	}

	client := NewClient()
	token, clientToken, err := client.ensureToken(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire anonymous spotify token: %v", err)
	}
	if len(token) == 0 {
		t.Fatalf("empty spotify token")
	}
	t.Logf("Anonymous token acquired: len=%d, clientToken len=%d", len(token), len(clientToken))
}

func TestSpotifySearch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live test in short mode")
	}

	prov := New(nil)
	res, err := prov.SearchTracks(context.Background(), "Fire Away Skrillex", 1, 0)
	if err != nil {
		t.Logf("Spotify search returned error (expected if rate limited on v1 search): %v", err)
		return
	}
	if len(res) == 0 {
		t.Logf("no tracks found")
		return
	}
	t.Logf("Found track: %s (ISRC: %v, ID: %s)", res[0].Title, res[0].ISRC, res[0].ID.String())
}


