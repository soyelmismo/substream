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

func TestSoundCloudProvider(t *testing.T) {
	prov := New()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Test GetAlbum with a single track (fallback to single album)
	trackID := "238289066"
	album, err := prov.GetAlbum(ctx, trackID)
	if err != nil {
		t.Fatalf("GetAlbum single fallback failed: %v", err)
	}
	if album == nil || len(album.Tracks) == 0 {
		t.Fatalf("expected album with at least 1 track")
	}
	t.Logf("GetAlbum: Title=%q Artist=%q Tracks=%d", album.Name, album.Artist, len(album.Tracks))

	// 2. Test GetCoverURL on cold cache
	coverURL := prov.GetCoverURL("sc:tr:238289066", 500)
	if coverURL == "" {
		t.Fatalf("expected non-empty cover URL")
	}
	t.Logf("GetCoverURL: %s", coverURL)

	// 3. Test GetRecommendations
	recs, err := prov.GetRecommendations(ctx, trackID)
	if err != nil {
		t.Fatalf("GetRecommendations failed: %v", err)
	}
	if len(recs) == 0 {
		t.Fatalf("expected at least 1 recommendation")
	}
	t.Logf("GetRecommendations: returned %d tracks. First: %s - %s", len(recs), recs[0].Artist, recs[0].Title)

	// 4. Test GetArtistAlbums
	artistID := "161472065" // William Crooks
	albums, topTracks, err := prov.GetArtistAlbums(ctx, artistID, false)
	if err != nil {
		t.Fatalf("GetArtistAlbums failed: %v", err)
	}
	t.Logf("GetArtistAlbums: found %d albums/playlists and %d tracks", len(albums), len(topTracks))
	if len(albums) == 0 && len(topTracks) == 0 {
		t.Fatalf("expected either albums or tracks for artist")
	}
}

func TestSoundCloudArtistAche(t *testing.T) {
	prov := New()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// ache's SoundCloud user ID: 133054467
	acheID := "133054467"
	albums, topTracks, err := prov.GetArtistAlbums(ctx, acheID, false)
	if err != nil {
		t.Fatalf("GetArtistAlbums failed for ache: %v", err)
	}
	if len(albums) == 0 {
		t.Fatalf("expected albums for ache, got 0")
	}
	t.Logf("ache has %d albums/releases and %d top tracks", len(albums), len(topTracks))

	// Verify every album has a non-empty Title and Name
	for i, alb := range albums {
		if alb.Title == "" {
			t.Errorf("album %d (%s) has empty Title", i, alb.ID.String())
		}
		if alb.Name == "" {
			t.Errorf("album %d (%s) has empty Name", i, alb.ID.String())
		}
	}

	// Verify top tracks
	top, err := prov.GetArtistTopTracks(ctx, acheID, 10)
	if err != nil {
		t.Fatalf("GetArtistTopTracks failed: %v", err)
	}
	if len(top) == 0 {
		t.Fatalf("expected at least 1 top track for ache")
	}
	t.Logf("ache first top track: Title=%q, Artist=%q", top[0].Title, top[0].Artist)
}

