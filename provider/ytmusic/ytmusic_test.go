package ytmusic

import (
	"context"
	"testing"
	"time"
)

func TestGetAlbum(t *testing.T) {
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test browsing Daft Punk - Discovery
	album, err := p.GetAlbum(ctx, "MPREb_7ltM34kr0mH")
	if err != nil {
		t.Fatalf("GetAlbum failed: %v", err)
	}
	if album == nil {
		t.Fatal("expected album, got nil")
	}
	if album.Title != "Discovery" {
		t.Errorf("expected title Discovery, got %q", album.Title)
	}
	if album.Artist != "Daft Punk" {
		t.Errorf("expected artist Daft Punk, got %q", album.Artist)
	}
	if len(album.Tracks) != 14 {
		t.Errorf("expected 14 tracks, got %d", len(album.Tracks))
	}
	if len(album.Tracks) > 0 {
		first := album.Tracks[0]
		if first.Title != "One More Time" {
			t.Errorf("expected first track One More Time, got %q", first.Title)
		}
		if first.TrackNumber != 1 {
			t.Errorf("expected track number 1, got %d", first.TrackNumber)
		}
		if first.ParentID == nil || first.ParentID.String() != "yt:al:MPREb_7ltM34kr0mH" {
			t.Errorf("expected ParentID yt:al:MPREb_7ltM34kr0mH, got %v", first.ParentID)
		}
	}
}

func TestGetAlbumFromVideoID(t *testing.T) {
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Video ID for Daft Punk - Voyager (should resolve its album Discovery)
	album, err := p.GetAlbum(ctx, "OWiVJMgms9E")
	if err != nil {
		t.Fatalf("GetAlbum with videoID failed: %v", err)
	}
	if album == nil {
		t.Fatal("expected album, got nil")
	}
	if album.Title != "Discovery" {
		t.Errorf("expected title Discovery, got %q", album.Title)
	}
	if len(album.Tracks) != 14 {
		t.Errorf("expected 14 tracks, got %d", len(album.Tracks))
	}
}

func TestGetArtist(t *testing.T) {
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test Daft Punk channel ID
	artist, err := p.GetArtist(ctx, "UCRr1xG_2WIDs18a6cIiCxeA")
	if err != nil {
		t.Fatalf("GetArtist failed: %v", err)
	}
	if artist == nil {
		t.Fatal("expected artist, got nil")
	}
	if artist.Name != "Daft Punk" {
		t.Errorf("expected artist Daft Punk, got %q", artist.Name)
	}
	if artist.AlbumCount == 0 {
		t.Errorf("expected AlbumCount > 0, got %d", artist.AlbumCount)
	}

	// Test GetArtistAlbums
	albums, topTracks, err := p.GetArtistAlbums(ctx, "UCRr1xG_2WIDs18a6cIiCxeA", false)
	if err != nil {
		t.Fatalf("GetArtistAlbums failed: %v", err)
	}
	if len(albums) == 0 {
		t.Errorf("expected albums > 0, got %d", len(albums))
	}
	if len(topTracks) == 0 {
		t.Errorf("expected topTracks > 0, got %d", len(topTracks))
	}

	// Test Similar Artists
	similar, err := p.GetSimilarArtists(ctx, "UCRr1xG_2WIDs18a6cIiCxeA")
	if err != nil {
		t.Fatalf("GetSimilarArtists failed: %v", err)
	}
	if len(similar) == 0 {
		t.Errorf("expected similar artists > 0, got %d", len(similar))
	}
}

func TestGetArtistByName(t *testing.T) {
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test resolving by name "Michael Jackson"
	artist, err := p.GetArtist(ctx, "Michael Jackson")
	if err != nil {
		t.Fatalf("GetArtist with name failed: %v", err)
	}
	if artist == nil {
		t.Fatal("expected artist, got nil")
	}
	if artist.Name != "Michael Jackson" {
		t.Errorf("expected artist Michael Jackson, got %q", artist.Name)
	}
	if artist.AlbumCount == 0 {
		t.Errorf("expected AlbumCount > 0, got %d", artist.AlbumCount)
	}
}

func TestGetRecommendations(t *testing.T) {
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test GetRecommendations for Daft Punk - Voyager (OWiVJMgms9E)
	recs, err := p.GetRecommendations(ctx, "OWiVJMgms9E")
	if err != nil {
		t.Fatalf("GetRecommendations failed: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("expected recommendations > 0, got 0")
	}
	t.Logf("Got %d recommendations for Voyager", len(recs))
	for i, r := range recs[:5] {
		t.Logf("  [%d] %s - %s (%s)", i, r.Artist, r.Title, r.ID.String())
		if r.ID == nil || r.ID.String() == "" {
			t.Errorf("expected valid ID for recommendation %d", i)
		}
		if r.Title == "" {
			t.Errorf("expected valid title for recommendation %d", i)
		}
	}
}

func TestSearchTracks(t *testing.T) {
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tracks, err := p.SearchTracks(ctx, "ache eighteen william crooks", 10, 0)
	if err != nil {
		t.Fatalf("SearchTracks failed: %v", err)
	}
	if len(tracks) == 0 {
		t.Fatal("expected at least 1 track, got 0")
	}

	for i, tr := range tracks {
		t.Logf("  [%d] ID=%s, Title=%q, Artist=%q", i, tr.ID.String(), tr.Title, tr.Artist)
	}

	found := false
	for _, tr := range tracks {
		if tr.ID.String() == "yt:tr:IfUQM_3e7l0" {
			found = true
			if tr.Artist == "Video" || tr.Artist == "" {
				t.Errorf("expected artist not to be 'Video', got %q", tr.Artist)
			}
			if tr.Title == "" {
				t.Errorf("expected non-empty title, got %q", tr.Title)
			}
			break
		}
	}
	if !found {
		t.Errorf("expected track yt:tr:IfUQM_3e7l0 in results, but was not found")
	}
}
