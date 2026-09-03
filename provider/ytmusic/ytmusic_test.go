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


