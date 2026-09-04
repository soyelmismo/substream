package spotify

import (
	"context"
	"os"
	"strings"
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

func TestSpotifyMetadataMappings(t *testing.T) {
	prov := New(nil)

	// Sample track with multi-artists and ISRC
	st := &SpotifyTrack{
		ID:          "tr123",
		Name:        "Midnight City",
		DurationMs:  243000,
		TrackNumber: 2,
		DiscNumber:  1,
		ExternalIDs: struct {
			ISRC string `json:"isrc"`
		}{
			ISRC: "FR6V81100084",
		},
		Artists: []SpotifyArtistRef{
			{ID: "ar1", Name: "M83"},
			{ID: "ar2", Name: "Anthony Gonzalez"},
		},
		Album: SpotifyAlbumRef{
			ID:          "al123",
			Name:        "Hurry Up, We're Dreaming",
			ReleaseDate: "2011-10-18",
			Images: []SpotifyImage{
				{URL: "https://i.scdn.co/image/ab67616d0000b273sample", Height: 640, Width: 640},
			},
			Artists: []SpotifyArtistRef{
				{ID: "ar1", Name: "M83"},
			},
		},
	}

	tc := prov.trackToSpec(st)
	if tc == nil {
		t.Fatalf("trackToSpec returned nil")
	}
	if tc.Title != "Midnight City" {
		t.Errorf("expected title 'Midnight City', got %q", tc.Title)
	}
	if len(tc.ISRC) != 1 || tc.ISRC[0] != "FR6V81100084" {
		t.Errorf("expected ISRC [FR6V81100084], got %v", tc.ISRC)
	}
	if tc.DisplayArtist != "M83, Anthony Gonzalez" {
		t.Errorf("expected display artist 'M83, Anthony Gonzalez', got %q", tc.DisplayArtist)
	}
	if len(tc.Artists) != 2 {
		t.Errorf("expected 2 artist refs, got %d", len(tc.Artists))
	}
	if tc.Duration != 243 {
		t.Errorf("expected duration 243, got %d", tc.Duration)
	}
	if tc.Year != 2011 {
		t.Errorf("expected year 2011, got %d", tc.Year)
	}

	// Verify cover caching
	coverURL := prov.GetCoverURL("al123", 500)
	if coverURL != "https://i.scdn.co/image/ab67616d0000b273sample" {
		t.Errorf("expected cached cover URL, got %q", coverURL)
	}

	// Sample album with tracks
	sa := &SpotifyAlbum{
		ID:          "al123",
		Name:        "Hurry Up, We're Dreaming",
		ReleaseDate: "2011-10-18",
		TotalTracks: 2,
		Images: []SpotifyImage{
			{URL: "https://i.scdn.co/image/ab67616d0000b273sample", Height: 640, Width: 640},
		},
		Artists: []SpotifyArtistRef{
			{ID: "ar1", Name: "M83"},
		},
		Tracks: struct {
			Items []SpotifyTrack `json:"items"`
			Total int            `json:"total"`
		}{
			Items: []SpotifyTrack{*st, *st},
			Total: 2,
		},
	}

	alb := prov.albumToSpec(sa)
	if alb == nil {
		t.Fatalf("albumToSpec returned nil")
	}
	if len(alb.Tracks) != 2 {
		t.Errorf("expected 2 tracks on album, got %d", len(alb.Tracks))
	}
	if alb.Duration != 486 {
		t.Errorf("expected album duration 486, got %d", alb.Duration)
	}
}

func TestSpotifyCoverURLResolution(t *testing.T) {
	prov := New(nil)

	// Direct HTTP URL
	direct := prov.GetCoverURL("https://images.example.com/cover.jpg", 300)
	if direct != "https://images.example.com/cover.jpg" {
		t.Errorf("expected direct URL, got %q", direct)
	}

	// Spotify hash
	hashCover := prov.GetCoverURL("ab67616d0000b273123456", 300)
	if hashCover != "https://i.scdn.co/image/ab67616d0000b273123456" {
		t.Errorf("expected cdn URL from hash, got %q", hashCover)
	}

	// Cached artist cover
	prov.artistCovers.Store("ar_test", "https://i.scdn.co/image/artist_sample.jpg")
	artCover := prov.GetCoverURL("ar_test", 300)
	if artCover != "https://i.scdn.co/image/artist_sample.jpg" {
		t.Errorf("expected cached artist cover, got %q", artCover)
	}
}

func TestSpotifyLiveMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live test in short mode")
	}

	var opts []Options
	if spDC := strings.TrimSpace(os.Getenv("SPOTIFY_SP_DC")); spDC != "" {
		opts = append(opts, Options{SPDC: spDC})
	}

	prov := New(nil, opts...)
	ctx := context.Background()

	// 1. Artist Albums & Top Tracks (M83: 63MQldasIQO9Q911JqG9vt)
	artistID := "63MQldasIQO9Q911JqG9vt"
	albums, topTracks, err := prov.GetArtistAlbums(ctx, artistID, false)
	if err != nil {
		t.Logf("GetArtistAlbums error (could be rate-limit/token): %v", err)
	} else {
		t.Logf("Fetched %d albums and %d top tracks for M83", len(albums), len(topTracks))
	}

	// 2. Similar Artists
	similars, err := prov.GetSimilarArtists(ctx, artistID)
	if err != nil {
		t.Logf("GetSimilarArtists error: %v", err)
	} else {
		t.Logf("Fetched %d similar artists for M83", len(similars))
	}

	// 3. Lyrics (Ed Sheeran - Shape of You: 7qiZfU4dY1lWllzX7mPBI3)
	lyrics, err := prov.GetLyrics(ctx, "7qiZfU4dY1lWllzX7mPBI3")
	if err != nil {
		t.Logf("GetLyrics error: %v", err)
	} else if lyrics != nil {
		t.Logf("Lyrics fetched: synced=%v, lines=%d, lang=%s", lyrics.Synced, len(lyrics.Lines), lyrics.Lang)
	}
}


