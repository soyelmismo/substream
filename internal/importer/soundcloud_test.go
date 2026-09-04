package importer

import (
	"context"
	"testing"
	"time"
)

func TestSoundCloudImporter(t *testing.T) {
	p := NewSoundCloudProvider()

	// 1. Test Match
	if !p.Match("https://soundcloud.com/lofi_chill/sets/lofi-chill") {
		t.Errorf("expected playlist URL to match")
	}
	if !p.Match("https://on.soundcloud.com/xyz123") {
		t.Errorf("expected short URL to match")
	}
	if !p.Match("https://soundcloud.com/williamcrooks21/fire-burning") {
		t.Errorf("expected track URL to match")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 2. Test Fetch playlist
	pl, err := p.Fetch(ctx, "https://soundcloud.com/lofi_chill/sets/lofi-chill")
	if err != nil {
		t.Fatalf("Fetch playlist failed: %v", err)
	}
	if pl == nil || len(pl.Tracks) == 0 {
		t.Fatalf("expected playlist with tracks")
	}
	t.Logf("Imported playlist: Title=%q Tracks=%d CoverURL=%s", pl.Title, len(pl.Tracks), pl.CoverURL)
	t.Logf("First track: Title=%q Artist=%q SourceURI=%s", pl.Tracks[0].Title, pl.Tracks[0].Artist, pl.Tracks[0].SourceURI)

	// 3. Test Fetch single track
	trPl, err := p.Fetch(ctx, "https://soundcloud.com/williamcrooks21/fire-burning")
	if err != nil {
		t.Fatalf("Fetch track failed: %v", err)
	}
	if trPl == nil || len(trPl.Tracks) != 1 {
		t.Fatalf("expected single track playlist")
	}
	t.Logf("Imported single track: Title=%q Artist=%q SourceURI=%s", trPl.Tracks[0].Title, trPl.Tracks[0].Artist, trPl.Tracks[0].SourceURI)
}
