package qobuz

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestQobuzSignature(t *testing.T) {
	client := NewClient(ClientOptions{
		AppID:     "712109809",
		AppSecret: "589be88e4538daea11f509d29e4a23b1",
	})

	params := url.Values{
		"query": {"USAT21400808"},
		"limit": {"5"},
	}
	ts := "1788445000"
	sig := client.qobuzRequestSignature("track/search", params, ts)
	if len(sig) != 32 {
		t.Fatalf("expected 32 hex char MD5, got %s (len %d)", sig, len(sig))
	}
}

func TestQobuzProviderBasic(t *testing.T) {
	prov := New(ClientOptions{})
	if prov.ID() != "qb" {
		t.Errorf("expected ID 'qb', got %s", prov.ID())
	}
	if prov.Name() != "Qobuz" {
		t.Errorf("expected Name 'Qobuz', got %s", prov.Name())
	}
}

func TestQobuzSearchLive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live test in short mode")
	}

	prov := New(ClientOptions{})
	res, err := prov.SearchTracks(context.Background(), "USAT21400808", 1, 0)
	if err != nil {
		t.Logf("Live Qobuz search error (network dependent): %v", err)
		return
	}
	if len(res) == 0 {
		t.Logf("No results returned for ISRC USAT21400808")
		return
	}
	track := res[0]
	if track.Title != "Fire Away (with Kid Harpoon)" {
		t.Errorf("unexpected track title: %s", track.Title)
	}
	if track.ID.Provider() != "qb" {
		t.Errorf("expected provider 'qb', got %s", track.ID.Provider())
	}

	streamURL, err := prov.GetStreamURL(context.Background(), track.ID.RawID(), "LOSSLESS", "")
	t.Logf("Qobuz stream resolution for %s (%s): url=%q, err=%v", track.Title, track.ID.RawID(), streamURL, err)
}

func TestLiveQobuzUserGet(t *testing.T) {
	tok := os.Getenv("QOBUZ_TOKEN")
	appID := os.Getenv("SUBSTREAM_QOBUZ_APP_ID")
	appSecret := os.Getenv("SUBSTREAM_QOBUZ_APP_SECRET")
	if tok == "" {
		t.Skip("skipping test: QOBUZ_TOKEN not set")
	}

	client := NewClient(ClientOptions{
		AppID:         appID,
		AppSecret:     appSecret,
		UserAuthToken: tok,
	})

	var res map[string]interface{}
	err := client.doSignedRequest(context.Background(), "user/get", nil, &res)
	if err != nil {
		t.Fatalf("user/get request failed: %v", err)
	}
	t.Logf("user/get response: %+v", res)
}



func TestLiveQobuzGetStreamURL(t *testing.T) {
	tok := os.Getenv("QOBUZ_TOKEN")
	appID := os.Getenv("SUBSTREAM_QOBUZ_APP_ID")
	appSecret := os.Getenv("SUBSTREAM_QOBUZ_APP_SECRET")
	if tok == "" {
		t.Skip("skipping test: QOBUZ_TOKEN not set")
	}

	client := NewClient(ClientOptions{
		AppID:         appID,
		AppSecret:     appSecret,
		UserAuthToken: tok,
	})

	// Search a track to get a valid Qobuz track ID
	res, err := client.SearchTracks(context.Background(), "daft punk one more time", 1, 0)
	if err != nil || len(res) == 0 {
		t.Fatalf("search track failed: %v", err)
	}
	track := res[0]
	t.Logf("Found track: %d (%s by %s, hires=%v)", track.ID, track.Title, track.Performer.Name, track.Hires)

	url, err := client.GetStreamURL(context.Background(), track.ID, "LOSSLESS")
	if err != nil {
		t.Fatalf("GetStreamURL failed: %v", err)
	}
	t.Logf("Successfully acquired raw FLAC stream URL: %s", url)
}

func TestQobuzMatchTrackByMetadataSweetTrip(t *testing.T) {
	prov := New(ClientOptions{})
	ctx := context.Background()

	matched, err := prov.MatchTrackByMetadata(ctx, "Sweet Trip", "Conservation of Two", 180)
	if err != nil {
		t.Fatalf("MatchTrackByMetadata failed: %v", err)
	}
	if matched == nil {
		t.Fatalf("expected matched track, got nil")
	}
	t.Logf("Matched track: Title=%q, Artist=%q, ID=%s", matched.Title, matched.Artist, matched.ID.String())
	if !strings.EqualFold(matched.Title, "Conservation of Two") {
		t.Errorf("expected title 'Conservation of Two', got %q", matched.Title)
	}
}

