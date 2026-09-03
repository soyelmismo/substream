package qobuz

import (
	"context"
	"net/url"
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
}
