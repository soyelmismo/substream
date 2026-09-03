package provider_test

import (
	"context"
	"testing"

	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

type mockProvider struct {
	id   string
	name string
}

func (m *mockProvider) ID() string   { return m.id }
func (m *mockProvider) Name() string { return m.name }

func (m *mockProvider) GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error) {
	return &spec.TrackChild{
		ID:    &specid.ID{URI: m.id + ":tr:" + rawID},
		Title: "Mock Track " + rawID,
	}, nil
}

func (m *mockProvider) GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild {
	res := make([]*spec.TrackChild, len(rawIDs))
	for i, id := range rawIDs {
		res[i], _ = m.GetTrack(ctx, id)
	}
	return res
}

func (m *mockProvider) GetAlbum(ctx context.Context, rawID string) (*spec.Album, error) {
	return &spec.Album{ID: &specid.ID{URI: m.id + ":al:" + rawID}, Name: "Mock Album " + rawID}, nil
}

func (m *mockProvider) GetArtist(ctx context.Context, rawID string) (*spec.Artist, error) {
	return &spec.Artist{ID: &specid.ID{URI: m.id + ":ar:" + rawID}, Name: "Mock Artist " + rawID}, nil
}

func (m *mockProvider) GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error) {
	return nil, nil, nil
}

func (m *mockProvider) SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error) {
	return []*spec.TrackChild{{Title: "Match " + query}}, nil
}

func (m *mockProvider) SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error) {
	return []*spec.Album{{Name: "Match Album " + query}}, nil
}

func (m *mockProvider) SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error) {
	return []*spec.Artist{{Name: "Match Artist " + query}}, nil
}

func (m *mockProvider) Search(ctx context.Context, query string, limit, offset int) (*provider.SearchResults, error) {
	return &provider.SearchResults{
		Tracks:  []*spec.TrackChild{{Title: "Match " + query}},
		Albums:  []*spec.Album{{Name: "Match Album " + query}},
		Artists: []*spec.Artist{{Name: "Match Artist " + query}},
	}, nil
}

func (m *mockProvider) GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error) {
	return "https://mock.stream/" + rawID, nil
}

func (m *mockProvider) GetCoverURL(coverID string, size int) string {
	return "https://mock.cover/" + coverID
}

func (m *mockProvider) GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error) {
	return &spec.StructuredLyrics{Lang: "en"}, nil
}

func (m *mockProvider) GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error) {
	return nil, nil
}

func (m *mockProvider) GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error) {
	return nil, nil
}

func (m *mockProvider) GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error) {
	return nil, nil
}

func (m *mockProvider) GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error) {
	return nil, nil
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := provider.NewRegistry()
	pTidal := &mockProvider{id: "td", name: "Tidal"}
	pQobuz := &mockProvider{id: "qb", name: "Qobuz"}

	reg.Register(pTidal)
	reg.Register(pQobuz)

	// First registered is default
	if def := reg.Default(); def == nil || def.ID() != "td" {
		t.Fatalf("expected default provider td, got %v", def)
	}

	// Resolve by ID
	p, err := reg.Get("qb")
	if err != nil || p.ID() != "qb" {
		t.Fatalf("expected qb provider, got %v (err: %v)", p, err)
	}

	// Fallback to default when empty
	pEmpty, err := reg.Get("")
	if err != nil || pEmpty.ID() != "td" {
		t.Fatalf("expected fallback to td, got %v (err: %v)", pEmpty, err)
	}
}

func TestRegistry_BatchGetTracksByURI(t *testing.T) {
	reg := provider.NewRegistry()
	reg.Register(&mockProvider{id: "td", name: "Tidal"})
	reg.Register(&mockProvider{id: "qb", name: "Qobuz"})

	uris := []string{"td:tr:100", "qb:tr:200", "td:tr:101"}
	tracks := reg.BatchGetTracksByURI(context.Background(), uris)

	if len(tracks) != 3 {
		t.Fatalf("expected 3 tracks, got %d", len(tracks))
	}
	if tracks[0].ID.String() != "td:tr:100" {
		t.Errorf("track 0 URI mismatch: %s", tracks[0].ID.String())
	}
	if tracks[1].ID.String() != "qb:tr:200" {
		t.Errorf("track 1 URI mismatch: %s", tracks[1].ID.String())
	}
	if tracks[2].ID.String() != "td:tr:101" {
		t.Errorf("track 2 URI mismatch: %s", tracks[2].ID.String())
	}
}

func TestRegistry_SearchAll(t *testing.T) {
	reg := provider.NewRegistry()
	reg.Register(&mockProvider{id: "td", name: "Tidal"})
	reg.Register(&mockProvider{id: "qb", name: "Qobuz"})

	results, err := reg.SearchAll(context.Background(), "test", 10, 0)
	if err != nil {
		t.Fatalf("SearchAll error: %v", err)
	}
	if len(results.Tracks) != 2 {
		t.Errorf("expected 2 tracks from 2 providers, got %d", len(results.Tracks))
	}
}
