package provider

import (
	"context"

	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
)

// SearchResults aggregates search results from a music provider
type SearchResults struct {
	Tracks  []*spec.TrackChild
	Albums  []*spec.Album
	Artists []*spec.Artist
}

// MusicProvider defines the universal interface that any music backend must implement.
// Handlers in ctrlsubsonic interact only with this interface, never with backend-specific structs.
type MusicProvider interface {
	// ID returns the unique URN prefix for this provider (e.g., "td" for Tidal, "qb" for Qobuz).
	ID() string

	// Name returns the display name of the provider (e.g., "Tidal", "Qobuz").
	Name() string

	// --- Catalog & Metadata ---
	GetTrack(ctx context.Context, rawID string) (*spec.TrackChild, error)
	GetTracksBatch(ctx context.Context, rawIDs []string) []*spec.TrackChild
	GetAlbum(ctx context.Context, rawID string) (*spec.Album, error)
	GetArtist(ctx context.Context, rawID string) (*spec.Artist, error)
	GetArtistAlbums(ctx context.Context, rawID string, skipTracks bool) ([]*spec.Album, []*spec.TrackChild, error)

	// --- Search ---
	SearchTracks(ctx context.Context, query string, limit, offset int) ([]*spec.TrackChild, error)
	SearchAlbums(ctx context.Context, query string, limit, offset int) ([]*spec.Album, error)
	SearchArtists(ctx context.Context, query string, limit, offset int) ([]*spec.Artist, error)
	Search(ctx context.Context, query string, limit, offset int) (*SearchResults, error)

	// --- Streaming & Media ---
	GetStreamURL(ctx context.Context, rawID string, quality string, clientIP string) (string, error)
	GetCoverURL(coverID string, size int) string
	GetLyrics(ctx context.Context, rawID string) (*spec.StructuredLyrics, error)

	// --- Discovery & Recommendations ---
	GetTopTracks(ctx context.Context, limit int) ([]*spec.TrackChild, error)
	GetArtistTopTracks(ctx context.Context, artistRawID string, limit int) ([]*spec.TrackChild, error)
	GetSimilarArtists(ctx context.Context, artistRawID string) ([]*spec.Artist, error)
	GetRecommendations(ctx context.Context, trackRawID string) ([]*spec.TrackChild, error)
}
