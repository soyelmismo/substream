package tidalproxy

import "context"

// CtxKey type for context keys
type CtxKey int

const (
	CtxPriority CtxKey = iota
)

// TaskPriority defines how aggressively the mirror manager should protect top proxies
type TaskPriority int

const (
	PriorityUrgent     TaskPriority = iota // Streaming (uses absolute best)
	PriorityNormal                         // User-facing metadata (spares the #1 proxy)
	PriorityBackground                     // Hydration (uses slowest healthy proxies first)
)

// WithPriority returns a context with the specified priority
func WithPriority(ctx context.Context, p TaskPriority) context.Context {
	return context.WithValue(ctx, CtxPriority, p)
}

// GetPriorityFromContext extracts the priority preference, defaults to Urgent
func GetPriorityFromContext(ctx context.Context) TaskPriority {
	if val := ctx.Value(CtxPriority); val != nil {
		if p, ok := val.(TaskPriority); ok {
			return p
		}
	}
	return PriorityUrgent
}

// TidalProxy abstracts all interaction with hifi-api instances
type TidalProxy interface {
	// Metadata
	GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error)
	GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error)
	GetAlbumMetadata(ctx context.Context, albumID int) (*TidalAlbum, error)     // Added for partial cache access
	GetAlbumsInfoBatch(ctx context.Context, albumIDs []int) map[int]*TidalAlbum // Batch fetch for efficiency
	GetArtistInfo(ctx context.Context, artistID int) (*TidalArtistDetail, error)
	GetArtistAlbums(ctx context.Context, artistID int, skipTracks bool) (*TidalArtistPage, error)
	GetArtistAlbumCount(ctx context.Context, artistID int) int

	// Search
	SearchTracks(ctx context.Context, query string, limit, offset int) ([]TidalTrack, error)
	SearchArtists(ctx context.Context, query string, limit, offset int) ([]TidalArtist, error)
	SearchAlbums(ctx context.Context, query string, limit, offset int) ([]TidalAlbum, error)

	// Streaming
	GetStreamURL(ctx context.Context, trackID int, quality string, clientIP string) (string, error)

	// Media
	GetCoverURL(coverUUID string, size int) string
	GetCoverByTrackID(ctx context.Context, trackID int) (*TidalCover, error)
	GetCoverUUIDForAlbum(ctx context.Context, albumID int) string

	// Discovery
	GetRecommendations(ctx context.Context, trackID int) ([]TidalTrack, error)
	GetTopTracks(ctx context.Context, limit int) ([]TidalTrack, error)                     // Global
	GetArtistTopTracks(ctx context.Context, artistID int, limit int) ([]TidalTrack, error) // Specific artist
	GetSimilarArtists(ctx context.Context, artistID int) ([]TidalArtist, error)

	// Lyrics
	GetLyrics(ctx context.Context, trackID int) (*TidalLyrics, error)

	// Playlists
	GetPlaylist(ctx context.Context, playlistUUID string) (*TidalPlaylist, error)

	// Management
	SetInstances(urls []string)
	GetMirrorManager() *MirrorManager
	ClearAll()         // Clear all in-memory caches
	Stats() CacheStats // Get cache statistics (empty for non-cached implementations)
}
