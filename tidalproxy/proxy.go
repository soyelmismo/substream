package tidalproxy

import "context"

// CtxKey type for context keys
type CtxKey int

const (
	// CtxTier allows specifying which mirror tier to use for the request
	// TierLow for streaming, TierMedium for metadata, TierHigh for cache
	CtxTier CtxKey = iota
)

// WithTier returns a context with the specified tier for mirror selection
func WithTier(ctx context.Context, tier LatencyTier) context.Context {
	return context.WithValue(ctx, CtxTier, tier)
}

// GetTierFromContext extracts the tier preference from context, defaults to TierLow
func GetTierFromContext(ctx context.Context) LatencyTier {
	if val := ctx.Value(CtxTier); val != nil {
		if tier, ok := val.(LatencyTier); ok {
			return tier
		}
	}
	return TierLow // Default: streaming priority
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

	// Management
	SetInstances(urls []string)
	GetMirrorManager() *MirrorManager
	ClearAll() // Clear all in-memory caches
}
