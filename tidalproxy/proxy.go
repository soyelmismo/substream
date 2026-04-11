package tidalproxy

import "context"

// TidalProxy abstracts all interaction with hifi-api instances
type TidalProxy interface {
	// Metadata
	GetTrackInfo(ctx context.Context, trackID int) (*TidalTrack, error)
	GetAlbumInfo(ctx context.Context, albumID int) (*TidalAlbum, error)
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
	ClearAll() // Clear all in-memory caches
}
