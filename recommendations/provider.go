// Package recommendations provides a pluggable architecture for music recommendation sources.
// Each external provider (Last.fm, Spotify, etc.) implements the Provider interface.
package recommendations

import (
	"context"

	"go.senan.xyz/gonic/db"
)

// TrackRef represents a track reference for recommendations
type TrackRef struct {
	ID       string // URN format: td:tr:12345
	Title    string
	Artist   string
	Album    string
	ISRC     string // For cross-provider matching
	TidalID  int    // Native ID
}

// ArtistRef represents an artist reference for recommendations
type ArtistRef struct {
	ID      string // URN format: td:ar:12345
	Name    string
	TidalID int
}

// Recommendation is a single recommended track
type Recommendation struct {
	Source   string    // Provider name: "lastfm", "spotify", etc.
	Score    float64   // 0.0 - 1.0 (relevance/confidence)
	Track    *TrackRef // Resolved track reference
	RawData  any       // Provider-specific data
}

// ArtistRecommendation is a recommended similar artist
type ArtistRecommendation struct {
	Source string     // Provider name
	Score  float64    // 0.0 - 1.0
	Artist *ArtistRef // Resolved artist reference
	RawData any
}

// Provider is the interface implemented by all recommendation sources
type Provider interface {
	// Name returns the provider identifier (e.g., "lastfm", "spotify")
	Name() string

	// IsAvailable returns true if this provider can serve requests for the user
	// (e.g., user has linked their Last.fm account)
	IsAvailable(user *db.User) bool

	// GetSimilarTracks returns tracks similar to the given track
	GetSimilarTracks(ctx context.Context, track TrackRef) ([]Recommendation, error)

	// GetSimilarArtists returns artists similar to the given artist
	GetSimilarArtists(ctx context.Context, artist ArtistRef) ([]ArtistRecommendation, error)

	// GetPersonalized returns personalized recommendations for the user
	// (e.g., based on their Last.fm loved tracks or listening history)
	GetPersonalized(ctx context.Context, user *db.User, limit int) ([]Recommendation, error)
}
