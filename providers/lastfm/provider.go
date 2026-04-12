// Package lastfmprovider implements the recommendations.Provider interface for Last.fm
package lastfmprovider

import (
	"context"
	"fmt"
	"log"

	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/internal/cache"
	"go.senan.xyz/gonic/internal/resolver"
	"go.senan.xyz/gonic/lastfm"
	"go.senan.xyz/gonic/recommendations"
	"go.senan.xyz/gonic/tidalproxy"
)

// Provider implements recommendations.Provider for Last.fm
type Provider struct {
	client   *lastfm.Client
	resolver *resolver.Resolver
	proxy    tidalproxy.TidalProxy
}

// New creates a new Last.fm recommendation provider
func New(client *lastfm.Client, proxy tidalproxy.TidalProxy) *Provider {
	return &Provider{
		client:   client,
		resolver: resolver.New(proxy),
		proxy:    proxy,
	}
}

// Name returns the provider identifier
func (p *Provider) Name() string {
	return "lastfm"
}

// IsAvailable returns true if the user has linked their Last.fm account
func (p *Provider) IsAvailable(user *db.User) bool {
	// Some Last.fm endpoints don't require auth (e.g., artist.getSimilar)
	// But user-specific ones do. Check if user has a session.
	return p.client.IsUserAuthenticated(*user)
}

// GetSimilarTracks returns tracks similar to the given track using Last.fm
func (p *Provider) GetSimilarTracks(ctx context.Context, track recommendations.TrackRef) ([]recommendations.Recommendation, error) {
	// Call Last.fm API
	similar, err := p.client.TrackGetSimilarTracks(track.Artist, track.Title)
	if err != nil {
		return nil, fmt.Errorf("last.fm track.getSimilar: %w", err)
	}

	// Convert Last.fm tracks to resolver queries
	queries := make([]resolver.Query, 0, len(similar.Tracks))
	for _, t := range similar.Tracks {
		queries = append(queries, resolver.Query{
			Artist: t.Artist.Name,
			Title:  t.Name,
		})
	}

	// Resolve to Tidal tracks concurrently
	resolved := p.resolver.ResolveBatch(ctx, queries)

	// Build recommendations from resolved tracks
	var recs []recommendations.Recommendation
	for i, t := range similar.Tracks {
		if i >= len(queries) {
			break
		}

		key := queries[i].Key()
		tidalID, ok := resolved[key]
		if !ok {
			continue // Could not resolve this track
		}

		// Get track info from Tidal for complete metadata
		trackInfo, err := p.proxy.GetTrackInfo(ctx, tidalID)
		if err != nil {
			log.Printf("[LASTFM] Warning: Could not get track info for %d: %v", tidalID, err)
			// Use basic info from what we have
			trackInfo = &tidalproxy.TidalTrack{
				ID:     tidalID,
				Title:  t.Name,
				Artist: tidalproxy.TidalArtist{Name: t.Artist.Name},
			}
		}

		// Normalize match score to 0-1 range (Last.fm returns values like 10.95)
		normalizedScore := float64(t.Match) / 100.0
		if normalizedScore > 1.0 {
			normalizedScore = 1.0
		}
		if normalizedScore < 0.0 {
			normalizedScore = 0.0
		}

		recs = append(recs, recommendations.Recommendation{
			Source: p.Name(),
			Score:  normalizedScore,
			Track: &recommendations.TrackRef{
				ID:      fmt.Sprintf("td:tr:%d", tidalID),
				Title:   trackInfo.Title,
				Artist:  trackInfo.Artist.Name,
				ISRC:    trackInfo.ISRC,
				TidalID: tidalID,
			},
			RawData: t,
		})
	}

	return recs, nil
}

// GetSimilarArtists returns artists similar to the given artist
func (p *Provider) GetSimilarArtists(ctx context.Context, artist recommendations.ArtistRef) ([]recommendations.ArtistRecommendation, error) {
	// Call Last.fm API
	similar, err := p.client.ArtistGetSimilar(artist.Name)
	if err != nil {
		return nil, fmt.Errorf("last.fm artist.getSimilar: %w", err)
	}

	var recs []recommendations.ArtistRecommendation
	for _, a := range similar.Artists {
		// Search for this artist in Tidal
		searchCtx := tidalproxy.WithTier(ctx, tidalproxy.TierMedium)
		artists, err := p.proxy.SearchArtists(searchCtx, a.Name, 3, 0)
		if err != nil || len(artists) == 0 {
			continue
		}

		// Use first result (best match from Tidal search)
		tidalArtist := artists[0]

		recs = append(recs, recommendations.ArtistRecommendation{
			Source: p.Name(),
			Score:  0.8, // Last.fm doesn't provide match score for artists
			Artist: &recommendations.ArtistRef{
				ID:      fmt.Sprintf("td:ar:%d", tidalArtist.ID),
				Name:    tidalArtist.Name,
				TidalID: tidalArtist.ID,
			},
			RawData: a,
		})
	}

	return recs, nil
}

// GetPersonalized returns personalized recommendations based on user's Last.fm loved tracks
func (p *Provider) GetPersonalized(ctx context.Context, user *db.User, limit int) ([]recommendations.Recommendation, error) {
	// Fetch user's loved tracks from Last.fm
	loved, err := p.client.UserGetLovedTracks(user.Name)
	if err != nil {
		return nil, fmt.Errorf("last.fm user.getLovedTracks: %w", err)
	}

	// For each loved track, get similar tracks
	var allRecs []recommendations.Recommendation
	trackLimit := 5 // Get similar tracks for up to 5 loved tracks

	for i, lovedTrack := range loved.Tracks {
		if i >= trackLimit {
			break
		}

		// Get similar tracks to this loved track
		similar, err := p.client.TrackGetSimilarTracks(lovedTrack.Artist.Name, lovedTrack.Name)
		if err != nil {
			continue
		}

		// Take top 3 similar tracks for each loved track
		for j, t := range similar.Tracks {
			if j >= 3 {
				break
			}

			// Try to resolve to Tidal
			query := resolver.Query{
				Artist: t.Artist.Name,
				Title:  t.Name,
			}

			tidalID, err := p.resolver.ResolveSingle(ctx, query)
			if err != nil {
				continue
			}

			// Normalize match score
			normalizedScore := float64(t.Match) / 100.0 * 0.9 // Slightly lower score for secondary recs
			if normalizedScore > 1.0 {
				normalizedScore = 1.0
			}

			allRecs = append(allRecs, recommendations.Recommendation{
				Source: p.Name(),
				Score:  normalizedScore,
				Track: &recommendations.TrackRef{
					ID:      fmt.Sprintf("td:tr:%d", tidalID),
					Title:   t.Name,
					Artist:  t.Artist.Name,
					TidalID: tidalID,
				},
			})
		}
	}

	return allRecs, nil
}

// CacheStats returns resolver cache statistics for monitoring
func (p *Provider) CacheStats() cache.Stats {
	return p.resolver.Stats()
}

// Stop gracefully stops the provider's background goroutines
func (p *Provider) Stop() {
	p.resolver.Stop()
}
