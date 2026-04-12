// Package recommendations provides the recommendation engine that coordinates
// multiple providers and merges their results.
package recommendations

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.senan.xyz/gonic/db"
)

// Engine coordinates multiple recommendation providers
type Engine struct {
	providers []Provider
	dbc       *db.DB
}

// NewEngine creates a new recommendation engine
func NewEngine(dbc *db.DB) *Engine {
	return &Engine{
		providers: make([]Provider, 0),
		dbc:       dbc,
	}
}

// Register adds a provider to the engine
func (e *Engine) Register(p Provider) {
	e.providers = append(e.providers, p)
	log.Printf("[REC] Registered provider: %s", p.Name())
}

// HasProviders returns true if any providers are registered
func (e *Engine) HasProviders() bool {
	return len(e.providers) > 0
}

// GetSimilarTracks returns similar tracks from all available providers.
// Results are merged and deduplicated by ISRC (falling back to TidalID).
func (e *Engine) GetSimilarTracks(ctx context.Context, user *db.User, track TrackRef, limit int) ([]Recommendation, error) {
	if len(e.providers) == 0 {
		return nil, nil
	}

	// Collect results from all providers concurrently
	type providerResult struct {
		providerName string
		recs         []Recommendation
		err          error
	}

	var wg sync.WaitGroup
	resultsCh := make(chan providerResult, len(e.providers))

	for _, p := range e.providers {
		if !p.IsAvailable(user) {
			continue
		}

		wg.Add(1)
		go func(provider Provider) {
			defer wg.Done()

			recs, err := provider.GetSimilarTracks(ctx, track)
			resultsCh <- providerResult{
				providerName: provider.Name(),
				recs:         recs,
				err:          err,
			}
		}(p)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect all results
	var allRecs []Recommendation
	for res := range resultsCh {
		if res.err != nil {
			log.Printf("[REC] Provider %s error: %v", res.providerName, res.err)
			continue
		}
		allRecs = append(allRecs, res.recs...)
	}

	// Merge and deduplicate
	merged := e.mergeAndRank(allRecs, limit)
	return merged, nil
}

// GetSimilarArtists returns similar artists from all available providers
func (e *Engine) GetSimilarArtists(ctx context.Context, user *db.User, artist ArtistRef, limit int) ([]ArtistRecommendation, error) {
	if len(e.providers) == 0 {
		return nil, nil
	}

	type providerResult struct {
		providerName string
		recs         []ArtistRecommendation
		err          error
	}

	var wg sync.WaitGroup
	resultsCh := make(chan providerResult, len(e.providers))

	for _, p := range e.providers {
		if !p.IsAvailable(user) {
			continue
		}

		wg.Add(1)
		go func(provider Provider) {
			defer wg.Done()

			recs, err := provider.GetSimilarArtists(ctx, artist)
			resultsCh <- providerResult{
				providerName: provider.Name(),
				recs:         recs,
				err:          err,
			}
		}(p)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var allRecs []ArtistRecommendation
	for res := range resultsCh {
		if res.err != nil {
			log.Printf("[REC] Provider %s error: %v", res.providerName, res.err)
			continue
		}
		allRecs = append(allRecs, res.recs...)
	}

	// Merge and deduplicate by TidalID
	merged := e.mergeAndRankArtists(allRecs, limit)
	return merged, nil
}

// GetPersonalized returns personalized recommendations from all available providers
func (e *Engine) GetPersonalized(ctx context.Context, user *db.User, limit int) ([]Recommendation, error) {
	if len(e.providers) == 0 {
		return nil, nil
	}

	type providerResult struct {
		providerName string
		recs         []Recommendation
		err          error
	}

	var wg sync.WaitGroup
	resultsCh := make(chan providerResult, len(e.providers))

	for _, p := range e.providers {
		if !p.IsAvailable(user) {
			continue
		}

		wg.Add(1)
		go func(provider Provider) {
			defer wg.Done()

			recs, err := provider.GetPersonalized(ctx, user, limit/len(e.providers))
			resultsCh <- providerResult{
				providerName: provider.Name(),
				recs:         recs,
				err:          err,
			}
		}(p)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var allRecs []Recommendation
	for res := range resultsCh {
		if res.err != nil {
			log.Printf("[REC] Provider %s error: %v", res.providerName, res.err)
			continue
		}
		allRecs = append(allRecs, res.recs...)
	}

	merged := e.mergeAndRank(allRecs, limit)
	return merged, nil
}

// mergeAndRank deduplicates and ranks track recommendations
func (e *Engine) mergeAndRank(recs []Recommendation, limit int) []Recommendation {
	if len(recs) == 0 {
		return nil
	}

	// Deduplicate by ISRC (preferred) or TidalID
	seen := make(map[string]bool)
	unique := make([]Recommendation, 0, len(recs))

	for _, r := range recs {
		if r.Track == nil {
			continue
		}

		// Use ISRC as primary dedup key, fallback to TidalID
		key := r.Track.ISRC
		if key == "" {
			key = r.Track.ID
		}
		if key == "" {
			key = fmt.Sprintf("tid:%d", r.Track.TidalID)
		}

		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, r)
	}

	// Sort by score descending (higher score = more relevant)
	for i := 0; i < len(unique)-1; i++ {
		for j := i + 1; j < len(unique); j++ {
			if unique[j].Score > unique[i].Score {
				unique[i], unique[j] = unique[j], unique[i]
			}
		}
	}

	// Limit results
	if len(unique) > limit {
		unique = unique[:limit]
	}

	return unique
}

// mergeAndRankArtists deduplicates and ranks artist recommendations
func (e *Engine) mergeAndRankArtists(recs []ArtistRecommendation, limit int) []ArtistRecommendation {
	if len(recs) == 0 {
		return nil
	}

	// Deduplicate by TidalID
	seen := make(map[int]bool)
	unique := make([]ArtistRecommendation, 0, len(recs))

	for _, r := range recs {
		if r.Artist == nil || r.Artist.TidalID == 0 {
			continue
		}

		if seen[r.Artist.TidalID] {
			continue
		}
		seen[r.Artist.TidalID] = true
		unique = append(unique, r)
	}

	// Sort by score descending
	for i := 0; i < len(unique)-1; i++ {
		for j := i + 1; j < len(unique); j++ {
			if unique[j].Score > unique[i].Score {
				unique[i], unique[j] = unique[j], unique[i]
			}
		}
	}

	if len(unique) > limit {
		unique = unique[:limit]
	}

	return unique
}
