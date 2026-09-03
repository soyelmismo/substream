package provider

import (
	"context"
	"fmt"
	"sync"

	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Registry manages registered music providers and routes requests by provider ID/URI.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]MusicProvider
	defaultID string
}

// NewRegistry creates an empty provider registry.
func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]MusicProvider),
	}
}

// Register adds or updates a provider. If no default is set, the first registered provider becomes default.
func (r *Registry) Register(p MusicProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := p.ID()
	r.providers[id] = p
	if r.defaultID == "" {
		r.defaultID = id
	}
}

// Get returns the provider by ID (e.g., "td", "qb"). If id is empty, returns the default provider.
func (r *Registry) Get(id string) (MusicProvider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if id == "" {
		id = r.defaultID
	}

	p, ok := r.providers[id]
	if !ok {
		// Fallback to default provider if available
		if def, defOk := r.providers[r.defaultID]; defOk {
			return def, nil
		}
		return nil, fmt.Errorf("provider %q not found and no default configured", id)
	}
	return p, nil
}

// Default returns the primary/default provider (typically "td").
func (r *Registry) Default() MusicProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.providers[r.defaultID]
}

// SetDefault changes the default provider ID.
func (r *Registry) SetDefault(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.providers[id]; !ok {
		return fmt.Errorf("cannot set unknown provider %q as default", id)
	}
	r.defaultID = id
	return nil
}

// All returns a slice of all registered providers.
func (r *Registry) All() []MusicProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]MusicProvider, 0, len(r.providers))
	for _, p := range r.providers {
		list = append(list, p)
	}
	return list
}

// ResolveID parses a URN string (e.g., "td:tr:12345") and returns the provider and raw ID.
func (r *Registry) ResolveID(uri string) (MusicProvider, string, error) {
	id, err := specid.New(uri)
	if err != nil {
		// If not a URN, fall back to default provider with the raw string
		def := r.Default()
		if def == nil {
			return nil, uri, fmt.Errorf("failed to parse ID %q and no default provider", uri)
		}
		return def, uri, nil
	}

	prov, err := r.Get(id.Provider())
	if err != nil {
		return nil, id.RawID(), err
	}
	return prov, id.RawID(), nil
}

// SearchAll searches across all registered providers concurrently and aggregates results.
func (r *Registry) SearchAll(ctx context.Context, query string, limit, offset int) (*SearchResults, error) {
	r.mu.RLock()
	providers := make([]MusicProvider, 0, len(r.providers))
	for _, p := range r.providers {
		providers = append(providers, p)
	}
	r.mu.RUnlock()

	if len(providers) == 0 {
		return &SearchResults{}, nil
	}

	if len(providers) == 1 {
		return providers[0].Search(ctx, query, limit, offset)
	}

	type searchOutcome struct {
		res *SearchResults
		err error
	}

	ch := make(chan searchOutcome, len(providers))
	for _, p := range providers {
		go func(prov MusicProvider) {
			res, err := prov.Search(ctx, query, limit, offset)
			ch <- searchOutcome{res: res, err: err}
		}(p)
	}

	aggregated := &SearchResults{
		Tracks:  make([]*spec.TrackChild, 0),
		Albums:  make([]*spec.Album, 0),
		Artists: make([]*spec.Artist, 0),
	}

	for i := 0; i < len(providers); i++ {
		outcome := <-ch
		if outcome.err != nil || outcome.res == nil {
			continue
		}
		aggregated.Tracks = append(aggregated.Tracks, outcome.res.Tracks...)
		aggregated.Albums = append(aggregated.Albums, outcome.res.Albums...)
		aggregated.Artists = append(aggregated.Artists, outcome.res.Artists...)
	}

	// Apply overall limit
	if limit > 0 {
		if len(aggregated.Tracks) > limit {
			aggregated.Tracks = aggregated.Tracks[:limit]
		}
		if len(aggregated.Albums) > limit {
			aggregated.Albums = aggregated.Albums[:limit]
		}
		if len(aggregated.Artists) > limit {
			aggregated.Artists = aggregated.Artists[:limit]
		}
	}

	return aggregated, nil
}

// BatchGetTracksByURI fetches tracks for multiple URIs preserving order,
// grouping by provider to optimize network calls.
func (r *Registry) BatchGetTracksByURI(ctx context.Context, uris []string) []*spec.TrackChild {
	if len(uris) == 0 {
		return nil
	}

	type trackRequest struct {
		originalIndex int
		rawID         string
	}

	// Group requests by provider ID
	grouped := make(map[string][]trackRequest)
	for i, uri := range uris {
		id, err := specid.New(uri)
		provID := r.defaultID
		rawID := uri
		if err == nil {
			provID = id.Provider()
			rawID = id.RawID()
		}
		grouped[provID] = append(grouped[provID], trackRequest{
			originalIndex: i,
			rawID:         rawID,
		})
	}

	results := make([]*spec.TrackChild, len(uris))
	var wg sync.WaitGroup

	for provID, reqs := range grouped {
		wg.Add(1)
		go func(pID string, trs []trackRequest) {
			defer wg.Done()
			prov, err := r.Get(pID)
			if err != nil {
				return
			}

			rawIDs := make([]string, len(trs))
			for i, req := range trs {
				rawIDs[i] = req.rawID
			}

			tracks := prov.GetTracksBatch(ctx, rawIDs)
			for i, t := range tracks {
				if t != nil && i < len(trs) {
					results[trs[i].originalIndex] = t
				}
			}
		}(provID, reqs)
	}

	wg.Wait()

	// Filter out nils while preserving order
	clean := make([]*spec.TrackChild, 0, len(results))
	for _, t := range results {
		if t != nil {
			clean = append(clean, t)
		}
	}
	return clean
}
