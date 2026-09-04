package provider

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.senan.xyz/gonic/server/ctrlsubsonic/spec"
	"go.senan.xyz/gonic/server/ctrlsubsonic/specid"
)

// Registry manages registered music providers and routes requests by provider ID/URI.
type Registry struct {
	mu              sync.RWMutex
	providers       map[string]MusicProvider
	defaultID       string
	metadataEnabled map[string]bool
	audioEnabled    map[string]bool
}

// ProviderStatusInfo represents the current runtime state of a provider
type ProviderStatusInfo struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	MetadataEnabled bool   `json:"metadata_enabled"`
	AudioEnabled    bool   `json:"audio_enabled"`
}

// NewRegistry creates an empty provider registry.
func NewRegistry() *Registry {
	return &Registry{
		providers:       make(map[string]MusicProvider),
		metadataEnabled: make(map[string]bool),
		audioEnabled:    make(map[string]bool),
	}
}

func (r *Registry) IsMetadataEnabled(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v, ok := r.metadataEnabled[id]; ok {
		return v
	}
	return true
}

func (r *Registry) IsAudioEnabled(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v, ok := r.audioEnabled[id]; ok {
		return v
	}
	return true
}

func (r *Registry) SetMetadataEnabled(id string, enabled bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metadataEnabled[id] = enabled
}

func (r *Registry) SetAudioEnabled(id string, enabled bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.audioEnabled[id] = enabled
}

// LoadDBSettings loads provider enabled/disabled settings from persistent DB
func (r *Registry) LoadDBSettings(getter func(key, defaultVal string) string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id := range r.providers {
		metaKey := fmt.Sprintf("provider.%s.metadata", id)
		audioKey := fmt.Sprintf("provider.%s.audio", id)
		metaVal := getter(metaKey, "true")
		audioVal := getter(audioKey, "true")
		r.metadataEnabled[id] = metaVal == "true" || metaVal == "1"
		r.audioEnabled[id] = audioVal == "true" || audioVal == "1"
	}
}

func (r *Registry) ListStatus() []ProviderStatusInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	order := []string{"td", "yt", "sp", "qb", "dz", "am", "sc"}
	seen := make(map[string]bool)
	var res []ProviderStatusInfo

	for _, id := range order {
		if p, ok := r.providers[id]; ok {
			seen[id] = true
			mEnabled := true
			if v, exists := r.metadataEnabled[id]; exists {
				mEnabled = v
			}
			aEnabled := true
			if v, exists := r.audioEnabled[id]; exists {
				aEnabled = v
			}
			res = append(res, ProviderStatusInfo{
				ID:              id,
				Name:            p.Name(),
				MetadataEnabled: mEnabled,
				AudioEnabled:    aEnabled,
			})
		}
	}
	for id, p := range r.providers {
		if !seen[id] {
			mEnabled := true
			if v, exists := r.metadataEnabled[id]; exists {
				mEnabled = v
			}
			aEnabled := true
			if v, exists := r.audioEnabled[id]; exists {
				aEnabled = v
			}
			res = append(res, ProviderStatusInfo{
				ID:              id,
				Name:            p.Name(),
				MetadataEnabled: mEnabled,
				AudioEnabled:    aEnabled,
			})
		}
	}
	return res
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

// ResolveStreamByISRC attempts to find a full stream across all registered providers using ISRC
func (r *Registry) ResolveStreamByISRC(ctx context.Context, isrc string, quality string, clientIP string, excludeProviderID string) (string, MusicProvider, *specid.ID, error) {
	if isrc == "" {
		return "", nil, nil, fmt.Errorf("empty isrc")
	}

	r.mu.RLock()
	providers := make([]MusicProvider, 0, len(r.providers))
	priorityOrder := []string{"qb", "dz", "am", "td", "sp", "yt", "sc"}
	for _, id := range priorityOrder {
		if !r.IsAudioEnabled(id) {
			continue
		}
		if p, ok := r.providers[id]; ok && id != excludeProviderID {
			providers = append(providers, p)
		}
	}
	for id, p := range r.providers {
		if id != excludeProviderID && r.IsAudioEnabled(id) {
			found := false
			for _, pid := range priorityOrder {
				if id == pid {
					found = true
					break
				}
			}
			if !found {
				providers = append(providers, p)
			}
		}
	}
	r.mu.RUnlock()

	for _, p := range providers {
		resolver, ok := p.(ISRCResolver)
		if !ok {
			continue
		}
		t, err := resolver.GetTrackByISRC(ctx, isrc)
		if err != nil || t == nil || t.ID == nil {
			continue
		}
		streamURL, err := p.GetStreamURL(ctx, t.ID.RawID(), quality, clientIP)
		if err == nil && streamURL != "" {
			isPreview := strings.Contains(streamURL, "range=20-30") || strings.Contains(streamURL, "range=0-30") || (t.Duration > 45 && strings.Contains(streamURL, "preview"))
			if !isPreview {
				return streamURL, p, t.ID, nil
			}
		}
	}

	return "", nil, nil, fmt.Errorf("no full stream found for ISRC %s across registered providers", isrc)
}
