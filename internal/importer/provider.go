package importer

import (
	"context"
	"strings"
	"sync"
)

// Provider defines the interface for external playlist sources
type Provider interface {
	// Match returns true if the provider can handle the given URL
	Match(url string) bool
	// Fetch retrieves the playlist data from the URL
	Fetch(ctx context.Context, url string) (*ImportedPlaylist, error)
	// Name returns the provider identifier
	Name() string
}

// Registry holds multiple providers and selects the appropriate one
type Registry struct {
	providers []Provider
	mu        sync.RWMutex
}

// NewRegistry creates a new provider registry with default providers
func NewRegistry() *Registry {
	r := &Registry{
		providers: make([]Provider, 0),
	}
	// Register default providers
	r.Register(&SpotifyProvider{})
	r.Register(&DeezerProvider{})
	return r
}

// Register adds a provider to the registry
func (r *Registry) Register(p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers = append(r.providers, p)
}

// Find returns the first provider that matches the URL
func (r *Registry) Find(url string) Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Normalize URL
	url = strings.TrimSpace(url)

	for _, p := range r.providers {
		if p.Match(url) {
			return p
		}
	}
	return nil
}

// IsImportURL checks if a string looks like an importable playlist URL
func (r *Registry) IsImportURL(s string) bool {
	return r.Find(s) != nil
}
