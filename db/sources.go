package db

import (
	"fmt"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
)

func normalizeString(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, " (feat.", " ")
	s = strings.ReplaceAll(s, " (ft.", " ")
	s = strings.ReplaceAll(s, " (featuring", " ")
	s = strings.ReplaceAll(s, " - ", " ")
	s = strings.ReplaceAll(s, "  ", " ")
	return strings.TrimSpace(s)
}

// MakeClusterKey creates a normalized cluster identity.
// If ISRC is provided, it has global priority. Otherwise falls back to normalized artist:title.
func MakeClusterKey(isrc, artist, title string) string {
	cleanISRC := strings.ToUpper(strings.TrimSpace(isrc))
	if cleanISRC != "" {
		return fmt.Sprintf("isrc:%s", cleanISRC)
	}

	normArtist := normalizeString(artist)
	normTitle := normalizeString(title)
	return fmt.Sprintf("text:%s:%s", normArtist, normTitle)
}

// SaveTrackSources stores or updates multiple provider sources linked under the same clusterKey.
func SaveTrackSources(db *gorm.DB, clusterKey string, sources ...TrackSource) error {
	if db == nil || clusterKey == "" || len(sources) == 0 {
		return nil
	}

	now := time.Now()
	for _, src := range sources {
		if src.URI == "" || src.Provider == "" {
			continue
		}

		// Use upsert query for SQLite
		err := db.Exec(`
			INSERT INTO track_sources (cluster_key, provider, uri, quality_tier, is_available, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(uri) DO UPDATE SET
				cluster_key = excluded.cluster_key,
				provider = excluded.provider,
				quality_tier = excluded.quality_tier,
				is_available = excluded.is_available,
				updated_at = excluded.updated_at
		`, clusterKey, src.Provider, src.URI, src.QualityTier, src.IsAvailable, now).Error

		if err != nil {
			return fmt.Errorf("upsert track source %s: %w", src.URI, err)
		}
	}
	return nil
}

// GetAlternativeSources finds all provider sources belonging to the same cluster as the given URI,
// ordered by quality_tier DESC (highest quality first).
func GetAlternativeSources(db *gorm.DB, uri string) ([]TrackSource, error) {
	if db == nil || uri == "" {
		return nil, nil
	}

	// 1. Find the cluster_key for the given URI
	var origin TrackSource
	if err := db.Where("uri = ?", uri).First(&origin).Error; err != nil {
		if gorm.IsRecordNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}

	// 2. Fetch all sources with this cluster_key, ordered by quality
	var siblings []TrackSource
	err := db.Where("cluster_key = ? AND is_available = ?", origin.ClusterKey, true).
		Order("quality_tier DESC").
		Find(&siblings).Error
	if err != nil {
		return nil, err
	}

	return siblings, nil
}

// GetBestStreamableSource returns the highest-quality available source for a URI
// whose provider is currently enabled for audio playback by the administrator.
func GetBestStreamableSource(db *gorm.DB, uri string, isProviderAudioEnabled func(string) bool) (*TrackSource, error) {
	sources, err := GetAlternativeSources(db, uri)
	if err != nil || len(sources) == 0 {
		return nil, err
	}

	for _, s := range sources {
		if isProviderAudioEnabled != nil && !isProviderAudioEnabled(s.Provider) {
			continue
		}
		if s.IsAvailable {
			return &s, nil
		}
	}
	return nil, nil
}
