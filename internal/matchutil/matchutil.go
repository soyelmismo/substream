// Package matchutil provides fuzzy matching utilities for track identification.
// Extracted from internal/importer/job.go for reuse across the codebase.
package matchutil

import (
	"strings"

	"go.senan.xyz/gonic/tidalproxy"
)

// MatchScore calculates similarity between search query and result (0.0 to 1.0)
func MatchScore(queryArtist, queryTitle string, track tidalproxy.TidalTrack) float64 {
	return MatchScoreWithAlbum(queryArtist, queryTitle, "", track)
}

// MatchScoreWithAlbum includes album name in matching for better precision
func MatchScoreWithAlbum(queryArtist, queryTitle, queryAlbum string, track tidalproxy.TidalTrack) float64 {
	normalize := func(s string) string {
		s = strings.ToLower(s)
		s = strings.ReplaceAll(s, " (feat.", " ")
		s = strings.ReplaceAll(s, " (ft.", " ")
		s = strings.ReplaceAll(s, " (featuring", " ")
		s = strings.ReplaceAll(s, " - ", " ")
		s = strings.ReplaceAll(s, "  ", " ")
		return strings.TrimSpace(s)
	}

	qArtist := normalize(queryArtist)
	qTitle := normalize(queryTitle)
	tArtist := normalize(track.Artist.Name)
	tTitle := normalize(track.Title)

	artistMatch := Similarity(qArtist, tArtist)
	for _, a := range track.Artists {
		if s := Similarity(qArtist, normalize(a.Name)); s > artistMatch {
			artistMatch = s
		}
	}

	titleMatch := Similarity(qTitle, tTitle)

	albumMatch := 0.0
	if queryAlbum != "" {
		albumMatch = Similarity(normalize(queryAlbum), normalize(track.Album.Title))
		if albumMatch > 0.8 {
			titleMatch = maxFloat(titleMatch, 0.9)
		}
	}

	if qTitle == tTitle || strings.EqualFold(qTitle, tTitle) {
		titleMatch = 1.0
	}

	if artistMatch > 0.7 && titleMatch < 0.5 {
		return artistMatch * 0.3
	}

	if titleMatch >= 0.99 {
		if artistMatch >= 0.2 || strings.Contains(qArtist, tArtist) || strings.Contains(tArtist, qArtist) {
			return 0.9 + (artistMatch * 0.1)
		}
		if albumMatch > 0.7 {
			return 0.85
		}
		return 0.75
	}

	score := (artistMatch * 0.25) + (titleMatch * 0.65)
	if queryAlbum != "" {
		score = score*0.9 + albumMatch*0.1
	}
	return score
}

// FindBest evalúa una lista de tracks de Tidal y devuelve el ID del mejor match
// Si ningún track supera el umbral (0.5), devuelve 0
func FindBest(artist, title, album string, tracks []tidalproxy.TidalTrack) int {
	bestMatch := 0
	bestScore := 0.0

	for _, t := range tracks {
		score := MatchScoreWithAlbum(artist, title, album, t)
		if score > bestScore {
			bestScore = score
			bestMatch = t.ID
		}
	}

	if bestScore >= 0.5 {
		return bestMatch
	}
	return 0
}

// Similarity calcula la similitud entre dos strings (0.0 a 1.0)
func Similarity(a, b string) float64 {
	if a == "" || b == "" {
		return 0.0
	}
	if a == b {
		return 1.0
	}
	if strings.EqualFold(a, b) {
		return 0.98
	}

	if strings.Contains(a, b) || strings.Contains(b, a) {
		longer := float64(max(len([]rune(a)), len([]rune(b))))
		shorter := float64(min(len([]rune(a)), len([]rune(b))))
		return 0.75 + (0.25 * shorter / longer)
	}

	aWords := strings.Fields(a)
	bWords := strings.Fields(b)
	if len(aWords) == 0 || len(bWords) == 0 {
		return 0.0
	}

	matches := 0
	for _, aw := range aWords {
		awLower := strings.ToLower(aw)
		for _, bw := range bWords {
			bwLower := strings.ToLower(bw)
			if awLower == bwLower || strings.Contains(awLower, bwLower) || strings.Contains(bwLower, awLower) {
				matches++
				break
			}
		}
	}

	return float64(matches) / float64(max(len(aWords), len(bWords)))
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
