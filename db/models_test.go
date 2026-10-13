package db

import "testing"

// TestEffectiveQualityTier pins the clamp that stops lossy providers from being trusted as
// lossless. Real rows written before this fix: yt:tr:UHGMpUgP1Dk stored with tier 80 while
// being a YouTube live stream, which made the mapped-source fast path skip Qobuz/Tidal.
func TestEffectiveQualityTier(t *testing.T) {
	tests := []struct {
		provider   string
		storedTier int
		want       int
	}{
		{"yt", QualityTierLossless, QualityTierStandard}, // the poisoning case
		{"yt", QualityTierHiRes, QualityTierStandard},
		{"sc", QualityTierLossless, QualityTierStandard},
		{"sp", QualityTierLossless, QualityTierHigh},
		{"sp", QualityTierHigh, QualityTierHigh},
		{"td", QualityTierLossless, QualityTierLossless},
		{"td", QualityTierHiRes, QualityTierHiRes},
		{"qb", QualityTierHiRes, QualityTierHiRes},
		{"dz", QualityTierLossless, QualityTierLossless},
		{"yt", QualityTierFree, QualityTierFree},
		{"YT", QualityTierLossless, QualityTierStandard},
		{"", QualityTierLossless, QualityTierStandard},
	}

	for _, tc := range tests {
		if got := EffectiveQualityTier(tc.provider, tc.storedTier); got != tc.want {
			t.Fatalf("EffectiveQualityTier(%q, %d) = %d, want %d", tc.provider, tc.storedTier, got, tc.want)
		}
	}
}
