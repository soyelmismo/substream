package ytmusic

import "testing"

// TestSameTrackMetadata pins the gate that keeps YouTube search noise out of the
// cross-provider fallback chain. Every rejected case below is real output returned by
// YouTube Music for "Home Resonance" / for a bare ISRC query.
func TestSameTrackMetadata(t *testing.T) {
	tests := []struct {
		name   string
		artist string
		title  string
		cand   YTTrack
		want   bool
	}{
		{
			name:   "official album track",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "aBhrEv5Z7uc", Title: "Resonance", Artist: "Home"},
			want:   true,
		},
		{
			name:   "case and punctuation differences",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "8GW6sLrK40k", Title: "HOME - Resonance", Artist: "HOME"},
			want:   true,
		},
		{
			name:   "artist with feat suffix",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "x", Title: "Resonance", Artist: "Home feat. Someone"},
			want:   true,
		},
		{
			name:   "one hour loop by another channel",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "M2USzBFARsI", Title: "HOME  - Resonance [1 Hour]", Artist: "TheMusicLooper"},
			want:   false,
		},
		{
			name:   "slowed reupload",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "IJs81BcpvFY", Title: "HOME | resonance (432 hz) (slowed + reverb)", Artist: "A D R I F T"},
			want:   false,
		},
		{
			name:   "cover version",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "LdVtUo-xngw", Title: "resonance (midwest emo version)", Artist: "jacal"},
			want:   false,
		},
		{
			name:   "live stream returned for an ISRC query",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "UHGMpUgP1Dk", Title: "UPDATE FOR WH  sept 25/26", Artist: "BlackPearl i17i"},
			want:   false,
		},
		{
			name:   "unrelated popular song returned for an ISRC query",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "cxp3k-BJS-M", Title: "Kaseh Elgadara", Artist: "حمو المرشدي"},
			want:   false,
		},
		{
			name:   "generic placeholder artist",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "x", Title: "Resonance", Artist: "Song"},
			want:   false,
		},
		{
			name:   "non-latin artist only matches on exact title",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "x", Title: "Resonance", Artist: "حمو المرشدي"},
			want:   true,
		},
		{
			name:   "non-latin artist with a different title",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "x", Title: "Kaseh Elgadara", Artist: "حمو المرشدي"},
			want:   false,
		},
		{
			name:   "empty title",
			artist: "Home",
			title:  "Resonance",
			cand:   YTTrack{VideoID: "x", Title: "", Artist: "Home"},
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameTrackMetadata(tc.artist, tc.title, &tc.cand); got != tc.want {
				t.Fatalf("sameTrackMetadata(%q, %q, %q by %q) = %v, want %v",
					tc.artist, tc.title, tc.cand.Title, tc.cand.Artist, got, tc.want)
			}
		})
	}
}

func TestNormalizeMatchText(t *testing.T) {
	tests := []struct{ in, want string }{
		{"HOME - Resonance", "homeresonance"},
		{"Home-Resonance", "homeresonance"},
		{"home resonance", "homeresonance"},
		{"A D R I F T", "adrift"},
		{"", ""},
		{"حمو المرشدي", ""},
	}
	for _, tc := range tests {
		if got := normalizeMatchText(tc.in); got != tc.want {
			t.Fatalf("normalizeMatchText(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
