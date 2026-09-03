package importer

// ImportedTrack represents a single track from an external playlist source
type ImportedTrack struct {
	ISRC    string
	Title   string
	Artist  string
	Album     string
	Duration  int    // Track duration in seconds for fuzzy tolerance matching
	TidalID   int    // Tidal track ID from API - allows direct lookup without search
	VideoID   string // YouTube Music video ID if imported from YouTube
	SourceURI string // Optional explicit source URI (e.g. yt:tr:...)
}

// ImportedPlaylist represents a playlist fetched from an external source
type ImportedPlaylist struct {
	Title       string
	Description string
	CoverURL    string // Cover image URL from source (Tidal, Spotify, etc.)
	CoverData   []byte // Raw cover image data (for processing/compression)
	Tracks      []ImportedTrack
}
