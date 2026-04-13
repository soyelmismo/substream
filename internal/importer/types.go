package importer

// ImportedTrack represents a single track from an external playlist source
type ImportedTrack struct {
	ISRC    string
	Title   string
	Artist  string
	Album   string
	TidalID int // Tidal track ID from API - allows direct lookup without search
}

// ImportedPlaylist represents a playlist fetched from an external source
type ImportedPlaylist struct {
	Title       string
	Description string
	CoverURL    string
	Tracks      []ImportedTrack
}
