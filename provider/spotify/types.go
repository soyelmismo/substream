package spotify

// SpotifyTrack represents a track in Spotify API
type SpotifyTrack struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	DurationMs  int    `json:"duration_ms"`
	TrackNumber int    `json:"track_number"`
	DiscNumber  int    `json:"disc_number"`
	Explicit    bool   `json:"explicit"`
	Popularity  int    `json:"popularity"`
	ExternalIDs struct {
		ISRC string `json:"isrc"`
	} `json:"external_ids"`
	Album   SpotifyAlbumRef    `json:"album"`
	Artists []SpotifyArtistRef `json:"artists"`
}

// SpotifyArtistRef is a reference to an artist inside a track/album
type SpotifyArtistRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	URI  string `json:"uri"`
}

// SpotifyAlbumRef is a reference to an album inside a track
type SpotifyAlbumRef struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	ReleaseDate string             `json:"release_date"`
	Images      []SpotifyImage     `json:"images"`
	Artists     []SpotifyArtistRef `json:"artists"`
}

// SpotifyImage represents an image in Spotify responses
type SpotifyImage struct {
	URL    string `json:"url"`
	Height int    `json:"height"`
	Width  int    `json:"width"`
}

// SpotifyAlbum represents an album in Spotify API
type SpotifyAlbum struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	ReleaseDate string             `json:"release_date"`
	TotalTracks int                `json:"total_tracks"`
	Images      []SpotifyImage     `json:"images"`
	Artists     []SpotifyArtistRef `json:"artists"`
	Tracks      struct {
		Items []SpotifyTrack `json:"items"`
		Total int            `json:"total"`
	} `json:"tracks"`
}

// SpotifyArtist represents an artist in Spotify API
type SpotifyArtist struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Images     []SpotifyImage `json:"images"`
	Popularity int            `json:"popularity"`
	Genres     []string       `json:"genres"`
}

// SpotifySearchResponse represents search results
type SpotifySearchResponse struct {
	Tracks struct {
		Items []SpotifyTrack `json:"items"`
		Total int            `json:"total"`
	} `json:"tracks"`
	Albums struct {
		Items []SpotifyAlbum `json:"items"`
		Total int            `json:"total"`
	} `json:"albums"`
	Artists struct {
		Items []SpotifyArtist `json:"items"`
		Total int             `json:"total"`
	} `json:"artists"`
}

// SpotifyArtistAlbumsResponse represents the paginated albums for an artist
type SpotifyArtistAlbumsResponse struct {
	Items []SpotifyAlbum `json:"items"`
	Total int            `json:"total"`
	Limit int            `json:"limit"`
}

// SpotifyTopTracksResponse represents the top tracks for an artist
type SpotifyTopTracksResponse struct {
	Tracks []SpotifyTrack `json:"tracks"`
}

// SpotifyRelatedArtistsResponse represents related/similar artists
type SpotifyRelatedArtistsResponse struct {
	Artists []SpotifyArtist `json:"artists"`
}

// SpotifyRecommendationsResponse represents tracks returned by recommendations endpoint
type SpotifyRecommendationsResponse struct {
	Tracks []SpotifyTrack `json:"tracks"`
}

// SpotifyLyricsLine represents a single line of lyrics
type SpotifyLyricsLine struct {
	StartTimeMs string `json:"startTimeMs"`
	Words       string `json:"words"`
	EndTimeMs   string `json:"endTimeMs"`
}

// SpotifyLyricsResponse represents color-lyrics from spclient
type SpotifyLyricsResponse struct {
	Lyrics struct {
		SyncType string              `json:"syncType"`
		Lines    []SpotifyLyricsLine `json:"lines"`
		Language string              `json:"language"`
	} `json:"lyrics"`
}
