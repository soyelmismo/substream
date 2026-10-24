package monochrome

// MonochromeTrack represents a track from tracks.monochrome.st
type MonochromeTrack struct {
	ID          string              `json:"id"`
	TrackID     string              `json:"trackId"`
	Title       string              `json:"title"`
	ArtistIDs   []string            `json:"artistIds"`
	ArtistNames []string            `json:"artistNames"`
	Artists     []MonochromeArtist  `json:"artists"`
	ReleaseID   string              `json:"releaseId"`
	Artwork     string              `json:"artwork"`
	Explicit    bool                `json:"explicit"`
	Playable    bool                `json:"playable"`
	Duration    int                 `json:"duration"` // milliseconds
	ISRC        string              `json:"isrc"`
	RecordingID string              `json:"recordingId"`
	DiscNumber  int                 `json:"discNumber"`
	TrackNumber int                 `json:"trackNumber"`
	AlbumTitle  string              `json:"albumTitle,omitempty"`
	ReleaseDate string              `json:"releaseDate,omitempty"`
}

// GetTrackID returns the reliable track ID
func (t *MonochromeTrack) GetTrackID() string {
	if t.TrackID != "" {
		return t.TrackID
	}
	return t.ID
}

// MonochromeArtist represents an artist reference or summary
type MonochromeArtist struct {
	ID          string `json:"id"`
	ArtistID    string `json:"artistId"`
	Name        string `json:"name"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
	Avatar      string `json:"avatar"`
	Bio         string `json:"bio,omitempty"`
}

// GetArtistID returns the reliable artist ID
func (a *MonochromeArtist) GetArtistID() string {
	if a.ArtistID != "" {
		return a.ArtistID
	}
	return a.ID
}

// MonochromeRelease represents an album or release
type MonochromeRelease struct {
	ID          string              `json:"id"`
	ReleaseID   string              `json:"releaseId"`
	Title       string              `json:"title"`
	ArtistIDs   []string            `json:"artistIds"`
	ArtistNames []string            `json:"artistNames"`
	Artists     []MonochromeArtist  `json:"artists"`
	ReleaseDate string              `json:"releaseDate"`
	ReleaseType string              `json:"releaseType"`
	Label       string              `json:"label"`
	Artwork     string              `json:"artwork"`
	TrackCount  int                 `json:"trackCount"`
	DiscCount   int                 `json:"discCount"`
	Tracks      []MonochromeTrack   `json:"tracks"`
}

// GetReleaseID returns the reliable release ID
func (r *MonochromeRelease) GetReleaseID() string {
	if r.ReleaseID != "" {
		return r.ReleaseID
	}
	return r.ID
}

// MonochromeArtistDetail represents a full artist profile
type MonochromeArtistDetail struct {
	ID           string              `json:"id"`
	ArtistID     string              `json:"artistId"`
	Name         string              `json:"name"`
	DisplayName  string              `json:"displayName"`
	Avatar       string              `json:"avatar"`
	Bio          string              `json:"bio"`
	TopTracks    []MonochromeTrack   `json:"topTracks"`
	Albums       []MonochromeRelease `json:"albums"`
	Singles      []MonochromeRelease `json:"singles"`
	Compilations []MonochromeRelease `json:"compilations"`
}

// SearchAllResponse represents results from /search?q=
type SearchAllResponse struct {
	Tracks    []MonochromeTrack   `json:"tracks"`
	Releases  []MonochromeRelease `json:"releases"`
	Artists   []MonochromeArtist  `json:"artists"`
}

// SearchTracksResponse represents results from /search/tracks
type SearchTracksResponse struct {
	Tracks []MonochromeTrack `json:"tracks"`
}

// SearchReleasesResponse represents results from /search/releases
type SearchReleasesResponse struct {
	Releases []MonochromeRelease `json:"releases"`
}

// SearchArtistsResponse represents results from /search/artists
type SearchArtistsResponse struct {
	Artists []MonochromeArtist `json:"artists"`
}
