package ytmusic

// YTTrack represents a track resolved from YouTube Music
type YTTrack struct {
	VideoID   string `json:"videoId"`
	Title     string `json:"title"`
	Artist    string `json:"artist"`
	ArtistID  string `json:"artistId"`
	Album     string `json:"album"`
	AlbumID   string `json:"albumId"`
	Year      int    `json:"year"`
	Duration  int    `json:"duration"` // in seconds
	Thumbnail string `json:"thumbnail"`
}

// YTArtist represents an artist in YouTube Music
type YTArtist struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Thumbnail string `json:"thumbnail"`
}

// YTAlbum represents an album in YouTube Music
type YTAlbum struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Artist    string `json:"artist"`
	Year      int    `json:"year"`
	Thumbnail string `json:"thumbnail"`
}
