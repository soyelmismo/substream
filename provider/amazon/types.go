package amazon

// AmazonTrack represents an Amazon Music track
type AmazonTrack struct {
	ASIN        string   `json:"asin"`
	Title       string   `json:"title"`
	Artist      string   `json:"artist"`
	ArtistASIN  string   `json:"artist_asin"`
	Album       string   `json:"album"`
	AlbumASIN   string   `json:"album_asin"`
	Duration    int      `json:"duration"`
	TrackNumber int      `json:"track_number"`
	ReleaseDate string   `json:"release_date"`
	Codec       string   `json:"codec"`
	BitDepth    int      `json:"bit_depth"`
	SampleRate  int      `json:"sample_rate"`
	StreamURL   string   `json:"stream_url"`
	Key         string   `json:"key"`
	KeySpecs    []string `json:"key_specs"`
	CoverURL    string   `json:"cover_url"`
}

// AmazonAlbum represents an Amazon Music album
type AmazonAlbum struct {
	ASIN        string        `json:"asin"`
	Title       string        `json:"title"`
	Artist      string        `json:"artist"`
	ArtistASIN  string        `json:"artist_asin"`
	CoverURL    string        `json:"cover_url"`
	ReleaseDate string        `json:"release_date"`
	Tracks      []AmazonTrack `json:"tracks"`
}

// AmazonArtist represents an Amazon Music artist
type AmazonArtist struct {
	ASIN   string        `json:"asin"`
	Name   string        `json:"name"`
	Image  string        `json:"image"`
	Albums []AmazonAlbum `json:"albums"`
}
