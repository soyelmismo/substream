package deezer

// DeezerTrack represents a track in Deezer API
type DeezerTrack struct {
	ID            int64            `json:"id"`
	Readable      bool             `json:"readable"`
	Title         string           `json:"title"`
	TitleShort    string           `json:"title_short"`
	ISRC          string           `json:"isrc"`
	Link          string           `json:"link"`
	Duration      int              `json:"duration"`
	TrackPosition int              `json:"track_position"`
	DiskNumber    int              `json:"disk_number"`
	Rank          int              `json:"rank"`
	ReleaseDate   string           `json:"release_date"`
	Explicit      bool             `json:"explicit_lyrics"`
	Preview       string           `json:"preview"`
	BPM           float64          `json:"bpm"`
	Gain          float64          `json:"gain"`
	Artist        DeezerArtistRef  `json:"artist"`
	Album         DeezerAlbumRef   `json:"album"`
	Contributors  []DeezerArtistRef `json:"contributors"`
}

// DeezerArtistRef is an artist reference inside a track or album
type DeezerArtistRef struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	PictureMedium string `json:"picture_medium"`
	PictureBig    string `json:"picture_big"`
	PictureXL     string `json:"picture_xl"`
}

// DeezerAlbumRef is an album reference inside a track
type DeezerAlbumRef struct {
	ID          int64  `json:"id"`
	Title       string `json:"title"`
	CoverMedium string `json:"cover_medium"`
	CoverBig    string `json:"cover_big"`
	CoverXL     string `json:"cover_xl"`
	ReleaseDate string `json:"release_date"`
}

// DeezerAlbum represents an album in Deezer API
type DeezerAlbum struct {
	ID          int64           `json:"id"`
	Title       string          `json:"title"`
	CoverMedium string          `json:"cover_medium"`
	CoverBig    string          `json:"cover_big"`
	CoverXL     string          `json:"cover_xl"`
	Label       string          `json:"label"`
	Duration    int             `json:"duration"`
	NbTracks    int             `json:"nb_tracks"`
	ReleaseDate string          `json:"release_date"`
	Artist      DeezerArtistRef `json:"artist"`
	Tracks      struct {
		Data []DeezerTrack `json:"data"`
	} `json:"tracks"`
}

// DeezerArtist represents an artist in Deezer API
type DeezerArtist struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	PictureMedium string `json:"picture_medium"`
	PictureBig    string `json:"picture_big"`
	PictureXL     string `json:"picture_xl"`
	NbAlbum       int    `json:"nb_album"`
	NbFan         int    `json:"nb_fan"`
}

// DeezerSearchTracksResponse represents track search results
type DeezerSearchTracksResponse struct {
	Data  []DeezerTrack `json:"data"`
	Total int           `json:"total"`
	Next  string        `json:"next"`
}

// DeezerSearchAlbumsResponse represents album search results
type DeezerSearchAlbumsResponse struct {
	Data  []DeezerAlbum `json:"data"`
	Total int           `json:"total"`
	Next  string        `json:"next"`
}

// DeezerSearchArtistsResponse represents artist search results
type DeezerSearchArtistsResponse struct {
	Data  []DeezerArtist `json:"data"`
	Total int            `json:"total"`
	Next  string         `json:"next"`
}
