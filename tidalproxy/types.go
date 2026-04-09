package tidalproxy

// TidalTrack maps to hifi-api /info/ and /track/ responses
type TidalTrack struct {
	ID           int           `json:"id"`
	Title        string        `json:"title"`
	Duration     int           `json:"duration"`
	TrackNumber  int           `json:"trackNumber"`
	VolumeNumber int           `json:"volumeNumber"`
	Explicit     bool          `json:"explicit"`
	ISRC         string        `json:"isrc"`
	AudioQuality string        `json:"audioQuality"`
	Artist       TidalArtist   `json:"artist"`
	Artists      []TidalArtist `json:"artists"`
	Album        TidalAlbumRef `json:"album"`
}

// TidalAlbumRef is the inline album reference inside a track
type TidalAlbumRef struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	Cover       string `json:"cover"`
	ReleaseDate string `json:"releaseDate"`
}

// TidalAlbum maps to hifi-api /album/ response
type TidalAlbum struct {
	ID             int           `json:"id"`
	Title          string        `json:"title"`
	Duration       int           `json:"duration"`
	NumberOfTracks int           `json:"numberOfTracks"`
	Cover          string        `json:"cover"`
	ReleaseDate    string        `json:"releaseDate"`
	Artists        []TidalArtist `json:"artists"`
	Items          []TidalTrack  `json:"items"` // populated by /album/ endpoint
}

// TidalArtist maps to hifi-api /artist/ response
type TidalArtist struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Picture string `json:"picture"`
}

// TidalArtistPage maps to hifi-api /artist/?f= response
type TidalArtistPage struct {
	Albums struct {
		Items []TidalAlbum `json:"items"`
	} `json:"albums"`
	Tracks []TidalTrack `json:"tracks"`
}

// TidalArtistDetail maps to hifi-api /artist/?id= response
type TidalArtistDetail struct {
	Artist TidalArtist `json:"artist"`
	Cover  *struct {
		Size750 string `json:"750"`
	} `json:"cover"`
}

// TidalStreamInfo maps to hifi-api /track/ playbackinfo response
type TidalStreamInfo struct {
	TrackID           int    `json:"trackId"`
	AudioQuality      string `json:"audioQuality"`
	ManifestMimeType  string `json:"manifestMimeType"`
	Manifest          string `json:"manifest"`
	TrackPresentation string `json:"trackPresentation"` // FULL or PREVIEW
}

// TidalCover maps to hifi-api /cover/ response
type TidalCover struct {
	URL1280 string
	URL640  string
	URL80   string
}

// TidalLyrics maps to hifi-api /lyrics/ response
type TidalLyrics struct {
	TrackID       int    `json:"trackId"`
	Lyrics        string `json:"lyrics"`
	Subtitles     string `json:"subtitles"`
	IsRightToLeft bool   `json:"isRightToLeft"`
}

// TidalRecommendation is a wrapper for the /recommendations/ response
type TidalRecommendation struct {
	Items []TidalTrack `json:"items"`
}

// TidalSimilarArtists wraps /artist/similar/ response
type TidalSimilarArtists struct {
	Artists []TidalArtist `json:"artists"`
}

// TidalSearchResult is a generic container for search responses
type TidalSearchResult struct {
	Items []TidalTrack `json:"items"`
	// For top-hits responses which have nested structure
	Tracks  *TidalSearchItems `json:"tracks,omitempty"`
	Artists *TidalSearchItems `json:"artists,omitempty"`
	Albums  *TidalSearchItems `json:"albums,omitempty"`
}

type TidalSearchItems struct {
	Items []interface{} `json:"items"`
}
