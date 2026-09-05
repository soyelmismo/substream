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

// DeezerAccountInfo holds validated subscriber information from an ARL cookie
type DeezerAccountInfo struct {
	UserID           string
	Username         string
	SubscriptionType string // "PREMIUM", "FREE"
	HighestQuality   string // "FLAC", "MP3_320", "MP3_128"
	QualityTier      int    // 80 (Lossless FLAC), 50 (MP3_320), 10 (MP3_128)
	LicenseToken     string
	CheckForm        string
	WebLossless      bool
	WebHQ            bool
}

// GWUserDataResponse represents the response from deezer.getUserData in gw-light.php
type GWUserDataResponse struct {
	Error   interface{} `json:"error"`
	Results struct {
		User struct {
			UserID   interface{} `json:"USER_ID"`
			BlogName string      `json:"BLOG_NAME"`
			Options  struct {
				LicenseToken   string `json:"license_token"`
				WebLossless    bool   `json:"web_lossless"`
				WebHQ          bool   `json:"web_hq"`
				ExpirationDate string `json:"expiration_date"`
			} `json:"OPTIONS"`
		} `json:"USER"`
		CheckForm string      `json:"checkForm"`
		OfferID   interface{} `json:"OFFER_ID"`
	} `json:"results"`
}

// GWSongDataResponse represents the response from song.getData in gw-light.php
type GWSongDataResponse struct {
	Error   interface{} `json:"error"`
	Results struct {
		SngID           string `json:"SNG_ID"`
		TrackToken      string `json:"TRACK_TOKEN"`
		MD5Origin       string `json:"MD5_ORIGIN"`
		MediaVersion    string `json:"MEDIA_VERSION"`
		FilesizeFLAC    string `json:"FILESIZE_FLAC"`
		FilesizeMP3_320 string `json:"FILESIZE_MP3_320"`
		FilesizeMP3_128 string `json:"FILESIZE_MP3_128"`
	} `json:"results"`
}

// MediaGetURLRequest is the payload sent to https://media.deezer.com/v1/get_url
type MediaGetURLRequest struct {
	LicenseToken string           `json:"license_token"`
	Media        []MediaFormatReq `json:"media"`
	TrackTokens  []string         `json:"track_tokens"`
}

// MediaFormatReq specifies the type and formats requested
type MediaFormatReq struct {
	Type    string       `json:"type"` // "FULL"
	Formats []FormatSpec `json:"formats"`
}

// FormatSpec represents cipher and audio format
type FormatSpec struct {
	Cipher string `json:"cipher"` // stream cipher mode tag
	Format string `json:"format"` // "FLAC", "MP3_320", "MP3_128"
}

// MediaGetURLResponse is the response from https://media.deezer.com/v1/get_url
type MediaGetURLResponse struct {
	Errors []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Data []struct {
		Errors []struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
		Media []struct {
			Sources []struct {
				URL      string `json:"url"`
				Provider string `json:"provider"`
			} `json:"sources"`
			Format string `json:"format"`
		} `json:"media"`
	} `json:"data"`
}

