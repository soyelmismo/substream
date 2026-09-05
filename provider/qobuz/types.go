package qobuz

// QobuzTrack represents a track in Qobuz API responses
type QobuzTrack struct {
	ID                  int64   `json:"id"`
	Title               string  `json:"title"`
	Version             string  `json:"version"`
	Duration            int     `json:"duration"`
	TrackNumber         int     `json:"track_number"`
	MediaNumber         int     `json:"media_number"`
	ISRC                string  `json:"isrc"`
	Copyright           string  `json:"copyright"`
	MaximumBitDepth     int     `json:"maximum_bit_depth"`
	MaximumSamplingRate float64 `json:"maximum_sampling_rate"`
	Hires               bool    `json:"hires"`
	HiresStreamable     bool    `json:"hires_streamable"`
	ReleaseDateOriginal string  `json:"release_date_original"`
	Performer           struct {
		Name string `json:"name"`
		ID   int64  `json:"id"`
	} `json:"performer"`
	Album struct {
		Title string `json:"title"`
		ID    string `json:"id"`
		Image struct {
			Small     string `json:"small"`
			Thumbnail string `json:"thumbnail"`
			Large     string `json:"large"`
		} `json:"image"`
		Artist struct {
			Name string `json:"name"`
			ID   int64  `json:"id"`
		} `json:"artist"`
		Label struct {
			Name string `json:"name"`
		} `json:"label"`
	} `json:"album"`
}

// QobuzAlbum represents an album in Qobuz API responses
type QobuzAlbum struct {
	ID                  string      `json:"id"`
	Title               string      `json:"title"`
	Version             string      `json:"version"`
	Duration            int         `json:"duration"`
	TracksCount         int         `json:"tracks_count"`
	MaximumBitDepth     int         `json:"maximum_bit_depth"`
	MaximumSamplingRate float64     `json:"maximum_sampling_rate"`
	Hires               bool        `json:"hires"`
	HiresStreamable     bool        `json:"hires_streamable"`
	ReleaseDateOriginal string      `json:"release_date_original"`
	Image               struct {
		Small     string `json:"small"`
		Thumbnail string `json:"thumbnail"`
		Large     string `json:"large"`
	} `json:"image"`
	Artist struct {
		Name string `json:"name"`
		ID   int64  `json:"id"`
	} `json:"artist"`
	Label struct {
		Name string `json:"name"`
		ID   int64  `json:"id"`
	} `json:"label"`
	Tracks struct {
		Items []QobuzTrack `json:"items"`
		Total int          `json:"total"`
	} `json:"tracks"`
}

// QobuzArtist represents an artist in Qobuz API responses
type QobuzArtist struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Image  struct {
		Small string `json:"small"`
		Large string `json:"large"`
	} `json:"image"`
	Albums struct {
		Items []QobuzAlbum `json:"items"`
		Total int          `json:"total"`
	} `json:"albums"`
	Tracks struct {
		Items []QobuzTrack `json:"items"`
		Total int          `json:"total"`
	} `json:"tracks"`
}

// QobuzSearchTracksResponse represents track search results
type QobuzSearchTracksResponse struct {
	Tracks struct {
		Total  int          `json:"total"`
		Offset int          `json:"offset"`
		Limit  int          `json:"limit"`
		Items  []QobuzTrack `json:"items"`
	} `json:"tracks"`
}

// QobuzSearchAlbumsResponse represents album search results
type QobuzSearchAlbumsResponse struct {
	Albums struct {
		Total  int          `json:"total"`
		Offset int          `json:"offset"`
		Limit  int          `json:"limit"`
		Items  []QobuzAlbum `json:"items"`
	} `json:"albums"`
}

// QobuzSearchArtistsResponse represents artist search results
type QobuzSearchArtistsResponse struct {
	Artists struct {
		Total  int           `json:"total"`
		Offset int           `json:"offset"`
		Limit  int           `json:"limit"`
		Items  []QobuzArtist `json:"items"`
	} `json:"artists"`
}

// QobuzFileUrlResponse represents the track/getFileUrl response
type QobuzFileUrlResponse struct {
	TrackID      int64   `json:"track_id"`
	URL          string  `json:"url"`
	FormatID     int     `json:"format_id"`
	MimeType     string  `json:"mime_type"`
	Duration     int     `json:"duration"`
	BitDepth     int     `json:"bit_depth"`
	SamplingRate float64 `json:"sampling_rate"`
	Restrictions []struct {
		Code string `json:"code"`
	} `json:"restrictions"`
}

// QobuzUserGetResponse represents the user/get API response
type QobuzUserGetResponse struct {
	ID          interface{} `json:"id"` // can be int64 or float64 or string
	Email       string      `json:"email"`
	DisplayName string      `json:"display_name"`
	Login       string      `json:"login"`
	CountryCode string      `json:"country_code"`
	Credential  struct {
		ID          interface{} `json:"id"`
		Description string      `json:"description"`
		Label       string      `json:"label"`
		Parameters  struct {
			Label                    string `json:"label"`
			ShortLabel               string `json:"short_label"`
			LosslessStreaming        bool   `json:"lossless_streaming"`
			HiresStreaming           bool   `json:"hires_streaming"`
			LossyStreaming           bool   `json:"lossy_streaming"`
			HiresPurchasesStreaming  bool   `json:"hires_purchases_streaming"`
			IncludedFormatGroupIDs   []int  `json:"included_format_group_ids"`
		} `json:"parameters"`
	} `json:"credential"`
	Subscription struct {
		Offer       string `json:"offer"`
		Periodicity string `json:"periodicity"`
		StartDate   string `json:"start_date"`
		EndDate     string `json:"end_date"`
		IsCanceled  bool   `json:"is_canceled"`
	} `json:"subscription"`
}

// QobuzAccountInfo holds validated metadata about a subscriber account
type QobuzAccountInfo struct {
	UserID           string
	Username         string
	SubscriptionType string
	HighestQuality   string
	QualityTier      int
	HiresStreaming   bool
	LosslessStreaming bool
	RawResponse      *QobuzUserGetResponse
}

