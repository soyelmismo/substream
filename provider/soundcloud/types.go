package soundcloud

// SoundCloudUser represents an artist/user on SoundCloud
type SoundCloudUser struct {
	ID             int64  `json:"id"`
	Username       string `json:"username"`
	AvatarURL      string `json:"avatar_url"`
	PermalinkURL   string `json:"permalink_url"`
	FollowersCount int    `json:"followers_count"`
	TrackCount     int    `json:"track_count"`
	Description    string `json:"description"`
}

// SoundCloudTranscoding represents a stream transcoding option
type SoundCloudTranscoding struct {
	URL      string `json:"url"`
	Preset   string `json:"preset"`
	Duration int    `json:"duration"`
	Format   struct {
		Protocol string `json:"protocol"`  // "progressive" or "hls"
		MimeType string `json:"mime_type"` // e.g. "audio/mpeg", "audio/ogg; codecs=\"opus\""
	} `json:"format"`
	Quality string `json:"quality"`
}

// SoundCloudTrack represents a track on SoundCloud
type SoundCloudTrack struct {
	ID            int64          `json:"id"`
	Title         string         `json:"title"`
	Duration      int            `json:"duration"` // in milliseconds
	ArtworkURL    string         `json:"artwork_url"`
	PermalinkURL  string         `json:"permalink_url"`
	Genre         string         `json:"genre"`
	Description   string         `json:"description"`
	CreatedAt     string         `json:"created_at"`
	ReleaseDate   string         `json:"release_date"`
	PlaybackCount int            `json:"playback_count"`
	LikesCount    int            `json:"likes_count"`
	User          SoundCloudUser `json:"user"`
	Media         struct {
		Transcodings []SoundCloudTranscoding `json:"transcodings"`
	} `json:"media"`
}

// SoundCloudPlaylist represents a playlist or album on SoundCloud
type SoundCloudPlaylist struct {
	ID           int64             `json:"id"`
	Title        string            `json:"title"`
	Duration     int               `json:"duration"` // in milliseconds
	ArtworkURL   string            `json:"artwork_url"`
	PermalinkURL string            `json:"permalink_url"`
	TrackCount   int               `json:"track_count"`
	CreatedAt    string            `json:"created_at"`
	ReleaseDate  string            `json:"release_date"`
	User         SoundCloudUser    `json:"user"`
	Tracks       []SoundCloudTrack `json:"tracks"`
}

// SoundCloudSearchTracksResponse represents a tracks search response
type SoundCloudSearchTracksResponse struct {
	Collection   []*SoundCloudTrack `json:"collection"`
	TotalResults int                `json:"total_results"`
	NextHref     string             `json:"next_href"`
}

// SoundCloudSearchAlbumsResponse represents an albums search response
type SoundCloudSearchAlbumsResponse struct {
	Collection   []*SoundCloudPlaylist `json:"collection"`
	TotalResults int                   `json:"total_results"`
	NextHref     string                `json:"next_href"`
}

// SoundCloudSearchUsersResponse represents a users search response
type SoundCloudSearchUsersResponse struct {
	Collection   []*SoundCloudUser `json:"collection"`
	TotalResults int               `json:"total_results"`
	NextHref     string            `json:"next_href"`
}

// SoundCloudStreamResponse represents the response when resolving a stream transcoding URL
type SoundCloudStreamResponse struct {
	URL string `json:"url"`
}

// SoundCloudResolveHeader identifies the kind of resource returned by /resolve
type SoundCloudResolveHeader struct {
	Kind string `json:"kind"` // "playlist", "track", "user"
}
