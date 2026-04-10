package ctrlsubsonic

// hotResponse is the top-level response structure for genre queries from hot.monochrome.tf.
type hotResponse struct {
	Version       string          `json:"version"`
	GenreID       string          `json:"genre_id"`
	TrendingAlbums []hotPlaylist  `json:"trending_albums,omitempty"`
	NewReleases   []hotAlbum      `json:"new_releases,omitempty"`
	TopTracks     []hotTrack      `json:"top_tracks,omitempty"`
	Sections      []hotSection    `json:"sections,omitempty"`
}

// hotSection represents a categorized section from hot.monochrome.tf.
type hotSection struct {
	Title string       `json:"title"`
	Type  string       `json:"type"` // PLAYLIST_LIST, ALBUM_LIST, etc.
	Items []hotSectionItem `json:"items"`
}

// hotSectionItem is a generic item in a section.
type hotSectionItem struct {
	UUID             string   `json:"uuid,omitempty"`
	ID               int      `json:"id,omitempty"`
	Title            string   `json:"title,omitempty"`
	Cover            string   `json:"image,omitempty"` // Playlists use "image"
	SquareImage      string   `json:"squareImage,omitempty"`
	NumberOfTracks   int      `json:"numberOfTracks,omitempty"`
	NumberOfVideos   int      `json:"numberOfVideos,omitempty"`
	URL              string   `json:"url,omitempty"`
	Type             string   `json:"type,omitempty"` // EDITIAL, etc
}

// hotTrack represents a track from hot.monochrome.tf top_tracks.
type hotTrack struct {
	ID       int    `json:"id"`
	Title    string `json:"title"`
	Duration int    `json:"duration"`
	Album    hotTrackAlbum `json:"album"`
	Artists  []hotArtist   `json:"artists"`
}

// hotTrackAlbum represents album info in a track.
type hotTrackAlbum struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	Cover string `json:"cover"`
}

// hotArtist represents artist info in a track.
type hotArtist struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// hotAlbum represents an album from hot.monochrome.tf new_releases or ALBUM_LIST sections.
type hotAlbum struct {
	ID             int    `json:"id"`
	Title          string `json:"title"`
	Cover          string `json:"cover"`
	NumberOfTracks int    `json:"numberOfTracks"`
	StreamReady    bool   `json:"streamReady"`
	ReleaseDate    string `json:"releaseDate,omitempty"`
}

// hotPlaylist represents a playlist from hot.monochrome.tf trending_albums or PLAYLIST_LIST sections.
type hotPlaylist struct {
	UUID           string `json:"uuid"`
	Title          string `json:"title"`
	Image          string `json:"image"`
	NumberOfTracks int    `json:"numberOfTracks"`
	URL            string `json:"url"`
}
