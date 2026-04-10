package ctrlsubsonic

// hotItem represents a base item (track or album) from hot.monochrome.tf API.
// Used across genre browsing and discovery features with optional fields
// depending on whether the item represents a track or album.
type hotItem struct {
	ID       int    `json:"id"`                // Tidal ID (track or album)
	Title    string `json:"title"`             // Item title
	Artist   string `json:"artist,omitempty"`  // Artist name (optional for albums)
	Album    string `json:"album,omitempty"`    // Album title (optional for tracks)
	Cover    string `json:"cover"`             // Cover art URL (absolute)
	Duration int    `json:"duration,omitempty"` // Duration in seconds (optional for albums)
}

// hotSection represents a categorized section from hot.monochrome.tf.
// Sections organize items by type (e.g., "ALBUM_LIST", "TRACK_LIST", "TRENDING").
type hotSection struct {
	Title string    `json:"title"` // Section display name shown to users
	Type  string    `json:"type"`  // Section type identifier for API filtering
	Items []hotItem `json:"items"` // Items contained in this section
}

// hotResponse is the top-level response structure for genre queries.
// Contains multiple sections of items organized by category or type.
type hotResponse struct {
	Sections []hotSection `json:"sections"` // List of sections
}
