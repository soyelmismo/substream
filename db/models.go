package db

import (
	"time"
)

type User struct {
	ID                int       `gorm:"primary_key"`
	CreatedAt         time.Time `sql:"DEFAULT:current_timestamp"`
	Name              string    `gorm:"not null; unique_index"`
	Password          string    `gorm:"not null"`
	IsAdmin           bool      `gorm:"not null" sql:"DEFAULT:false"`
	Avatar            []byte    `sql:"default: null"`
	LastfmSession     string    `sql:"default: null"`
	ListenbrainzUrl   string    `sql:"default: null"`
	ListenbrainzToken string    `sql:"default: null"`
}

type TrackStar struct {
	UserID         int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI            string    `gorm:"primary_key; auto_increment:false; index:idx_track_star_uri"` // URN format: td:tr:12345
	Provider       string    `gorm:"default:'tidal'; index:idx_track_star_provider"`              // tidal, spotify, deezer, etc.
	ISRC           string    `gorm:"index:idx_track_star_isrc"`                                  // For cross-provider matching
	FallbackArtist string    // Artist name for cross-matching when URI changes
	FallbackTitle  string    // Track title for cross-matching when URI changes
	StarDate       time.Time `sql:"DEFAULT:current_timestamp"`
}

type AlbumStar struct {
	UserID     int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI        string    `gorm:"primary_key; auto_increment:false; index:idx_album_star_uri"` // URN format: td:al:12345
	StarDate   time.Time `sql:"DEFAULT:current_timestamp"`
	LastPlayed time.Time `sql:"DEFAULT:NULL"` // last time any track from this album was played
	PlayCount  int       `sql:"DEFAULT:0"`  // aggregated play count for this album
}

type ArtistStar struct {
	UserID   int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI      string    `gorm:"primary_key; auto_increment:false; index:idx_artist_star_uri"` // URN format: td:ar:12345
	StarDate time.Time `sql:"DEFAULT:current_timestamp"`
}

type TrackRating struct {
	UserID int    `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI    string `gorm:"primary_key; auto_increment:false; index:idx_track_rating_uri"` // URN format: td:tr:12345
	Rating int    `gorm:"not null; check:(rating >= 1 AND rating <= 5)"`
}

type AlbumRating struct {
	UserID int    `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI    string `gorm:"primary_key; auto_increment:false; index:idx_album_rating_uri"` // URN format: td:al:12345
	Rating int    `gorm:"not null; check:(rating >= 1 AND rating <= 5)"`
}

type Playlist struct {
	ID        int       `gorm:"primary_key"`
	UserID    int       `gorm:"not null" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	Name      string    `gorm:"not null"`
	Comment   string    `sql:"default: ''"`
	IsPublic  bool      `gorm:"not null" sql:"DEFAULT:false"`
	CreatedAt time.Time `sql:"DEFAULT:current_timestamp"`
	UpdatedAt time.Time `sql:"DEFAULT:current_timestamp"`
}

type PlaylistTrack struct {
	ID         int    `gorm:"primary_key"`
	PlaylistID int    `gorm:"not null" sql:"type:int REFERENCES playlists(id) ON DELETE CASCADE"`
	URI        string `gorm:"not null; index:idx_playlist_track_uri"` // URN format: td:tr:12345
	Position   int    `gorm:"not null"`
}

type Play struct {
	ID             int       `gorm:"primary_key"`
	UserID         int       `gorm:"not null; index:idx_plays_user_time; index:idx_plays_user_count; unique_index:idx_plays_user_uri" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI            string    `gorm:"not null; index:idx_plays_uri; unique_index:idx_plays_user_uri"`               // URN format: td:tr:12345
	Provider       string    `gorm:"default:'tidal'"`                             // Provider for this track
	ISRC           string    `gorm:"index:idx_plays_isrc"`                        // For cross-provider matching
	FallbackArtist string    // Artist name for cross-matching
	FallbackTitle  string    // Track title for cross-matching
	PlayedAt       time.Time `gorm:"index:idx_plays_user_time" sql:"DEFAULT:current_timestamp"`
	Count          int       `gorm:"not null; index:idx_plays_user_count" sql:"DEFAULT:1"`
}

type PlayQueue struct {
	UserID    int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	Current   int       `gorm:"not null" sql:"DEFAULT:0"`           // Position in queue (index)
	Position  int       `gorm:"not null" sql:"DEFAULT:0"`           // Playback position within current track (ms)
	Items     string    `gorm:"not null" sql:"DEFAULT:'[]'"`        // JSON array of URIs (e.g., ["td:tr:123", "td:tr:456"])
	CurrentURI string   `gorm:"not null; default:''"`               // Current track URI (e.g., "td:tr:12345")
	ChangedBy string    `sql:"default: ''"`
	UpdatedAt time.Time `sql:"DEFAULT:current_timestamp"`
}

type Setting struct {
	Key   string `gorm:"primary_key; auto_increment:false"`
	Value string
}

type Bookmark struct {
	ID             int       `gorm:"primary_key"`
	UserID         int       `gorm:"not null" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI            string    `gorm:"not null; index:idx_bookmark_uri"` // URN format: td:tr:12345
	Provider       string    `gorm:"default:'tidal'"`                  // Provider for this bookmark
	ISRC           string    `gorm:"index:idx_bookmark_isrc"`          // For cross-provider matching
	FallbackArtist string    // Artist name for cross-matching
	FallbackTitle  string    // Track title for cross-matching
	Position       int       `gorm:"not null" sql:"DEFAULT:0"`
	Comment        string    `sql:"default: ''"`
	CreatedAt      time.Time `sql:"DEFAULT:current_timestamp"`
	UpdatedAt      time.Time `sql:"DEFAULT:current_timestamp"`
}

type ProxyInstance struct {
	ID        int       `gorm:"primary_key"`
	URL       string    `gorm:"not null; unique_index"`
	Name      string    `sql:"default: ''"`
	IsHealthy bool      `sql:"DEFAULT: true"`
	Source    string    `sql:"DEFAULT: 'manual'"`
	CreatedAt time.Time `sql:"DEFAULT:current_timestamp"`
}

// TrackMetadata stores the mapping between tracks and their album/artist.
// This is essential for the virtual library feature - it allows us to infer
// which artists and albums a user has interacted with based on track plays/stars.
type TrackMetadata struct {
	URI        string    `gorm:"primary_key"` // URN format: td:tr:12345
	AlbumURI   string    `gorm:"not null; index"` // URN format: td:al:12345
	ArtistURI  string    `gorm:"not null; index"` // URN format: td:ar:12345
	UpdatedAt  time.Time `sql:"DEFAULT:current_timestamp"`
}

// MetadataCache stores cached metadata from Tidal/hot.monochrome to avoid cold-start issues.
// Used for persistent caching of artist/album/track metadata.
type MetadataCache struct {
	Key         string    `gorm:"primary_key"` // e.g., "artist:12345" or "album:67890"
	Value       []byte    `gorm:"not null"`     // JSON serialized metadata
	FetchedAt   time.Time `gorm:"not null"`
	TTLSeconds  int       `gorm:"default:86400"` // 24h default
}


