package db

import (
	"strings"
	"time"
)

type User struct {
	ID                int       `gorm:"primary_key"`
	CreatedAt         time.Time `sql:"DEFAULT:current_timestamp"`
	Name              string    `gorm:"not null; unique_index"`
	Password          string    `gorm:"not null"`      // Bcrypt hash for secure auth (login, basic auth)
	SubsonicPassword  string    `gorm:"default: null"` // Plaintext for Subsonic token auth (t/s params)
	IsAdmin           bool      `gorm:"not null" sql:"DEFAULT:false"`
	Avatar            []byte    `sql:"default: null"`
	LastfmSession     string    `sql:"default: null"`
	ListenbrainzUrl   string    `sql:"default: null"`
	ListenbrainzToken string    `sql:"default: null"`
	SessionVersion    int       `gorm:"not null" sql:"DEFAULT:0"` // Incremented to invalidate all sessions
}

type TrackStar struct {
	UserID         int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI            string    `gorm:"primary_key; auto_increment:false; index:idx_track_star_uri"` // URN format: td:tr:12345
	Provider       string    `gorm:"default:'tidal'; index:idx_track_star_provider"`              // tidal, spotify, deezer, etc.
	ISRC           string    `gorm:"index:idx_track_star_isrc"`                                   // For cross-provider matching
	FallbackArtist string    // Artist name for cross-matching when URI changes
	FallbackTitle  string    // Track title for cross-matching when URI changes
	StarDate       time.Time `sql:"DEFAULT:current_timestamp"`
}

type AlbumStar struct {
	UserID         int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI            string    `gorm:"primary_key; auto_increment:false; index:idx_album_star_uri"` // URN format: td:al:12345
	FallbackArtist string    // Artist name for searching
	FallbackTitle  string    // Album title for searching
	StarDate       time.Time `sql:"DEFAULT:current_timestamp"`
	LastPlayed     time.Time `sql:"DEFAULT:NULL"` // last time any track from this album was played
	PlayCount      int       `sql:"DEFAULT:0"`    // aggregated play count for this album
}

type ArtistStar struct {
	UserID       int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	URI          string    `gorm:"primary_key; auto_increment:false; index:idx_artist_star_uri"` // URN format: td:ar:12345
	FallbackName string    // Artist name for searching
	StarDate     time.Time `sql:"DEFAULT:current_timestamp"`
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
	CoverURL  string    `sql:"default: ''"` // External cover URL (from Tidal, etc.)
	CoverPath string    `sql:"default: ''"` // Local path for custom uploaded covers
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
	URI            string    `gorm:"not null; index:idx_plays_uri; unique_index:idx_plays_user_uri"` // URN format: td:tr:12345
	Provider       string    `gorm:"default:'tidal'"`                                                // Provider for this track
	ISRC           string    `gorm:"index:idx_plays_isrc"`                                           // For cross-provider matching
	FallbackArtist string    // Artist name for cross-matching
	FallbackTitle  string    // Track title for cross-matching
	PlayedAt       time.Time `gorm:"index:idx_plays_user_time" sql:"DEFAULT:current_timestamp"`
	Count          int       `gorm:"not null; index:idx_plays_user_count" sql:"DEFAULT:1"`
}

type PlayQueue struct {
	UserID     int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	Current    int       `gorm:"not null" sql:"DEFAULT:0"`    // Position in queue (index)
	Position   int       `gorm:"not null" sql:"DEFAULT:0"`    // Playback position within current track (ms)
	Items      string    `gorm:"not null" sql:"DEFAULT:'[]'"` // JSON array of URIs (e.g., ["td:tr:123", "td:tr:456"])
	CurrentURI string    `gorm:"not null; default:''"`        // Current track URI (e.g., "td:tr:12345")
	ChangedBy  string    `sql:"default: ''"`
	UpdatedAt  time.Time `sql:"DEFAULT:current_timestamp"`
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

type TidalAccount struct {
	ID               int       `gorm:"primary_key"`
	UserID           string    `gorm:"index"`
	Username         string    `sql:"default: ''"`
	RefreshToken     string    `gorm:"not null"`
	AccessToken      string    `sql:"default: ''"`
	SubscriptionType string    `sql:"default: 'PREMIUM'"` // e.g. "HIFI", "PREMIUM", "FREE"
	HighestQuality   string    `sql:"default: 'HIGH'"`    // e.g. "HI_RES", "LOSSLESS", "HIGH", "LOW"
	QualityTier      int       `sql:"default: 50"`        // e.g. 100 (HiRes), 80 (Lossless FLAC), 50 (High 320k)
	ExpiresAt        time.Time `sql:"DEFAULT:current_timestamp"`
	IsActive         bool      `sql:"DEFAULT: true"`
	CreatedAt        time.Time `sql:"DEFAULT:current_timestamp"`
	UpdatedAt        time.Time `sql:"DEFAULT:current_timestamp"`
}

// DeezerAccount represents a single subscriber account used to mint direct
// Deezer stream URLs. Authentication is performed with the long-lived `arl`
// session cookie (NOT an OAuth refresh token), so the fields below differ from
// TidalAccount: there is no access/refresh pair, and "subscription" is read
// straight from the public user endpoint whenever we validate the cookie.
type DeezerAccount struct {
	ID               int       `gorm:"primary_key"`
	ARL              string    `gorm:"not null"` // Deezer session cookie (NOT an OAuth token)
	UserID           string    `sql:"default: ''"`
	Username         string    `sql:"default: ''"`
	SubscriptionType string    `sql:"default: 'FREE'"` // "PREMIUM" or "FREE" (Deezer calls HiFi "PREMIUM", not "HIFI")
	HighestQuality   string    `sql:"default: 'MP3_128'"`
	QualityTier      int       `sql:"default: 10"` // 80 (HiFi/FLAC), 50 (320k MP3), 10 (128k MP3)
	ExpiresAt        time.Time `sql:"DEFAULT:current_timestamp"`
	IsActive         bool      `sql:"DEFAULT: true"`
	LastError        string    `sql:"default: ''"` // Last verification error, for admin visibility
	LastValidatedAt  time.Time `sql:"DEFAULT:current_timestamp"`
	CreatedAt        time.Time `sql:"DEFAULT:current_timestamp"`
	UpdatedAt        time.Time `sql:"DEFAULT:current_timestamp"`
}

// TableName overrides GORM pluralization (deezer_accounts -> "deezer_accounts")
func (DeezerAccount) TableName() string {
	return "deezer_accounts"
}

// QobuzAccount represents a subscriber account used by the Qobuz provider to fetch
// lossless FLAC and Hi-Res 24-bit streams. Authentication is performed via user_auth_token.
type QobuzAccount struct {
	ID               int       `gorm:"primary_key"`
	UserAuthToken    string    `gorm:"not null;unique_index"`
	UserID           string    `sql:"default: ''"`
	Username         string    `sql:"default: ''"`
	SubscriptionType string    `sql:"default: 'FREE'"` // "Studio", "Sublime", "Premium", "FREE"
	HighestQuality   string    `sql:"default: 'MP3_320'"`
	QualityTier      int       `sql:"default: 10"` // 90 (24/192), 85 (24/96), 80 (16/44.1 FLAC), 10 (Free)
	IsActive         bool      `sql:"DEFAULT: true"`
	LastError        string    `sql:"default: ''"`
	LastValidatedAt  time.Time `sql:"DEFAULT:current_timestamp"`
	CreatedAt        time.Time `sql:"DEFAULT:current_timestamp"`
	UpdatedAt        time.Time `sql:"DEFAULT:current_timestamp"`
}

// TableName overrides GORM pluralization
func (QobuzAccount) TableName() string {
	return "qobuz_accounts"
}

// TrackMetadata stores the mapping between tracks and their album/artist.
// This is essential for the virtual library feature - it allows us to infer
// which artists and albums a user has interacted with based on track plays/stars.
type TrackMetadata struct {
	URI       string    `gorm:"primary_key"`     // URN format: td:tr:12345
	AlbumURI  string    `gorm:"not null; index"` // URN format: td:al:12345
	ArtistURI string    `gorm:"not null; index"` // URN format: td:ar:12345
	UpdatedAt time.Time `sql:"DEFAULT:current_timestamp"`
}

// MetadataCache stores cached metadata from Tidal/hot.monochrome to avoid cold-start issues.
// Used for persistent caching of artist/album/track metadata.
type MetadataCache struct {
	Key        string    `gorm:"primary_key"` // e.g., "artist:12345" or "album:67890"
	Value      []byte    `gorm:"not null"`    // JSON serialized metadata
	FetchedAt  time.Time `gorm:"not null"`
	TTLSeconds int       `gorm:"default:86400"` // 24h default
}

// TableName overrides the table name used by GORM to prevent pluralization (metadata_caches)
func (MetadataCache) TableName() string {
	return "metadata_cache"
}

// TableName overrides the table name used by GORM to prevent pluralization (track_metadatas)
func (TrackMetadata) TableName() string {
	return "track_metadata"
}

// QualityTier constants for ranking stream fidelity across providers
const (
	QualityTierHiRes    = 100 // Hi-Res Lossless (24-bit / 96-192kHz - Tidal Hi-Res, Qobuz)
	QualityTierLossless = 80  // CD Lossless (16-bit / 44.1kHz FLAC - Tidal HiFi, Deezer)
	QualityTierHigh     = 50  // High Quality Lossy (320kbps MP3 / 256kbps AAC)
	QualityTierStandard = 30  // Standard Streaming (160kbps Opus - YouTube Music)
	QualityTierFree     = 10  // Free / Preview-only account (no full-track playback)
)

// MaxQualityTierForProvider returns the best fidelity a provider can physically deliver.
// Cross-provider fallbacks used to memorize every mapped source as QualityTierLossless,
// which made lossy sources (YouTube, SoundCloud, Spotify) look like FLAC and let the
// 0 ms mapped-source fast path serve them forever, skipping Qobuz/Tidal/Deezer entirely.
func MaxQualityTierForProvider(provider string) int {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "qb", "td", "dz", "am":
		return QualityTierHiRes
	case "sp":
		return QualityTierHigh
	case "yt", "sc":
		return QualityTierStandard
	default:
		return QualityTierStandard
	}
}

// EffectiveQualityTier clamps a stored quality tier to what its provider can deliver.
// Read track_sources tiers through this helper: rows written by older versions (or by a
// mis-matched fallback) can claim lossless for a lossy provider.
func EffectiveQualityTier(provider string, storedTier int) int {
	if maxTier := MaxQualityTierForProvider(provider); storedTier > maxTier {
		return maxTier
	}
	return storedTier
}

// TrackSource represents a specific provider audio source for a canonical song cluster.
// Enables infinite provider scalability, zero-migration additions, and dynamic quality fallback.
type TrackSource struct {
	ID          int       `gorm:"primary_key"`
	ClusterKey  string    `gorm:"not null; index:idx_track_sources_cluster"`    // "isrc:..." or "text:artist:title"
	Provider    string    `gorm:"not null; index:idx_track_sources_provider"`   // "td", "yt", "dz", "qb", etc.
	URI         string    `gorm:"not null; unique_index:idx_track_sources_uri"` // "td:tr:123", "yt:tr:xyz"
	QualityTier int       `gorm:"not null"`                                     // 100 (HiRes), 80 (FLAC), 50 (320k), 30 (Opus)
	IsAvailable bool      `gorm:"not null; default:true"`
	UpdatedAt   time.Time `sql:"DEFAULT:current_timestamp"`
}

// TableName overrides the table name used by GORM to prevent pluralization
func (TrackSource) TableName() string {
	return "track_sources"
}

// YTMusicAccount represents a stored YouTube Music user account / session cookies
type YTMusicAccount struct {
	ID           int       `gorm:"primary_key"`
	Name         string    `gorm:"not null"`
	CookieData   string    `gorm:"type:text;not null"`
	CookieFile   string    `gorm:"type:text;not null"`
	CookieHeader string    `gorm:"type:text;not null"`
	IsActive     bool      `gorm:"not null;default:true"`
	IsPremium    bool      `gorm:"not null;default:false"`
	CreatedAt    time.Time `sql:"DEFAULT:current_timestamp"`
	UpdatedAt    time.Time `sql:"DEFAULT:current_timestamp"`
}

// TableName overrides table name for YTMusicAccount
func (YTMusicAccount) TableName() string {
	return "ytmusic_accounts"
}
