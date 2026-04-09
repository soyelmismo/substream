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
	UserID   int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	TidalID  int       `gorm:"primary_key; auto_increment:false"`
	StarDate time.Time `sql:"DEFAULT:current_timestamp"`
}

type AlbumStar struct {
	UserID   int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	TidalID  int       `gorm:"primary_key; auto_increment:false"`
	StarDate time.Time `sql:"DEFAULT:current_timestamp"`
}

type ArtistStar struct {
	UserID   int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	TidalID  int       `gorm:"primary_key; auto_increment:false"`
	StarDate time.Time `sql:"DEFAULT:current_timestamp"`
}

type TrackRating struct {
	UserID  int `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	TidalID int `gorm:"primary_key; auto_increment:false"`
	Rating  int `gorm:"not null; check:(rating >= 1 AND rating <= 5)"`
}

type AlbumRating struct {
	UserID  int `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	TidalID int `gorm:"primary_key; auto_increment:false"`
	Rating  int `gorm:"not null; check:(rating >= 1 AND rating <= 5)"`
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
	ID         int `gorm:"primary_key"`
	PlaylistID int `gorm:"not null" sql:"type:int REFERENCES playlists(id) ON DELETE CASCADE"`
	TidalID    int `gorm:"not null"`
	Position   int `gorm:"not null"`
}

type Play struct {
	ID       int       `gorm:"primary_key"`
	UserID   int       `gorm:"not null; index:idx_plays_user_time; index:idx_plays_user_count" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	TidalID  int       `gorm:"not null"`
	PlayedAt time.Time `gorm:"index:idx_plays_user_time" sql:"DEFAULT:current_timestamp"`
	Count    int       `gorm:"not null; index:idx_plays_user_count" sql:"DEFAULT:1"`
}

type PlayQueue struct {
	UserID    int       `gorm:"primary_key; auto_increment:false" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	Current   int       `gorm:"not null" sql:"DEFAULT:0"`
	Position  int       `gorm:"not null" sql:"DEFAULT:0"`
	Items     string    `gorm:"not null" sql:"DEFAULT:'[]'"`
	ChangedBy string    `sql:"default: ''"`
	UpdatedAt time.Time `sql:"DEFAULT:current_timestamp"`
}

type Setting struct {
	Key   string `gorm:"primary_key; auto_increment:false"`
	Value string
}

type Bookmark struct {
	ID        int       `gorm:"primary_key"`
	UserID    int       `gorm:"not null" sql:"type:int REFERENCES users(id) ON DELETE CASCADE"`
	TidalID   int       `gorm:"not null"`
	Position  int       `gorm:"not null" sql:"DEFAULT:0"`
	Comment   string    `sql:"default: ''"`
	CreatedAt time.Time `sql:"DEFAULT:current_timestamp"`
	UpdatedAt time.Time `sql:"DEFAULT:current_timestamp"`
}

type ProxyInstance struct {
	ID        int       `gorm:"primary_key"`
	URL       string    `gorm:"not null; unique_index"`
	Name      string    `sql:"default: ''"`
	IsHealthy bool      `sql:"DEFAULT: true"`
	Source    string    `sql:"DEFAULT: 'manual'"`
	CreatedAt time.Time `sql:"DEFAULT:current_timestamp"`
}


