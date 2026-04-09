package db

func (db *DB) Migrate() error {
	return db.AutoMigrate(
		&User{},
		&TrackStar{},
		&AlbumStar{},
		&ArtistStar{},
		&TrackRating{},
		&AlbumRating{},
		&Playlist{},
		&PlaylistTrack{},
		&Play{},
		&PlayQueue{},
		&Setting{},
		&Bookmark{},
		&ProxyInstance{},
	).Error
}

