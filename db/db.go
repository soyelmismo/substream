package db

import (
	"fmt"
	"log"
	"os"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

type DB struct {
	*gorm.DB
}

func New(path string) (*DB, error) {
	db, err := gorm.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("with gorm: %w", err)
	}

	db.SetLogger(log.New(os.Stdout, "gorm ", 0))
	db.DB().SetMaxOpenConns(4)

	// Custom pragmas for SQLite optimization
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA foreign_keys=ON")

	return &DB{DB: db}, nil
}

func (db *DB) GetUserByName(username string) *User {
	var user User
	err := db.Where("name=?", username).First(&user).Error
	if err != nil {
		return nil
	}
	return &user
}
