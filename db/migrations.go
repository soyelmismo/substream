package db

import (
	"fmt"
	"strings"

	"github.com/jinzhu/gorm"
)

// MigrateToURNs performs a safe migration from integer TidalIDs to URN string format.
// This function must be called BEFORE AutoMigrate to preserve existing data.
// It uses raw SQLite queries since SQLite doesn't support ALTER COLUMN TYPE.
func MigrateToURNs(db *gorm.DB) error {
	// Check if migration has already been run
	var setting Setting
	err := db.Where("key = ?", "urn_migration_completed").First(&setting).Error
	if err == nil && setting.Value == "true" {
		return nil
	}

	// Use a transaction for safety
	return db.Transaction(func(tx *gorm.DB) error {
		// Migrate track_stars table
		if err := migrateTable(tx, "track_stars", "tidal_id", "uri", "td", "tr"); err != nil {
			return fmt.Errorf("migrate track_stars: %w", err)
		}

		// Migrate album_stars table
		if err := migrateTable(tx, "album_stars", "tidal_id", "uri", "td", "al"); err != nil {
			return fmt.Errorf("migrate album_stars: %w", err)
		}

		// Migrate artist_stars table
		if err := migrateTable(tx, "artist_stars", "tidal_id", "uri", "td", "ar"); err != nil {
			return fmt.Errorf("migrate artist_stars: %w", err)
		}

		// Migrate track_ratings table
		if err := migrateTable(tx, "track_ratings", "tidal_id", "uri", "td", "tr"); err != nil {
			return fmt.Errorf("migrate track_ratings: %w", err)
		}

		// Migrate album_ratings table
		if err := migrateTable(tx, "album_ratings", "tidal_id", "uri", "td", "al"); err != nil {
			return fmt.Errorf("migrate album_ratings: %w", err)
		}

		// Migrate playlist_tracks table
		if err := migrateTable(tx, "playlist_tracks", "tidal_id", "uri", "td", "tr"); err != nil {
			return fmt.Errorf("migrate playlist_tracks: %w", err)
		}

		// Migrate plays table (adds fallback columns too)
		if err := migratePlaysTable(tx); err != nil {
			return fmt.Errorf("migrate plays: %w", err)
		}

		// Migrate bookmarks table
		if err := migrateBookmarksTable(tx); err != nil {
			return fmt.Errorf("migrate bookmarks: %w", err)
		}

		// Migrate play_queues table
		if err := migratePlayQueueTable(tx); err != nil {
			return fmt.Errorf("migrate play_queues: %w", err)
		}

		// Mark migration as completed
		if err := tx.Exec(`
			INSERT INTO settings (key, value) VALUES ('urn_migration_completed', 'true')
			ON CONFLICT(key) DO UPDATE SET value = 'true'
		`).Error; err != nil {
			return fmt.Errorf("mark migration complete: %w", err)
		}

		return nil
	})
}

// migrateTable handles the standard migration pattern for tables with TidalID -> URI
func migrateTable(tx *gorm.DB, tableName, oldColumn, newColumn, provider, resourceType string) error {
	// Try to add new column - if it fails because it already exists, that's fine
	// If old column doesn't exist, this is a new table and we're done
	if err := tx.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s TEXT`, tableName, newColumn)).Error; err != nil {
		// Check if error is "duplicate column" - that's OK, means already migrated
		if !isDuplicateColumnError(err) {
			// Could be that old column doesn't exist (new table), which is also OK
			// Check if table exists at all by trying a simple query
			if !tableExists(tx, tableName) {
				return nil
			}
			// Some other error
			return fmt.Errorf("add %s column: %w", newColumn, err)
		}
		// Column already exists - continue to populate it
	}

	// Populate new column with URN format: provider:type:id
	// Only update rows where the new column is NULL or empty
	if err := tx.Exec(fmt.Sprintf(`
		UPDATE %s 
		SET %s = '%s:' || '%s:' || CAST(%s AS TEXT)
		WHERE %s IS NULL OR %s = ''
	`, tableName, newColumn, provider, resourceType, oldColumn, newColumn, newColumn)).Error; err != nil {
		// This might fail if old column doesn't exist, which is fine for new tables
		if isNoSuchColumnError(err, oldColumn) {
			return nil
		}
		return fmt.Errorf("populate %s column: %w", newColumn, err)
	}

	return nil
}

// migratePlaysTable handles the plays table which has additional fallback columns
func migratePlaysTable(tx *gorm.DB) error {
	// Add new columns if they don't exist
	columns := []string{
		"uri TEXT",
		"provider TEXT DEFAULT 'tidal'",
		"isrc TEXT",
		"fallback_artist TEXT",
		"fallback_title TEXT",
	}

	for _, col := range columns {
		if err := tx.Exec(fmt.Sprintf(`ALTER TABLE plays ADD COLUMN %s`, col)).Error; err != nil {
			// Column might already exist, continue
			if !isDuplicateColumnError(err) {
				// Table might not exist, which is fine
				if !tableExists(tx, "plays") {
					return nil
				}
				return fmt.Errorf("add column %s: %w", col, err)
			}
		}
	}

	// Populate URI column with URN format
	if err := tx.Exec(`
		UPDATE plays 
		SET uri = 'td:tr:' || CAST(tidal_id AS TEXT),
		    provider = 'tidal'
		WHERE uri IS NULL OR uri = ''
	`).Error; err != nil {
		if isNoSuchColumnError(err, "tidal_id") {
			return nil // New table, nothing to migrate
		}
		return fmt.Errorf("populate uri: %w", err)
	}

	return nil
}

// migrateBookmarksTable handles the bookmarks table with fallback columns
func migrateBookmarksTable(tx *gorm.DB) error {
	// Add new columns
	columns := []string{
		"uri TEXT",
		"provider TEXT DEFAULT 'tidal'",
		"isrc TEXT",
		"fallback_artist TEXT",
		"fallback_title TEXT",
	}

	for _, col := range columns {
		if err := tx.Exec(fmt.Sprintf(`ALTER TABLE bookmarks ADD COLUMN %s`, col)).Error; err != nil {
			if !isDuplicateColumnError(err) {
				if !tableExists(tx, "bookmarks") {
					return nil
				}
				return fmt.Errorf("add column: %w", err)
			}
		}
	}

	// Populate URI
	if err := tx.Exec(`
		UPDATE bookmarks 
		SET uri = 'td:tr:' || CAST(tidal_id AS TEXT),
		    provider = 'tidal'
		WHERE uri IS NULL OR uri = ''
	`).Error; err != nil {
		if isNoSuchColumnError(err, "tidal_id") {
			return nil
		}
		return fmt.Errorf("populate uri: %w", err)
	}

	return nil
}

// migratePlayQueueTable handles the play_queues table with CurrentURI and Items as JSON array of URIs
func migratePlayQueueTable(tx *gorm.DB) error {
	// Add new columns
	if err := tx.Exec(`ALTER TABLE play_queues ADD COLUMN current_uri TEXT DEFAULT ''`).Error; err != nil {
		if !isDuplicateColumnError(err) {
			if !tableExists(tx, "play_queues") {
				return nil
			}
			return fmt.Errorf("add current_uri: %w", err)
		}
	}

	// Note: Items column already exists as string (JSON), but we need to migrate content
	// from int array to URI string array. This is complex and depends on existing data.
	// For now, we just ensure the column exists and new format will be handled by application logic.

	return nil
}

// tableExists checks if a table exists in the database
func tableExists(tx *gorm.DB, tableName string) bool {
	// Use a simple approach: try to select from the table
	// If it fails, the table doesn't exist
	result := map[string]interface{}{}
	err := tx.Raw(fmt.Sprintf("SELECT 1 as n FROM %s LIMIT 1", tableName)).Scan(&result).Error
	return err == nil
}

// isDuplicateColumnError checks if error is due to column already existing
func isDuplicateColumnError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return contains(errStr, "duplicate column name") ||
		contains(errStr, "already exists") ||
		contains(errStr, "UNIQUE constraint failed")
}

// isNoSuchColumnError checks if error is due to column not existing
func isNoSuchColumnError(err error, colName string) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "no such column") ||
		strings.Contains(errStr, "unknown column") ||
		strings.Contains(errStr, strings.ToLower(colName))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsInternal(s, substr))
}

func containsInternal(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (db *DB) Migrate() error {
	// Run URN migration before AutoMigrate
	if err := MigrateToURNs(db.DB); err != nil {
		return fmt.Errorf("urn migration failed: %w", err)
	}

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

