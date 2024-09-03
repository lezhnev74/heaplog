package db

import (
	"database/sql"
	"embed"
	"io"
	"strings"
)

//go:embed migrations
var migrationFS embed.FS

func Migrate(db *sql.DB) (err error) {
	f, err := migrationFS.Open("migrations/1_init.up.sql")
	if err != nil {
		return err
	}
	migrateContent, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	_, err = db.Exec(string(migrateContent))
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil // skip migrations
	}
	if err != nil {
		return err
	}
	return nil
}
