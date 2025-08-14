package duckdb

import (
	"context"
	"database/sql"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type DuckDB struct {
	db *sql.DB
}

func (duck *DuckDB) Open(ctx context.Context, dir string) (err error) {
	duck.db, err = sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		duck.Close() // Flush buffers
	}()
	return nil
}

func (duck *DuckDB) Close() (err error) { return duck.db.Close() }
