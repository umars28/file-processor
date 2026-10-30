package testdb

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/jackc/pgx/v5"

	"github.com/umars28/file-processor/internal/store"
)

func URL(ctx context.Context, schema string) (string, error) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		return "", nil
	}
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		return "", fmt.Errorf("connect test database: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
		return "", fmt.Errorf("create schema %s: %w", schema, err)
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse TEST_DATABASE_URL: %w", err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	scoped := u.String()
	if err := store.Migrate(ctx, scoped, io.Discard); err != nil {
		return "", fmt.Errorf("migrate schema %s: %w", schema, err)
	}
	return scoped, nil
}
