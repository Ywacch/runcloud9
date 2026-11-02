package urlstore

import (
	"context"
	"time"
)

// Record is one row in urls.
type Record struct {
	Code      string
	Original  string
	CreatedAt time.Time
}

// Store is the url-shortener main_db job (Postgres today).
type Store interface {
	Ping(ctx context.Context) error
	InitSchema(ctx context.Context) error
	InsertURL(ctx context.Context, code, original string) (inserted bool, err error)
	GetURL(ctx context.Context, code string) (Record, error)
	URLExists(ctx context.Context, code string) (bool, error)
	ListCodes(ctx context.Context, limit int) ([]string, error)
	Close()
}
