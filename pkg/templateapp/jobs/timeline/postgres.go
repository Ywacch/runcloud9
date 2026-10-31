package timeline

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

//go:embed postgres_schema.sql
var postgresSchemaSQL string

const postgresEngine = "postgresql"

const postgresAppendBatchSize = 500

// PGWriter executes timeline writes on the primary database.
type PGWriter interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// PGReader executes timeline read queries (often via readrouter in template images).
type PGReader interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PostgresOptions configures the Postgres timelines table adapter.
type PostgresOptions struct {
	MaxEntries int
}

type postgresStore struct {
	writer     PGWriter
	reader     PGReader
	maxEntries int
}

// InitPostgresSchema creates the timelines table and indexes on the primary.
func InitPostgresSchema(ctx context.Context, w PGWriter) error {
	if w == nil {
		return fmt.Errorf("timeline postgres: nil writer")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := w.Exec(ctx, postgresSchemaSQL)
	if err != nil {
		return fmt.Errorf("timeline postgres schema: %w", err)
	}
	return nil
}

// NewPostgres returns a Store backed by the timelines table.
// When r is nil, w must also implement PGReader (for example *pgxpool.Pool).
func NewPostgres(w PGWriter, r PGReader, opts PostgresOptions) (Store, error) {
	if w == nil {
		return nil, fmt.Errorf("timeline postgres: nil writer")
	}
	max := opts.MaxEntries
	if max < 1 {
		max = DefaultMaxEntries
	}
	reader, err := coalescePGReader(w, r)
	if err != nil {
		return nil, err
	}
	return &postgresStore{writer: w, reader: reader, maxEntries: max}, nil
}

func coalescePGReader(w PGWriter, r PGReader) (PGReader, error) {
	if r != nil {
		return r, nil
	}
	if qr, ok := w.(PGReader); ok {
		return qr, nil
	}
	return nil, fmt.Errorf("timeline postgres: reader required when writer does not implement Query")
}

func (p *postgresStore) Engine() string {
	return postgresEngine
}

func (p *postgresStore) Append(ctx context.Context, userIDs []int64, item Item) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(userIDs) == 0 {
		return nil
	}
	payload, err := postgresPayloadJSON(item)
	if err != nil {
		return err
	}
	for start := 0; start < len(userIDs); start += postgresAppendBatchSize {
		end := start + postgresAppendBatchSize
		if end > len(userIDs) {
			end = len(userIDs)
		}
		batch := userIDs[start:end]
		if err := p.appendBatch(ctx, batch, item, payload); err != nil {
			return err
		}
	}
	return p.trimUsersWithKeep(ctx, userIDs, p.maxEntries)
}

func (p *postgresStore) appendBatch(ctx context.Context, userIDs []int64, item Item, payload []byte) error {
	var b strings.Builder
	b.WriteString(`INSERT INTO timelines (user_id, post_id, author_id, payload, created_at) VALUES `)
	args := make([]any, 0, len(userIDs)*5)
	for i, uid := range userIDs {
		if i > 0 {
			b.WriteByte(',')
		}
		base := i*5 + 1
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d)", base, base+1, base+2, base+3, base+4)
		args = append(args, uid, item.PostID, item.AuthorID, payload, item.CreatedAt)
	}
	b.WriteString(` ON CONFLICT (user_id, post_id) DO NOTHING`)
	_, err := p.writer.Exec(ctx, b.String(), args...)
	if err != nil {
		return fmt.Errorf("timeline postgres append: %w", err)
	}
	return nil
}

func (p *postgresStore) trimUsersWithKeep(ctx context.Context, userIDs []int64, keep int) error {
	if len(userIDs) == 0 {
		return nil
	}
	const trimSQL = `
DELETE FROM timelines t USING (
  SELECT user_id, post_id FROM (
    SELECT user_id, post_id,
           row_number() OVER (PARTITION BY user_id ORDER BY created_at DESC, post_id DESC) AS rn
    FROM timelines WHERE user_id = ANY($1)
  ) ranked WHERE rn > $2
) old WHERE t.user_id = old.user_id AND t.post_id = old.post_id`
	_, err := p.writer.Exec(ctx, trimSQL, userIDs, keep)
	if err != nil {
		return fmt.Errorf("timeline postgres trim: %w", err)
	}
	return nil
}

func (p *postgresStore) Range(ctx context.Context, userID int64, limit int) ([]Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	const q = `
SELECT post_id, author_id, payload, created_at
FROM timelines
WHERE user_id = $1
ORDER BY created_at DESC, post_id DESC
LIMIT $2`
	rows, err := p.reader.Query(ctx, q, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("timeline postgres range: %w", err)
	}
	defer rows.Close()
	out := make([]Item, 0, limit)
	for rows.Next() {
		var (
			postID, authorID int64
			payload          []byte
			createdAt        time.Time
		)
		if err := rows.Scan(&postID, &authorID, &payload, &createdAt); err != nil {
			return nil, fmt.Errorf("timeline postgres scan: %w", err)
		}
		out = append(out, Item{
			PostID:    postID,
			AuthorID:  authorID,
			Payload:   append([]byte(nil), payload...),
			CreatedAt: createdAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("timeline postgres rows: %w", err)
	}
	return out, nil
}

func (p *postgresStore) Trim(ctx context.Context, userID int64, keep int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if keep <= 0 {
		return ErrInvalidLimit
	}
	return p.trimUsersWithKeep(ctx, []int64{userID}, keep)
}

func postgresPayloadJSON(item Item) ([]byte, error) {
	if len(item.Payload) > 0 {
		return item.Payload, nil
	}
	raw, err := json.Marshal(map[string]any{
		"post_id":    item.PostID,
		"author_id":  item.AuthorID,
		"created_at": item.CreatedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("timeline postgres marshal payload: %w", err)
	}
	return raw, nil
}
