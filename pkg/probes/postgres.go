package probes

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const (
	defaultPostgresUser = "syscrack"
	defaultPostgresDB   = "syscrack"
	defaultPostgresPort = "5432"
)

// PostgresProbe implements ConsistencyProbe over a single pgx connection.
type PostgresProbe struct {
	conn *pgx.Conn
}

// NewPostgresProbe connects to Postgres at host:5432 using the same credentials as deployer/introspect.
func NewPostgresProbe(ctx context.Context, host string) (*PostgresProbe, error) {
	cs := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		defaultPostgresUser, defaultPostgresUser, host, defaultPostgresPort, defaultPostgresDB,
	)
	conn, err := pgx.Connect(ctx, cs)
	if err != nil {
		return nil, fmt.Errorf("postgres probe connect: %w", err)
	}
	return &PostgresProbe{conn: conn}, nil
}

// EnsureSchema creates the probe table if it does not exist.
func (p *PostgresProbe) EnsureSchema(ctx context.Context) error {
	if p == nil || p.conn == nil {
		return fmt.Errorf("postgres probe: not connected")
	}
	_, err := p.conn.Exec(ctx, probeCreateTableSQL)
	if err != nil {
		return fmt.Errorf("postgres probe ensure schema: %w", err)
	}
	return nil
}

// WriteVersion bumps the singleton logical version and returns the new value.
func (p *PostgresProbe) WriteVersion(ctx context.Context) (int64, error) {
	if p == nil || p.conn == nil {
		return 0, fmt.Errorf("postgres probe: not connected")
	}
	var version int64
	err := p.conn.QueryRow(ctx, `
		INSERT INTO _c9_probe (id, version, written_at)
		VALUES ($1, 1, now())
		ON CONFLICT (id) DO UPDATE
		SET version = _c9_probe.version + 1,
		    written_at = now()
		RETURNING version`, probeRowID).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("postgres probe write version: %w", err)
	}
	return version, nil
}

// ReadVersion returns the current logical version on this connection's host.
func (p *PostgresProbe) ReadVersion(ctx context.Context) (int64, bool, error) {
	if p == nil || p.conn == nil {
		return 0, false, fmt.Errorf("postgres probe: not connected")
	}
	var version int64
	err := p.conn.QueryRow(ctx, `SELECT version FROM _c9_probe WHERE id = $1`, probeRowID).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("postgres probe read version: %w", err)
	}
	return version, true, nil
}

// Close releases the underlying connection.
func (p *PostgresProbe) Close() error {
	if p == nil || p.conn == nil {
		return nil
	}
	return p.conn.Close(context.Background())
}
