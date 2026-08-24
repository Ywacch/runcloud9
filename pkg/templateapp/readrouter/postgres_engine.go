package readrouter

import "fmt"

// PostgresEngine implements Engine for postgres WAL LSN tokens.
type PostgresEngine struct{}

// WriteTokenExpr returns the SQL expression for the current write LSN.
func (PostgresEngine) WriteTokenExpr() string {
	return "pg_current_wal_lsn()::text"
}

// ReadTokenExpr returns the SQL expression for the replica replay LSN.
func (PostgresEngine) ReadTokenExpr() string {
	return "pg_last_wal_replay_lsn()::text"
}

// ParseToken parses a postgres LSN text value into a ConsistencyToken.
func (PostgresEngine) ParseToken(raw string) (ConsistencyToken, error) {
	n, err := parseLSN(raw)
	if err != nil {
		return nil, fmt.Errorf("parse postgres lsn: %w", err)
	}
	tok := lsnToken(n)
	return tok, nil
}
