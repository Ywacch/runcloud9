package introspect

import "context"

type Capability string

const (
	CapTables   Capability = "tables"
	CapSchema   Capability = "schema"
	CapRows     Capability = "rows"
	CapIndexes  Capability = "indexes"
	CapStats    Capability = "stats"
	CapKeys     Capability = "keys"
	CapKeyValue Capability = "key_value"
)

type TableInfo struct {
	Name     string `json:"name"`
	RowCount int64  `json:"rowCount"`
}

type ColumnInfo struct {
	Name         string  `json:"name"`
	DataType     string  `json:"dataType"`
	Nullable     bool    `json:"nullable"`
	DefaultValue *string `json:"defaultValue,omitempty"`
	IsPrimaryKey bool    `json:"isPrimaryKey"`
}

type IndexInfo struct {
	Name       string   `json:"name"`
	Table      string   `json:"table"`
	Columns    []string `json:"columns"`
	Unique     bool     `json:"unique"`
	Primary    bool     `json:"primary"`
	Definition string   `json:"definition"`
}

type PageOpts struct {
	Page  int    `json:"page"`
	Limit int    `json:"limit"`
	Sort  string `json:"sort"`
	Order string `json:"order"` // "asc" or "desc"
}

type RowsResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Total   int64            `json:"total"`
	Page    int              `json:"page"`
	Limit   int              `json:"limit"`
}

type KeyInfo struct {
	Key    string `json:"key"`
	Type   string `json:"type"`
	TTL    int64  `json:"ttl"`    // seconds, -1 = no expiry, -2 = not found
	Memory int64  `json:"memory"` // bytes
}

type KeyValue struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value any    `json:"value"`
	TTL   int64  `json:"ttl"`
}

// Driver is the base interface all introspection drivers implement.
type Driver interface {
	Capabilities() []Capability
	Close() error
}

// StorageDriver can browse tables, schema, rows, indexes.
type StorageDriver interface {
	Driver
	ListTables(ctx context.Context) ([]TableInfo, error)
	GetSchema(ctx context.Context, table string) ([]ColumnInfo, error)
	GetRows(ctx context.Context, table string, opts PageOpts) (RowsResult, error)
	GetIndexes(ctx context.Context) ([]IndexInfo, error)
}

// CacheDriver can scan keys and get values.
type CacheDriver interface {
	Driver
	ScanKeys(ctx context.Context, pattern string, cursor uint64, count int) ([]KeyInfo, uint64, error)
	GetKeyValue(ctx context.Context, key string) (KeyValue, error)
}
