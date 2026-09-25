package introspect

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const maxRowsPerRequest = 100

// PostgresDriver implements StorageDriver with a single pgx connection.
type PostgresDriver struct {
	conn *pgx.Conn
}

// NewPostgresDriver connects to Postgres at host:5432 (syscrack/syscrack/syscrack).
func NewPostgresDriver(ctx context.Context, host string) (*PostgresDriver, error) {
	cs := fmt.Sprintf("postgres://syscrack:syscrack@%s:5432/syscrack?sslmode=disable", host)
	conn, err := pgx.Connect(ctx, cs)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	return &PostgresDriver{conn: conn}, nil
}

func (d *PostgresDriver) Capabilities() []Capability {
	return []Capability{CapTables, CapSchema, CapRows, CapIndexes}
}

func (d *PostgresDriver) Close() error {
	if d.conn == nil {
		return nil
	}
	return d.conn.Close(context.Background())
}

// ClampPageOpts enforces sane pagination defaults and max page size.
func ClampPageOpts(opts PageOpts) PageOpts {
	if opts.Page < 1 {
		opts.Page = 1
	}
	if opts.Limit < 1 {
		opts.Limit = 50
	}
	if opts.Limit > maxRowsPerRequest {
		opts.Limit = maxRowsPerRequest
	}
	order := strings.ToLower(strings.TrimSpace(opts.Order))
	if order != "desc" {
		opts.Order = "asc"
	} else {
		opts.Order = "desc"
	}
	opts.Sort = strings.TrimSpace(opts.Sort)
	return opts
}

func (d *PostgresDriver) ListTables(ctx context.Context) ([]TableInfo, error) {
	rows, err := d.conn.Query(ctx, `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
		ORDER BY tablename`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TableInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		var est int64
		_ = d.conn.QueryRow(ctx, `
			SELECT COALESCE(n_live_tup, 0)::bigint
			FROM pg_stat_user_tables
			WHERE schemaname = 'public' AND relname = $1`, name).Scan(&est)
		if est < 0 {
			est = 0
		}
		out = append(out, TableInfo{Name: name, RowCount: est})
	}
	return out, rows.Err()
}

func (d *PostgresDriver) GetSchema(ctx context.Context, table string) ([]ColumnInfo, error) {
	pkCols, err := d.primaryKeyColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	pkSet := make(map[string]bool, len(pkCols))
	for _, c := range pkCols {
		pkSet[c] = true
	}

	rows, err := d.conn.Query(ctx, `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		var def *string
		if err := rows.Scan(&col.Name, &col.DataType, &nullable, &def); err != nil {
			return nil, err
		}
		col.Nullable = nullable == "YES"
		col.DefaultValue = def
		col.IsPrimaryKey = pkSet[col.Name]
		out = append(out, col)
	}
	return out, rows.Err()
}

func (d *PostgresDriver) primaryKeyColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := d.conn.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		WHERE i.indisprimary AND c.relname = $1`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

func (d *PostgresDriver) GetRows(ctx context.Context, table string, opts PageOpts) (RowsResult, error) {
	opts = ClampPageOpts(opts)

	cols, err := d.GetSchema(ctx, table)
	if err != nil {
		return RowsResult{}, err
	}
	if len(cols) == 0 {
		return RowsResult{}, fmt.Errorf("table %q has no columns", table)
	}

	sortCol := opts.Sort
	if sortCol == "" {
		for _, c := range cols {
			if c.IsPrimaryKey {
				sortCol = c.Name
				break
			}
		}
		if sortCol == "" {
			sortCol = cols[0].Name
		}
	}
	allowed := false
	for _, c := range cols {
		if c.Name == sortCol {
			allowed = true
			break
		}
	}
	if !allowed {
		return RowsResult{}, fmt.Errorf("invalid sort column %q", sortCol)
	}

	tableIdent := pgx.Identifier{table}.Sanitize()
	sortIdent := pgx.Identifier{sortCol}.Sanitize()
	order := "ASC"
	if opts.Order == "desc" {
		order = "DESC"
	}
	offset := (opts.Page - 1) * opts.Limit

	var total int64
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableIdent)
	if err := d.conn.QueryRow(ctx, countSQL).Scan(&total); err != nil {
		return RowsResult{}, err
	}

	querySQL := fmt.Sprintf(
		`SELECT * FROM %s ORDER BY %s %s LIMIT %d OFFSET %d`,
		tableIdent, sortIdent, order, opts.Limit, offset,
	)
	rows, err := d.conn.Query(ctx, querySQL)
	if err != nil {
		return RowsResult{}, err
	}
	defer rows.Close()

	fd := rows.FieldDescriptions()
	colNames := make([]string, len(fd))
	for i, f := range fd {
		colNames[i] = f.Name
	}

	resultRows := make([]map[string]any, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return RowsResult{}, err
		}
		row := make(map[string]any, len(colNames))
		for i, v := range vals {
			row[colNames[i]] = pgValueToAny(v)
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return RowsResult{}, err
	}

	return RowsResult{
		Columns: colNames,
		Rows:    resultRows,
		Total:   total,
		Page:    opts.Page,
		Limit:   opts.Limit,
	}, nil
}

func pgValueToAny(v any) any {
	if b, ok := v.([16]byte); ok {
		return fmt.Sprintf("%x", b[:])
	}
	return v
}

func (d *PostgresDriver) GetIndexes(ctx context.Context) ([]IndexInfo, error) {
	rows, err := d.conn.Query(ctx, `
		SELECT indexname, tablename, indexdef
		FROM pg_indexes
		WHERE schemaname = 'public'
		ORDER BY tablename, indexname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []IndexInfo
	for rows.Next() {
		var idx IndexInfo
		if err := rows.Scan(&idx.Name, &idx.Table, &idx.Definition); err != nil {
			return nil, err
		}
		def := strings.ToUpper(idx.Definition)
		idx.Primary = strings.Contains(def, "PRIMARY KEY")
		idx.Unique = idx.Primary || strings.Contains(def, " UNIQUE ")
		out = append(out, idx)
	}
	return out, rows.Err()
}
