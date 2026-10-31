package timeline

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// OpenPostgres builds an instrumented Postgres timeline store.
func OpenPostgres(w PGWriter, r PGReader, opts PostgresOptions, reg prometheus.Registerer) (Store, error) {
	if w == nil {
		return nil, fmt.Errorf("timeline: nil postgres writer")
	}
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	if r == nil {
		var err error
		r, err = coalescePGReader(w, nil)
		if err != nil {
			return nil, err
		}
	}
	base, err := NewPostgres(w, r, opts)
	if err != nil {
		return nil, err
	}
	return Instrumented(base, reg), nil
}
