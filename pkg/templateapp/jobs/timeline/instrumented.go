package timeline

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type instrumentedStore struct {
	base   Store
	engine string

	appendTotal    *prometheus.CounterVec
	appendDuration *prometheus.HistogramVec
	rangeDuration  *prometheus.HistogramVec
	trimDuration   *prometheus.HistogramVec
	opErrors       *prometheus.CounterVec
}

// Instrumented wraps s and records shared timeline operation metrics.
func Instrumented(s Store, reg prometheus.Registerer) Store {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	engine := s.Engine()
	w := &instrumentedStore{
		base:   s,
		engine: engine,
		appendTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: MetricAppendTotal,
			Help: "Timeline append operations completed.",
		}, []string{MetricEngineLabel}),
		appendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    MetricAppendDuration,
			Help:    "Timeline append operation duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{MetricEngineLabel}),
		rangeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    MetricRangeDuration,
			Help:    "Timeline range operation duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{MetricEngineLabel}),
		trimDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    MetricTrimDuration,
			Help:    "Timeline trim operation duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{MetricEngineLabel}),
		opErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: MetricOpErrorsTotal,
			Help: "Timeline store operation errors.",
		}, []string{MetricEngineLabel, MetricOpErrorsLabelOp}),
	}
	reg.MustRegister(w.appendTotal, w.appendDuration, w.rangeDuration, w.trimDuration, w.opErrors)
	return w
}

func (w *instrumentedStore) Engine() string {
	return w.engine
}

func (w *instrumentedStore) Append(ctx context.Context, userIDs []int64, item Item) error {
	start := time.Now()
	err := w.base.Append(ctx, userIDs, item)
	w.observe(w.appendDuration, start)
	if err != nil {
		w.opErrors.WithLabelValues(w.engine, "append").Inc()
		return err
	}
	w.appendTotal.WithLabelValues(w.engine).Inc()
	return nil
}

func (w *instrumentedStore) Range(ctx context.Context, userID int64, limit int) ([]Item, error) {
	start := time.Now()
	items, err := w.base.Range(ctx, userID, limit)
	w.observe(w.rangeDuration, start)
	if err != nil {
		w.opErrors.WithLabelValues(w.engine, "range").Inc()
		return nil, err
	}
	return items, nil
}

func (w *instrumentedStore) Trim(ctx context.Context, userID int64, keep int) error {
	start := time.Now()
	err := w.base.Trim(ctx, userID, keep)
	w.observe(w.trimDuration, start)
	if err != nil {
		w.opErrors.WithLabelValues(w.engine, "trim").Inc()
		return err
	}
	return nil
}

func (w *instrumentedStore) observe(h *prometheus.HistogramVec, start time.Time) {
	h.WithLabelValues(w.engine).Observe(time.Since(start).Seconds())
}
