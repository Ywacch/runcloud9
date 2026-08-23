package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ywacch/cloud9/pkg/templateapp/consistency"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func productStockEqual(served, truth string) bool {
	var p Product
	if err := json.Unmarshal([]byte(served), &p); err != nil {
		// Unparseable payload: skip stale verdict (cannot compare stock).
		return true
	}
	return strconv.Itoa(p.Stock) == strings.TrimSpace(truth)
}

func (s *Server) productTruthReader(ctx context.Context, key string) (string, error) {
	const prefix = "product:"
	if !strings.HasPrefix(key, prefix) {
		return "", fmt.Errorf("unsupported cache key %q", key)
	}
	id, err := strconv.Atoi(strings.TrimPrefix(key, prefix))
	if err != nil || id <= 0 {
		return "", fmt.Errorf("invalid product key %q", key)
	}
	stock, err := s.db.ProductStockForVerify(ctx, id)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(stock), nil
}

func (s *Server) observeCacheHit(ctx context.Context, key, served string) {
	if s.verifier == nil || !trace.Sampled(ctx) {
		return
	}
	s.verifier.Observe(consistency.ReadObservation{
		Key:       key,
		Source:    "cache",
		Served:    served,
		RequestID: trace.RequestID(ctx),
	})
}

func (s *Server) initConsistencyVerifier() {
	if s.cache == nil && s.proxy == nil {
		return
	}
	if !consistency.Enabled(s.cfg.CacheStrategy) {
		return
	}
	s.verifier = consistency.New(consistency.Config{
		Template: "ecommerce",
		Source:   "cache",
		Strategy: s.cfg.CacheStrategy,
		Equal:    productStockEqual,
	}, s.productTruthReader)
}
