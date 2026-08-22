package probes

import (
	"fmt"
	"strconv"
)

// ConsistencyToken is an opaque comparable version for cache↔DB and replica probes.
type ConsistencyToken interface {
	GTE(other ConsistencyToken) bool
}

// Int64Token is the Postgres logical-version encoding used by _c9_probe today.
type Int64Token int64

// GTE reports whether t is greater than or equal to other.
func (t Int64Token) GTE(other ConsistencyToken) bool {
	o, ok := other.(Int64Token)
	if !ok {
		return false
	}
	return int64(t) >= int64(o)
}

// FormatInt64Token encodes a logical version for cache storage.
func FormatInt64Token(t Int64Token) string {
	return strconv.FormatInt(int64(t), 10)
}

// ParseInt64Token decodes a logical version from cache storage.
func ParseInt64Token(s string) (Int64Token, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse int64 token: %w", err)
	}
	return Int64Token(n), nil
}

// TokenBehind reports whether cached lags behind written (stale cache hit vs primary).
func TokenBehind(written, cached ConsistencyToken) bool {
	w, ok := written.(Int64Token)
	if !ok {
		return false
	}
	c, ok := cached.(Int64Token)
	if !ok {
		return false
	}
	return c < w
}
