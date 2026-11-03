package introspect

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

const metadumpMaxLines = 500

// MemcachedDriver exposes stats-only introspection for Memcached.
type MemcachedDriver struct {
	addr string
}

// NewMemcachedDriver connects to Memcached at host:11211.
func NewMemcachedDriver(ctx context.Context, host string) (*MemcachedDriver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(host, "11211")
	client := memcache.New(addr)
	if err := client.Ping(); err != nil {
		return nil, fmt.Errorf("memcached ping: %w", err)
	}
	return &MemcachedDriver{addr: addr}, nil
}

func (d *MemcachedDriver) Capabilities() []Capability {
	return []Capability{CapStats, CapKeys, CapKeyValue}
}

// ServerStats returns parsed STAT lines from the server.
func (d *MemcachedDriver) ServerStats(ctx context.Context) (map[string]map[string]string, error) {
	stats, err := fetchMemcachedStats(ctx, d.addr)
	if err != nil {
		return nil, fmt.Errorf("memcached stats: %w", err)
	}
	return map[string]map[string]string{d.addr: stats}, nil
}

func fetchMemcachedStats(ctx context.Context, addr string) (map[string]string, error) {
	var dialer net.Dialer
	dialer.Timeout = 5 * time.Second
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "stats\r\n"); err != nil {
		return nil, err
	}

	out := make(map[string]string)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "END" {
			break
		}
		if !strings.HasPrefix(line, "STAT ") {
			continue
		}
		fields := strings.SplitN(line, " ", 3)
		if len(fields) != 3 {
			continue
		}
		out[fields[1]] = fields[2]
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListKeys reads one lru_crawler metadump and returns up to limit matching keys.
// The connection closes when the read stops, including when the cap is hit.
func (d *MemcachedDriver) ListKeys(ctx context.Context, pattern string, limit int, now time.Time) (keys []KeyInfo, truncated bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	var dialer net.Dialer
	dialer.Timeout = 5 * time.Second
	conn, err := dialer.DialContext(ctx, "tcp", d.addr)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := fmt.Fprintf(conn, "lru_crawler metadump all\r\n"); err != nil {
		return nil, false, err
	}
	scanner := bufio.NewScanner(conn)
	lines := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "END" {
			break
		}
		if line == "BUSY" || strings.HasPrefix(line, "SERVER_ERROR") || strings.HasPrefix(line, "ERROR") {
			return nil, false, fmt.Errorf("memcached metadump: %s", line)
		}
		lines++
		if lines > metadumpMaxLines {
			truncated = true
			break
		}
		key, exp, size, ok := parseMetadumpLine(line)
		if !ok {
			continue
		}
		if pattern != "" && !KeyMatchesPatterns(key, []string{pattern}) {
			continue
		}
		keys = append(keys, KeyInfo{
			Key:    key,
			Type:   "string",
			TTL:    metadumpTTL(exp, now),
			Memory: int64(size),
		})
		if len(keys) >= limit {
			truncated = true
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, false, err
	}
	if keys == nil {
		keys = []KeyInfo{}
	}
	return keys, truncated, nil
}

// GetKey fetches one value. A miss returns ok=false.
func (d *MemcachedDriver) GetKey(ctx context.Context, key string) (KeyValue, bool, error) {
	if err := ctx.Err(); err != nil {
		return KeyValue{}, false, err
	}
	client := memcache.New(d.addr)
	client.Timeout = 2 * time.Second
	item, err := client.Get(key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return KeyValue{Key: key, Type: "string", TTL: -2}, false, nil
	}
	if err != nil {
		return KeyValue{}, false, fmt.Errorf("memcached get: %w", err)
	}
	return KeyValue{Key: key, Type: "string", Value: string(item.Value), TTL: -1}, true, nil
}

func parseMetadumpLine(line string) (key string, exp int64, size int, ok bool) {
	if !strings.HasPrefix(line, "key=") {
		return "", 0, 0, false
	}
	var keyRaw, expRaw, sizeRaw string
	for _, field := range strings.Fields(line) {
		switch {
		case strings.HasPrefix(field, "key="):
			keyRaw = strings.TrimPrefix(field, "key=")
		case strings.HasPrefix(field, "exp="):
			expRaw = strings.TrimPrefix(field, "exp=")
		case strings.HasPrefix(field, "size="):
			sizeRaw = strings.TrimPrefix(field, "size=")
		}
	}
	if keyRaw == "" {
		return "", 0, 0, false
	}
	decoded, err := url.QueryUnescape(keyRaw)
	if err != nil {
		decoded = keyRaw
	}
	exp, _ = strconv.ParseInt(expRaw, 10, 64)
	sz, _ := strconv.ParseInt(sizeRaw, 10, 64)
	return decoded, exp, int(sz), true
}

func metadumpTTL(exp int64, now time.Time) int64 {
	if exp <= 0 {
		return -1
	}
	sec := exp - now.Unix()
	if sec < 0 {
		return 0
	}
	return sec
}
