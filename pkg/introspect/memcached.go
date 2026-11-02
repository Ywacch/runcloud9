package introspect

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

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
	return []Capability{CapStats}
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
