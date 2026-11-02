package kvcache

import (
	"fmt"
	"os"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
)

// Config selects a cache implementation from deployer job env vars.
type Config struct {
	EngineEnv       string
	HostEnv         string
	PortEnv         string
	RedisClient     redis.UniversalClient
	MemcachedClient *memcache.Client
}

// Open returns a Cache from HOT_CACHE_* (or legacy CACHE_HOST) configuration.
func Open(cfg Config) (Cache, error) {
	engine := strings.ToLower(strings.TrimSpace(cfg.EngineEnv))
	host := strings.TrimSpace(cfg.HostEnv)
	port := strings.TrimSpace(cfg.PortEnv)
	if host == "" {
		return nil, fmt.Errorf("kvcache: host is required")
	}
	if port == "" {
		port = defaultPort(engine)
	}
	switch engine {
	case "", "redis":
		client := cfg.RedisClient
		if client == nil {
			client = redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", host, port)})
		}
		return NewRedis(client)
	case "memcached":
		client := cfg.MemcachedClient
		if client == nil {
			client = memcache.New(fmt.Sprintf("%s:%s", host, port))
		}
		return NewMemcached(client)
	default:
		return nil, fmt.Errorf("kvcache: unsupported HOT_CACHE_ENGINE %q", engine)
	}
}

func defaultPort(engine string) string {
	switch engine {
	case "memcached":
		return "11211"
	default:
		return "6379"
	}
}

// EnvConfig builds Config from standard process env.
func EnvConfig(rdb redis.UniversalClient) Config {
	engine := os.Getenv("HOT_CACHE_ENGINE")
	host := os.Getenv("HOT_CACHE_HOST")
	port := os.Getenv("HOT_CACHE_PORT")
	if strings.TrimSpace(host) == "" {
		host = os.Getenv("CACHE_HOST")
	}
	if strings.TrimSpace(port) == "" {
		port = os.Getenv("CACHE_PORT")
	}
	if strings.TrimSpace(engine) == "" {
		engine = os.Getenv("CACHE_ENGINE")
	}
	if strings.TrimSpace(engine) == "" {
		engine = "redis"
	}
	return Config{
		EngineEnv:   engine,
		HostEnv:     host,
		PortEnv:     port,
		RedisClient: rdb,
	}
}

// WithLegacyAddr fills host and port only when EnvConfig left them empty.
// A legacy CACHE_PORT of 6379 must not replace HOT_CACHE_PORT.
func (c Config) WithLegacyAddr(host, port string) Config {
	if strings.TrimSpace(c.HostEnv) == "" {
		c.HostEnv = strings.TrimSpace(host)
	}
	if strings.TrimSpace(c.PortEnv) == "" {
		c.PortEnv = strings.TrimSpace(port)
	}
	return c
}
