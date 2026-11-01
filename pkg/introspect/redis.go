package introspect

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RedisDriver implements CacheDriver against a single Redis instance.
type RedisDriver struct {
	client *redis.Client
}

// NewRedisDriver connects to Redis at host:6379 (no auth).
func NewRedisDriver(ctx context.Context, host string) (*RedisDriver, error) {
	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:6379", host),
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &RedisDriver{client: client}, nil
}

func (d *RedisDriver) Capabilities() []Capability {
	return []Capability{CapKeys, CapKeyValue, CapStats}
}

func (d *RedisDriver) Close() error {
	if d.client == nil {
		return nil
	}
	return d.client.Close()
}

// CountStudentKeys returns the number of student-facing keys in Redis.
// Platform probe keys (_c9_*) are excluded from the total.
func (d *RedisDriver) CountStudentKeys(ctx context.Context) (int64, error) {
	dbSize, err := d.client.DBSize(ctx).Result()
	if err != nil {
		return 0, fmt.Errorf("redis dbsize: %w", err)
	}

	var probeCount int64
	var cursor uint64
	for {
		keys, next, err := d.client.Scan(ctx, cursor, platformTablePrefix+"*", 100).Result()
		if err != nil {
			return 0, fmt.Errorf("redis scan: %w", err)
		}
		probeCount += int64(len(keys))
		cursor = next
		if cursor == 0 {
			break
		}
	}

	count := dbSize - probeCount
	if count < 0 {
		count = 0
	}
	return count, nil
}

func (d *RedisDriver) ScanKeys(ctx context.Context, pattern string, cursor uint64, count int) ([]KeyInfo, uint64, error) {
	if count < 1 {
		count = 50
	}
	if count > 100 {
		count = 100
	}
	// Exact key patterns (e.g. work-queue stream "fanout") must not use SCAN:
	// MATCH can return an empty batch while the cursor is still non-zero.
	if !PatternHasGlob(pattern) {
		if cursor != 0 {
			return nil, 0, nil
		}
		exists, err := d.client.Exists(ctx, pattern).Result()
		if err != nil {
			return nil, 0, err
		}
		if exists == 0 || !IsStudentCacheKey(pattern) {
			return nil, 0, nil
		}
		out, err := d.keyInfosForKeys(ctx, []string{pattern})
		return out, 0, err
	}

	keys, next, err := d.client.Scan(ctx, cursor, pattern, int64(count)).Result()
	if err != nil {
		return nil, 0, err
	}
	out, err := d.keyInfosForKeys(ctx, keys)
	return out, next, err
}

func (d *RedisDriver) keyInfosForKeys(ctx context.Context, keys []string) ([]KeyInfo, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	pipe := d.client.Pipeline()
	typeCmds := make([]*redis.StatusCmd, len(keys))
	ttlCmds := make([]*redis.DurationCmd, len(keys))
	memCmds := make([]*redis.IntCmd, len(keys))
	for i, key := range keys {
		typeCmds[i] = pipe.Type(ctx, key)
		ttlCmds[i] = pipe.TTL(ctx, key)
		memCmds[i] = pipe.MemoryUsage(ctx, key)
	}
	_, _ = pipe.Exec(ctx)

	out := make([]KeyInfo, 0, len(keys))
	for i, key := range keys {
		if !IsStudentCacheKey(key) {
			continue
		}
		ki := KeyInfo{Key: key}
		if typeCmds[i] != nil {
			ki.Type = typeCmds[i].Val()
		}
		if ttlCmds[i] != nil {
			ttl := ttlCmds[i].Val()
			switch {
			case ttl == -1:
				ki.TTL = -1
			case ttl == -2:
				ki.TTL = -2
			default:
				ki.TTL = int64(ttl.Seconds())
			}
		}
		if memCmds[i] != nil {
			if mem, err := memCmds[i].Result(); err == nil {
				ki.Memory = mem
			}
		}
		out = append(out, ki)
	}
	return out, nil
}

func (d *RedisDriver) GetKeyValue(ctx context.Context, key string) (KeyValue, error) {
	typ, err := d.client.Type(ctx, key).Result()
	if err != nil {
		return KeyValue{}, err
	}
	ttlDur, err := d.client.TTL(ctx, key).Result()
	if err != nil {
		return KeyValue{}, err
	}
	ttl := int64(-1)
	switch {
	case ttlDur == -1:
		ttl = -1
	case ttlDur == -2:
		ttl = -2
	default:
		ttl = int64(ttlDur.Seconds())
	}

	kv := KeyValue{Key: key, Type: typ, TTL: ttl}
	switch typ {
	case "string":
		kv.Value, err = d.client.Get(ctx, key).Result()
	case "list":
		kv.Value, err = d.client.LRange(ctx, key, 0, 99).Result()
	case "set":
		var members []string
		members, err = d.client.SMembers(ctx, key).Result()
		if err == nil && len(members) > 100 {
			members = members[:100]
		}
		kv.Value = members
	case "zset":
		z, zerr := d.client.ZRevRangeWithScores(ctx, key, 0, 99).Result()
		err = zerr
		members := make([]map[string]string, 0, len(z))
		for _, z := range z {
			members = append(members, map[string]string{
				"member": fmt.Sprint(z.Member),
				"score":  strconv.FormatFloat(z.Score, 'f', -1, 64),
			})
		}
		kv.Value = members
	case "hash":
		kv.Value, err = d.client.HGetAll(ctx, key).Result()
	case "stream":
		var entries []StreamEntry
		entries, err = readStreamPreview(ctx, d.client, key)
		kv.Value = entries
	default:
		kv.Value = nil
	}
	if err != nil {
		return KeyValue{}, err
	}
	return kv, nil
}

// streamPreviewLimit is how many recent stream entries the cache browser returns.
const streamPreviewLimit = 20

// StreamEntry is one Redis stream record for the cache data browser.
// Fields keeps the XADD map (work queues store the payload under "body").
type StreamEntry struct {
	ID     string            `json:"id"`
	Fields map[string]string `json:"fields"`
}

// readStreamPreview returns the newest stream entries, oldest first.
// XRevRangeN avoids loading the whole stream.
func readStreamPreview(ctx context.Context, client *redis.Client, key string) ([]StreamEntry, error) {
	msgs, err := client.XRevRangeN(ctx, key, "+", "-", streamPreviewLimit).Result()
	if err != nil {
		return nil, fmt.Errorf("redis xrange: %w", err)
	}
	entries := make([]StreamEntry, len(msgs))
	for i, msg := range msgs {
		fields := make(map[string]string, len(msg.Values))
		for k, v := range msg.Values {
			fields[k] = fmt.Sprint(v)
		}
		entries[len(msgs)-1-i] = StreamEntry{ID: msg.ID, Fields: fields}
	}
	return entries, nil
}
