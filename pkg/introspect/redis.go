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
	return []Capability{CapKeys, CapKeyValue}
}

func (d *RedisDriver) Close() error {
	if d.client == nil {
		return nil
	}
	return d.client.Close()
}

func (d *RedisDriver) ScanKeys(ctx context.Context, pattern string, cursor uint64, count int) ([]KeyInfo, uint64, error) {
	if count < 1 {
		count = 50
	}
	if count > 100 {
		count = 100
	}
	keys, next, err := d.client.Scan(ctx, cursor, pattern, int64(count)).Result()
	if err != nil {
		return nil, 0, err
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
	return out, next, nil
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
		msgs, serr := d.client.XRange(ctx, key, "-", "+").Result()
		err = serr
		if len(msgs) > 20 {
			msgs = msgs[len(msgs)-20:]
		}
		kv.Value = msgs
	default:
		kv.Value = nil
	}
	if err != nil {
		return KeyValue{}, err
	}
	return kv, nil
}
