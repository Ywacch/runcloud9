package timeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// DefaultMaxEntries matches social-feed and fanout-worker timeline caps before E-7.
	DefaultMaxEntries = 1000
	// DefaultKeyPrefix is the Redis key prefix for follower timelines (timeline:{userID}).
	DefaultKeyPrefix = "timeline:"
)

const redisEngine = "redis"

// RedisOptions configures a sorted-set timeline store.
//
// Key format: {KeyPrefix}{userID}, e.g. timeline:42.
// Member: UTF-8 JSON payload bytes (unchanged from pre-refactor images).
// Score: CreatedAt as Unix milliseconds (float64).
// Trim: ZREMRANGEBYRANK key 0 -(MaxEntries+1) after each append (same as appendTimelineWrite).
// TTL: EXPIRE after append when TTL > 0; zero leaves keys persistent for the pod lifetime.
type RedisOptions struct {
	MaxEntries int
	TTL        time.Duration
	KeyPrefix  string
}

type redisStore struct {
	client     redis.UniversalClient
	maxEntries int
	ttl        time.Duration
	prefix     string
}

// NewRedis returns a sorted-set timeline backed by client.
func NewRedis(client redis.UniversalClient, opts RedisOptions) Store {
	max := opts.MaxEntries
	if max < 1 {
		max = DefaultMaxEntries
	}
	prefix := NormalizeKeyPrefix(opts.KeyPrefix)
	return &redisStore{
		client:     client,
		maxEntries: max,
		ttl:        opts.TTL,
		prefix:     prefix,
	}
}

func (r *redisStore) Engine() string {
	return redisEngine
}

func (r *redisStore) Append(ctx context.Context, userIDs []int64, item Item) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(userIDs) == 0 {
		return nil
	}
	member, score, err := redisMemberAndScore(item)
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	for _, uid := range userIDs {
		key := r.userKey(uid)
		pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: member})
		pipe.ZRemRangeByRank(ctx, key, 0, int64(-(r.maxEntries + 1)))
		if r.ttl > 0 {
			pipe.Expire(ctx, key, r.ttl)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("timeline redis append: %w", err)
	}
	return nil
}

func (r *redisStore) Range(ctx context.Context, userID int64, limit int) ([]Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	key := r.userKey(userID)
	raw, err := r.client.ZRevRangeWithScores(ctx, key, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("timeline redis range: %w", err)
	}
	out := make([]Item, 0, len(raw))
	for _, z := range raw {
		it, err := itemFromRedisMember(z.Member, z.Score)
		if err != nil {
			continue
		}
		out = append(out, it)
	}
	sortItems(out)
	return out, nil
}

func (r *redisStore) Trim(ctx context.Context, userID int64, keep int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if keep <= 0 {
		return ErrInvalidLimit
	}
	key := r.userKey(userID)
	if err := r.client.ZRemRangeByRank(ctx, key, 0, int64(-(keep + 1))).Err(); err != nil {
		return fmt.Errorf("timeline redis trim: %w", err)
	}
	return nil
}

func (r *redisStore) userKey(userID int64) string {
	return fmt.Sprintf("%s%d", r.prefix, userID)
}

func redisMemberAndScore(item Item) (member string, score float64, err error) {
	payload := item.Payload
	if len(payload) == 0 {
		payload, err = json.Marshal(map[string]any{
			"post_id":    item.PostID,
			"author_id":  item.AuthorID,
			"created_at": item.CreatedAt,
		})
		if err != nil {
			return "", 0, fmt.Errorf("timeline redis marshal payload: %w", err)
		}
	}
	return string(payload), float64(item.CreatedAt.UnixMilli()), nil
}

func itemFromRedisMember(member any, score float64) (Item, error) {
	raw, ok := member.(string)
	if !ok {
		return Item{}, fmt.Errorf("timeline redis member type %T", member)
	}
	payload := []byte(raw)
	var fields struct {
		PostID   int64 `json:"post_id"`
		AuthorID int64 `json:"author_id"`
		UserID   int64 `json:"user_id"`
	}
	_ = json.Unmarshal(payload, &fields)
	authorID := fields.AuthorID
	if authorID == 0 {
		authorID = fields.UserID
	}
	createdAt := time.UnixMilli(int64(score)).UTC()
	return Item{
		PostID:    fields.PostID,
		AuthorID:  authorID,
		Payload:   append([]byte(nil), payload...),
		CreatedAt: createdAt,
	}, nil
}

// MemberStringForTest exposes the Redis member string for golden tests.
func MemberStringForTest(item Item) (string, float64, error) {
	return redisMemberAndScore(item)
}

// NormalizeKeyPrefix ensures a trailing colon when the prefix is non-empty.
func NormalizeKeyPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return DefaultKeyPrefix
	}
	if !strings.HasSuffix(prefix, ":") {
		return prefix + ":"
	}
	return prefix
}
