package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAppendTimelineWrite_TTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wantTTL time.Duration // miniredis reports 0 for persistent keys
	}{
		{name: "zero TTL leaves timeline persistent", ttl: 0, wantTTL: 0},
		{name: "positive TTL sets expiry", ttl: 45 * time.Second, wantTTL: 45 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(srv.Close)

			rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
			t.Cleanup(func() { _ = rdb.Close() })

			ctx := context.Background()
			followerID := 3
			pipe := rdb.Pipeline()
			appendTimelineWrite(ctx, pipe, followerID, []byte(`{"post_id":1}`), time.Unix(100, 0).UTC(), tt.ttl)
			if _, err := pipe.Exec(ctx); err != nil {
				t.Fatalf("pipeline exec: %v", err)
			}

			key := fmt.Sprintf("timeline:%d", followerID)
			if !srv.Exists(key) {
				t.Fatalf("expected %s to exist", key)
			}
			if got := srv.TTL(key); got != tt.wantTTL {
				t.Fatalf("TTL(%s) = %v, want %v", key, got, tt.wantTTL)
			}
		})
	}
}

func TestParseCacheTTLSeconds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		def  int
		want int
	}{
		{name: "absent uses default", raw: "", def: 0, want: 0},
		{name: "zero means no expiry", raw: "0", def: 0, want: 0},
		{name: "timed expiry", raw: "300", def: 0, want: 300},
		{name: "negative uses default", raw: "-1", def: 0, want: 0},
		{name: "malformed uses default", raw: "abc", def: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseCacheTTLSeconds(tt.raw, tt.def); got != tt.want {
				t.Fatalf("parseCacheTTLSeconds(%q, %d) = %d, want %d", tt.raw, tt.def, got, tt.want)
			}
		})
	}
}
