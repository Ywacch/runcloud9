package chaos

import (
	"strings"
	"testing"
)

func TestCacheFlushCommand(t *testing.T) {
	tests := []struct {
		engine    string
		want      []string
		wantErr   bool
		errSubstr string
	}{
		{engine: "redis", want: []string{"redis-cli", "FLUSHALL"}},
		{engine: "Redis", want: []string{"redis-cli", "FLUSHALL"}},
		{engine: "memcached", want: []string{"sh", "-c", "printf 'flush_all\\r\\n' | nc -q 1 127.0.0.1 11211"}},
		{engine: "scylla", wantErr: true, errSubstr: "supported: redis"},
	}

	for _, tc := range tests {
		t.Run(tc.engine, func(t *testing.T) {
			got, err := cacheFlushCommand(tc.engine)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("error %q should contain %q", err.Error(), tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("cacheFlushCommand: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v want %v", got, tc.want)
				}
			}
		})
	}
}
