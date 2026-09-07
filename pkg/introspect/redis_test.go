package introspect

import (
	"slices"
	"testing"
)

func TestRedisCapabilities(t *testing.T) {
	t.Parallel()
	d := &RedisDriver{}
	caps := d.Capabilities()
	for _, want := range []Capability{CapKeys, CapKeyValue, CapStats} {
		if !slices.Contains(caps, want) {
			t.Fatalf("capabilities %v missing %q", caps, want)
		}
	}
}
