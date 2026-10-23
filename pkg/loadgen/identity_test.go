package loadgen

import (
	"strconv"
	"testing"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func TestCapIdentityUniverse(t *testing.T) {
	t.Parallel()

	flat := intsToStr(1, 50)
	celebrity := intsToStr(1, 10)
	normal := intsToStr(11, 60)
	pools := [][]string{flat, celebrity, normal}

	t.Run("n0 unchanged union 60", func(t *testing.T) {
		t.Parallel()
		capped, allowed := capIdentityUniverse(pools, 0)
		if len(allowed) != 60 {
			t.Fatalf("allowed: got %d want 60", len(allowed))
		}
		if len(capped[0]) != 50 || len(capped[1]) != 10 || len(capped[2]) != 50 {
			t.Fatalf("capped lengths: %d %d %d", len(capped[0]), len(capped[1]), len(capped[2]))
		}
	})

	t.Run("n5", func(t *testing.T) {
		t.Parallel()
		_, allowed := capIdentityUniverse(pools, 5)
		if len(allowed) != 5 || allowed[0] != "1" || allowed[4] != "5" {
			t.Fatalf("allowed: %v", allowed)
		}
		capped, _ := capIdentityUniverse(pools, 5)
		if len(capped[1]) != 5 {
			t.Fatalf("celebrity capped: %v", capped[1])
		}
		if len(capped[2]) != 0 {
			t.Fatalf("normal should be empty: %v", capped[2])
		}
	})

	t.Run("n500 union 60", func(t *testing.T) {
		t.Parallel()
		_, allowed := capIdentityUniverse(pools, 500)
		if len(allowed) != 60 {
			t.Fatalf("allowed: got %d", len(allowed))
		}
	})

	t.Run("dedupe", func(t *testing.T) {
		t.Parallel()
		dup := [][]string{{"1", "2"}, {"2", "3"}}
		_, allowed := capIdentityUniverse(dup, 0)
		if len(allowed) != 3 {
			t.Fatalf("allowed: %v", allowed)
		}
	})

	t.Run("string sort", func(t *testing.T) {
		t.Parallel()
		p := [][]string{{"b", "a2", "a10"}}
		_, allowed := capIdentityUniverse(p, 0)
		if len(allowed) != 3 {
			t.Fatalf("allowed: %v", allowed)
		}
		// non-numeric → lexicographic
		if allowed[0] != "a10" || allowed[1] != "a2" || allowed[2] != "b" {
			t.Fatalf("order: %v", allowed)
		}
	})
}

func TestWithPoolLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		endpoint string
		n        int
		want     string
	}{
		{"/user-pool", 500, "/user-pool?limit=500"},
		{"/user-pool?segment=normal", 500, "/user-pool?limit=500&segment=normal"},
		{"/user-pool", 0, "/user-pool"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := withPoolLimit(tc.endpoint, tc.n); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestClientIDCounter(t *testing.T) {
	t.Parallel()
	c := NewClientIDCounter(3)
	for _, id := range []string{"a", "b", "a", "c", "d"} {
		c.record(id)
	}
	if c.Count() != 3 {
		t.Fatalf("count: got %d want 3", c.Count())
	}
}

func TestCountClientIDs(t *testing.T) {
	t.Parallel()
	c := NewClientIDCounter(100)
	inner := vegeta.Targeter(func(tgt *vegeta.Target) error {
		tgt.Header = make(map[string][]string)
		tgt.Header.Set("X-C9-Client-ID", "u1")
		return nil
	})
	wrapped := CountClientIDs(inner, c)
	var tgt vegeta.Target
	if err := wrapped(&tgt); err != nil {
		t.Fatal(err)
	}
	if c.Count() != 1 {
		t.Fatalf("count: %d", c.Count())
	}
}

func intsToStr(from, to int) []string {
	out := make([]string, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, strconv.Itoa(i))
	}
	return out
}
