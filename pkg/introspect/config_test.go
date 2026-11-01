package introspect

import "testing"

func TestAppendQueueStreamKeys(t *testing.T) {
	t.Parallel()
	got := AppendQueueStreamKeys([]string{"timeline:*"}, []string{" fanout ", "", "fanout", "orders"})
	want := []string{"timeline:*", "fanout", "orders"}
	if len(got) != len(want) {
		t.Fatalf("len %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	original := []string{"timeline:*"}
	_ = AppendQueueStreamKeys(original, []string{"fanout"})
	if len(original) != 1 || original[0] != "timeline:*" {
		t.Fatalf("patterns mutated: %v", original)
	}
}
