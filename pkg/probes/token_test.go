package probes

import "testing"

func TestInt64Token_GTE(t *testing.T) {
	t.Parallel()
	if !(Int64Token(5).GTE(Int64Token(3))) {
		t.Fatal("5 should be GTE 3")
	}
	if Int64Token(2).GTE(Int64Token(5)) {
		t.Fatal("2 should not be GTE 5")
	}
}

func TestTokenBehind(t *testing.T) {
	t.Parallel()
	written := Int64Token(10)
	if !TokenBehind(written, Int64Token(7)) {
		t.Fatal("expected stale cache hit")
	}
	if TokenBehind(written, Int64Token(10)) {
		t.Fatal("equal versions are not stale")
	}
	if TokenBehind(written, Int64Token(11)) {
		t.Fatal("newer cache is not stale")
	}
}

func TestParseInt64Token(t *testing.T) {
	t.Parallel()
	tok, err := ParseInt64Token("42")
	if err != nil {
		t.Fatal(err)
	}
	if tok != 42 {
		t.Fatalf("got %d", tok)
	}
	if _, err := ParseInt64Token("nope"); err == nil {
		t.Fatal("expected parse error")
	}
}
