package metrics

import "testing"

func TestParseNginxActiveConnections(t *testing.T) {
	t.Parallel()
	sample := `Active connections: 7 
server accepts handled requests
 10 10 100 
Reading: 0 Writing: 1 Waiting: 4 
`
	if got := parseNginxActiveConnections(sample); got != 7 {
		t.Fatalf("want 7, got %v", got)
	}
	if got := parseNginxActiveConnections("nope"); got != 0 {
		t.Fatalf("invalid body: want 0, got %v", got)
	}
}
