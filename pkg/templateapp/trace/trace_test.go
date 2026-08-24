package trace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestContextFromRequest_ClientID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
		value  string
		want   string
	}{
		{name: "xff first hop", header: "X-Forwarded-For", value: "203.0.113.7, 10.0.0.1", want: "203.0.113.7"},
		{name: "real ip", header: "X-Real-IP", value: "198.51.100.2", want: "198.51.100.2"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "http://example.com/r/abc", nil)
			req.Header.Set(tc.header, tc.value)
			req.RemoteAddr = "127.0.0.1:1234"
			req = ContextFromRequest(req)
			if got := ClientID(req.Context()); got != tc.want {
				t.Fatalf("ClientID() = %q want %q", got, tc.want)
			}
		})
	}
}

func TestClientID_remoteAddrFallback(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.RemoteAddr = "192.0.2.1:4242"
	req = ContextFromRequest(req)
	if got := ClientID(req.Context()); got != "192.0.2.1" {
		t.Fatalf("ClientID() = %q want 192.0.2.1", got)
	}
}

func TestWithIDs_doesNotSetClientID(t *testing.T) {
	t.Parallel()
	ctx := WithIDs(context.Background(), "req-1", true)
	if got := ClientID(ctx); got != "" {
		t.Fatalf("ClientID() = %q want empty", got)
	}
}
