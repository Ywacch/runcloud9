package loadgen

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func TestParsePoolValues_StringArray(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage(`["1","2","3"]`)
	got, ok := parsePoolValues(raw)
	if !ok {
		t.Fatal("expected parsePoolValues to succeed")
	}
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("len: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

func TestParsePoolValues_NumericArray(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage(`[1,2,3]`)
	got, ok := parsePoolValues(raw)
	if !ok {
		t.Fatal("expected parsePoolValues to succeed")
	}
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("len: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

func TestFetchPoolOnce_MixedTypePoolValues(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/product-pool" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"productIds":[1,2,3]}`))
	}))
	defer srv.Close()

	got := fetchPoolOnce(context.Background(), srv.URL, "/product-pool", "productIds")
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("len: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

func TestBuildTargeter_EmptyPoolUsesNumericFallbackForProductID(t *testing.T) {
	t.Parallel()
	specs := []EndpointSpec{
		{
			Method:       "GET",
			PathTemplate: "/products/{productId}",
			ParamName:    "productId",
			Weight:       1,
		},
	}
	targeter := buildTargeter("http://example.test", specs, map[string][]string{})
	var tgt vegeta.Target
	if err := targeter(&tgt); err != nil {
		t.Fatalf("targeter error: %v", err)
	}
	if !strings.HasPrefix(tgt.URL, "http://example.test/products/") {
		t.Fatalf("unexpected URL: %s", tgt.URL)
	}
	id := strings.TrimPrefix(tgt.URL, "http://example.test/products/")
	if id == "" {
		t.Fatal("expected non-empty product id")
	}
	for _, ch := range id {
		if ch < '0' || ch > '9' {
			t.Fatalf("expected numeric product id, got %q in %q", ch, id)
		}
	}
}

func TestBuildTargeter_EmptyPoolCheckoutBodyReplacesParam(t *testing.T) {
	t.Parallel()
	specs := []EndpointSpec{
		{
			Method:       "POST",
			PathTemplate: "/checkout",
			ParamName:    "productId",
			BodyTemplate: `{"product_id":{{.Param}}, "quantity":1}`,
			Weight:       1,
		},
	}
	targeter := buildTargeter("http://example.test", specs, map[string][]string{})
	var tgt vegeta.Target
	if err := targeter(&tgt); err != nil {
		t.Fatalf("targeter error: %v", err)
	}
	if strings.Contains(string(tgt.Body), "{{.Param}}") {
		t.Fatalf("placeholder was not replaced: %s", string(tgt.Body))
	}
	var decoded map[string]any
	if err := json.Unmarshal(tgt.Body, &decoded); err != nil {
		t.Fatalf("checkout body should be valid JSON: %v body=%s", err, string(tgt.Body))
	}
	if _, ok := decoded["product_id"]; !ok {
		t.Fatalf("checkout body missing product_id: %s", string(tgt.Body))
	}
}

func TestBuildTargeter_SetsXRequestIDHeader(t *testing.T) {
	t.Parallel()
	specs := []EndpointSpec{
		{Method: "GET", PathTemplate: "/health", Weight: 1},
	}
	targeter := buildTargeter("http://example.test", specs, map[string][]string{})
	var a, b vegeta.Target
	if err := targeter(&a); err != nil {
		t.Fatalf("targeter: %v", err)
	}
	if err := targeter(&b); err != nil {
		t.Fatalf("targeter: %v", err)
	}
	rida := a.Header.Get("X-Request-ID")
	ridb := b.Header.Get("X-Request-ID")
	if rida == "" || ridb == "" {
		t.Fatalf("want X-Request-ID on target, got a=%q b=%q", rida, ridb)
	}
	if rida == ridb {
		t.Fatalf("expected unique IDs per request, got duplicate %q", rida)
	}
}

func TestLocalLoadGenerator_UpdateNotRunning(t *testing.T) {
	t.Parallel()
	lg := NewLocalLoadGenerator()
	rps := 20
	err := lg.Update(context.Background(), "ns-none", LiveUpdate{RPS: &rps})
	if err != ErrNotRunning {
		t.Fatalf("got %v want ErrNotRunning", err)
	}
}

func TestLocalLoadGenerator_UpdateRunning(t *testing.T) {
	t.Parallel()
	lg := NewLocalLoadGenerator()
	lg.mu.Lock()
	lg.active["ns-live"] = &attackSession{control: NewAttackControl(10)}
	lg.mu.Unlock()

	rps := 40
	if err := lg.Update(context.Background(), "ns-live", LiveUpdate{RPS: &rps}); err != nil {
		t.Fatal(err)
	}
	lg.mu.Lock()
	got := lg.active["ns-live"].control.BaseRPS()
	lg.mu.Unlock()
	if got != 40 {
		t.Fatalf("base rps: got %d want 40", got)
	}
}

func TestLocalLoadGenerator_UpdateMixRunning(t *testing.T) {
	t.Parallel()
	lg := NewLocalLoadGenerator()
	control := NewAttackControl(10)
	control.SetBaseDistribution(map[string]int{
		"GET /r/{code}": 80,
		"POST /shorten": 20,
	})
	lg.mu.Lock()
	lg.active["ns-mix"] = &attackSession{
		control: control,
		profile: LoadProfile{Template: "url-shortener"},
	}
	lg.mu.Unlock()

	mix := map[string]int{
		"GET /r/{code}": 50,
		"POST /shorten": 50,
	}
	if err := lg.Update(context.Background(), "ns-mix", LiveUpdate{Distribution: mix}); err != nil {
		t.Fatal(err)
	}
	lg.mu.Lock()
	got := lg.active["ns-mix"].control.BaseDistribution()
	gen := lg.active["ns-mix"].control.MixGeneration()
	lg.mu.Unlock()
	if got["GET /r/{code}"] != 50 || got["POST /shorten"] != 50 {
		t.Fatalf("base distribution: got %+v", got)
	}
	if gen == 0 {
		t.Fatal("expected mix generation bump")
	}
}

func TestValidateLiveDistribution(t *testing.T) {
	t.Parallel()
	valid := map[string]int{
		"GET /r/{code}": 80,
		"POST /shorten": 20,
	}
	if err := ValidateLiveDistribution("url-shortener", valid); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLiveDistribution("url-shortener", map[string]int{"GET /r/{code}": 0, "POST /shorten": 0}); err == nil {
		t.Fatal("expected error for all-zero weights")
	}
	if err := ValidateLiveDistribution("url-shortener", map[string]int{"GET /r/{code}": 100}); err == nil {
		t.Fatal("expected error for missing endpoint key")
	}
	if err := ValidateLiveDistribution("url-shortener", map[string]int{
		"GET /r/{code}": 80,
		"POST /shorten": 20,
		"GET /nope":     1,
	}); err == nil {
		t.Fatal("expected error for unknown key")
	}
}
