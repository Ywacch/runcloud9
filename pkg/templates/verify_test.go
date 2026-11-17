package templates

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublishedImageTags(t *testing.T) {
	tags, err := PublishedImageTags(map[string]string{
		"caching-proxy":    "git-a",
		"ecommerce":        "git-a",
		"ecommerce-worker": "git-a",
		"fanout-worker":    "git-a",
		"loadgen":          "git-a",
		"social-feed":      "git-a",
		"url-shortener":    "git-a",
		"ticket-drop":      "git-other",
	})
	if err != nil {
		t.Fatal(err)
	}
	if tags["ticket-drop"] != "" {
		t.Fatalf("ticket-drop should not be checked here: %+v", tags)
	}
	if _, err := PublishedImageTags(map[string]string{"loadgen": "git-a"}); err == nil {
		t.Fatal("expected missing manifest keys")
	}
}

func TestVerifyPublishedTags(t *testing.T) {
	const token = "test-token"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "auth", http.StatusUnauthorized)
			return
		}
		if !strings.Contains(r.Header.Get("Accept"), "application/vnd.oci.image.index.v1+json") {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Path {
		case "/v2/proj/repo/loadgen/manifests/git-ok":
			w.WriteHeader(http.StatusOK)
		case "/v2/proj/repo/url-shortener/manifests/git-missing":
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	registry := srv.URL + "/proj/repo"
	client := srv.Client()
	ctx := context.Background()
	tokenFn := func(context.Context, *http.Client) (string, error) { return token, nil }

	err := verifyPublishedTags(ctx, registry, map[string]string{
		"loadgen":       "git-ok",
		"url-shortener": "git-missing",
	}, client, tokenFn)
	if err == nil {
		t.Fatal("expected missing tag error")
	}
	if !strings.Contains(err.Error(), "url-shortener:git-missing") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), "loadgen:git-ok") {
		t.Fatalf("present tag listed as missing: %v", err)
	}

	if err := verifyPublishedTags(ctx, registry, map[string]string{"loadgen": "git-ok"}, client, tokenFn); err != nil {
		t.Fatalf("present tag: %v", err)
	}
}

func TestVerifyPublishedTags_RetriesWhenAcceptIsRejected(t *testing.T) {
	const token = "test-token"
	var accepts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accepts = append(accepts, r.Header.Get("Accept"))
		if r.Header.Get("Accept") != "*/*" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	err := verifyPublishedTags(context.Background(), srv.URL+"/proj/repo", map[string]string{
		"loadgen": "git-ok",
	}, srv.Client(), func(context.Context, *http.Client) (string, error) { return token, nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(accepts) != 2 || accepts[1] != "*/*" {
		t.Fatalf("accept sequence = %#v", accepts)
	}
}

func TestManifestURL(t *testing.T) {
	got, err := manifestURL("us-central1-docker.pkg.dev/proj/repo", "loadgen", "git-abc")
	if err != nil {
		t.Fatal(err)
	}
	want := "https://us-central1-docker.pkg.dev/v2/proj/repo/loadgen/manifests/git-abc"
	if got != want {
		t.Fatalf("got %s", got)
	}
	if _, err := manifestURL("not-a-registry", "loadgen", "git-abc"); err == nil {
		t.Fatal("expected registry shape error")
	}
}
