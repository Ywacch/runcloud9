package loadgen

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestBuildPodStartBody_identityCap(t *testing.T) {
	makeServer := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seg := r.URL.Query().Get("segment")
			limit := 50
			if l := r.URL.Query().Get("limit"); l != "" {
				if n, err := strconv.Atoi(l); err == nil {
					limit = n
				}
			}
			var ids []int
			switch seg {
			case "celebrity":
				for i := 1; i <= 10; i++ {
					ids = append(ids, i)
				}
			case "normal":
				end := 10 + limit
				if end > 60 {
					end = 60
				}
				for i := 11; i <= end; i++ {
					ids = append(ids, i)
				}
			default:
				n := limit
				if n > 5000 {
					n = 5000
				}
				for i := 1; i <= n; i++ {
					ids = append(ids, i)
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"userIds": ids})
		}))
	}

	t.Run("social-feed N500", func(t *testing.T) {
		srv := makeServer()
		defer srv.Close()
		t.Setenv("LOADGEN_BASE_URL", srv.URL)

		body, err := BuildPodStartBody(t.Context(), "ns", NormalizeProfile(LoadProfile{
			Template:    "social-feed",
			RPS:         10,
			BaseURL:     srv.URL,
			ActiveUsers: 500,
		}))
		if err != nil {
			t.Fatal(err)
		}
		if body.IdentityCount != 500 {
			t.Fatalf("IdentityCount: got %d want 500", body.IdentityCount)
		}
		if body.ActiveUsers != 0 || body.Actor.ActiveUsers != 0 {
			t.Fatalf("ActiveUsers: body=%d actor=%d", body.ActiveUsers, body.Actor.ActiveUsers)
		}
	})

	t.Run("social-feed N0", func(t *testing.T) {
		srv := makeServer()
		defer srv.Close()
		t.Setenv("LOADGEN_BASE_URL", srv.URL)

		body, err := BuildPodStartBody(t.Context(), "ns", NormalizeProfile(LoadProfile{
			Template: "social-feed",
			RPS:      10,
			BaseURL:  srv.URL,
		}))
		if err != nil {
			t.Fatal(err)
		}
		if body.IdentityCount != 60 {
			t.Fatalf("IdentityCount: got %d want 60", body.IdentityCount)
		}
	})

	t.Run("ecommerce N5", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/product-pool" {
				ids := make([]int, 20)
				for i := range ids {
					ids[i] = i + 1
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"productIds": ids})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"userIds": []int{1}})
		}))
		defer srv.Close()
		t.Setenv("LOADGEN_BASE_URL", srv.URL)

		body, err := BuildPodStartBody(t.Context(), "ns", NormalizeProfile(LoadProfile{
			Template:    "ecommerce",
			RPS:         10,
			BaseURL:     srv.URL,
			ActiveUsers: 5,
		}))
		if err != nil {
			t.Fatal(err)
		}
		if body.IdentityCount != 0 {
			t.Fatalf("IdentityCount: got %d want 0", body.IdentityCount)
		}
		if len(body.Actor.Pools["productId"]) != 20 {
			t.Fatalf("product pool capped: %d", len(body.Actor.Pools["productId"]))
		}
	})
}
