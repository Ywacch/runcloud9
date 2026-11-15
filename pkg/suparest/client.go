// Package suparest builds a PostgREST client for the Cloud9 API.
//
// Legacy service_role keys are JWTs. The client sends them on apikey and
// Authorization: Bearer.
//
// Publishable and secret keys (sb_publishable_, sb_secret_) are not JWTs.
// Supabase rejects them when they also appear on Authorization: Bearer.
// Those keys are sent on apikey only.
package suparest

import (
	"fmt"
	"strings"

	postgrest "github.com/supabase-community/postgrest-go"
)

// New returns a PostgREST client for projectURL using key.
func New(projectURL, key string) (*postgrest.Client, error) {
	projectURL = strings.TrimRight(strings.TrimSpace(projectURL), "/")
	key = strings.TrimSpace(key)
	if projectURL == "" || key == "" {
		return nil, fmt.Errorf("suparest: url and key are required")
	}
	client := postgrest.NewClient(projectURL+"/rest/v1", "public", headers(key))
	if client.ClientError != nil {
		return nil, fmt.Errorf("suparest: %w", client.ClientError)
	}
	return client, nil
}

func headers(key string) map[string]string {
	h := map[string]string{"apikey": key}
	if strings.HasPrefix(key, "sb_") {
		return h
	}
	h["Authorization"] = "Bearer " + key
	return h
}
