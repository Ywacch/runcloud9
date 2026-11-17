package templates

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

const metadataTokenURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

// manifestAccept lists the manifest types Artifact Registry actually stores.
// A narrower Accept makes the registry return 404 for an existing OCI index.
const manifestAccept = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

// publishedImageNames are the template images this repo builds and pushes.
// ticket-drop is pinned in the manifest but published from another repository.
var publishedImageNames = []string{
	"caching-proxy",
	"ecommerce",
	"ecommerce-worker",
	"fanout-worker",
	"loadgen",
	"social-feed",
	"url-shortener",
}

// PublishedImageTags returns the tags this process will pull for in-repo images.
// It fails when the manifest has no tag for one of those images.
func PublishedImageTags(tags map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(publishedImageNames))
	var missing []string
	for _, name := range publishedImageNames {
		tag := strings.TrimSpace(tags[name])
		if tag == "" {
			missing = append(missing, name)
			continue
		}
		out[name] = tag
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("template images: manifest missing %s", strings.Join(missing, ", "))
	}
	return out, nil
}

// VerifyPublishedTags checks that every manifest tag exists in Artifact Registry.
// registry is host/project/repo with no scheme, for example
// us-central1-docker.pkg.dev/runcloud9-514803/cloud9-images.
// The call uses the GCE metadata server for a bearer token.
func VerifyPublishedTags(ctx context.Context, registry string, tags map[string]string) error {
	client := &http.Client{Timeout: 10 * time.Second}
	return verifyPublishedTags(ctx, registry, tags, client, gcpAccessToken)
}

func verifyPublishedTags(ctx context.Context, registry string, tags map[string]string, client *http.Client, tokenFn func(context.Context, *http.Client) (string, error)) error {
	if len(tags) == 0 {
		return nil
	}
	if client == nil {
		return fmt.Errorf("template images: http client is required")
	}
	if tokenFn == nil {
		return fmt.Errorf("template images: token lookup is required")
	}
	token, err := tokenFn(ctx, client)
	if err != nil {
		return fmt.Errorf("template images: access token: %w", err)
	}

	names := make([]string, 0, len(tags))
	for name := range tags {
		names = append(names, name)
	}
	sort.Strings(names)

	var missing []string
	for _, name := range names {
		tag := strings.TrimSpace(tags[name])
		if tag == "" {
			continue
		}
		ref := strings.Trim(registry, "/") + "/" + name + ":" + tag
		url, err := manifestURL(registry, name, tag)
		if err != nil {
			return err
		}
		status, err := tagStatus(ctx, client, token, url)
		if err != nil {
			return fmt.Errorf("template images: check %s: %w", ref, err)
		}
		switch status {
		case http.StatusOK:
		case http.StatusNotFound:
			missing = append(missing, ref)
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("template images: registry denied %s (HTTP %d)", ref, status)
		default:
			return fmt.Errorf("template images: registry returned HTTP %d for %s", status, ref)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("template image tags are not in the registry: %s", strings.Join(missing, ", "))
	}
	return nil
}

func manifestURL(registry, name, tag string) (string, error) {
	registry = strings.TrimSpace(registry)
	scheme := "https"
	switch {
	case strings.HasPrefix(registry, "https://"):
		registry = strings.TrimPrefix(registry, "https://")
	case strings.HasPrefix(registry, "http://"):
		scheme = "http"
		registry = strings.TrimPrefix(registry, "http://")
	}
	registry = strings.Trim(registry, "/")
	host, repo, ok := strings.Cut(registry, "/")
	if !ok || host == "" || repo == "" || strings.Contains(host, "://") {
		return "", fmt.Errorf("template images: registry %q must be host/project/repo", registry)
	}
	if name == "" || tag == "" || strings.Contains(name, "/") || strings.Contains(tag, "/") {
		return "", fmt.Errorf("template images: invalid image name %q tag %q", name, tag)
	}
	return scheme + "://" + host + "/v2/" + repo + "/" + name + "/manifests/" + tag, nil
}

func tagStatus(ctx context.Context, client *http.Client, token, manifestURL string) (int, error) {
	status, err := tagRequest(ctx, client, token, manifestURL, manifestAccept)
	if err != nil || status != http.StatusNotFound {
		return status, err
	}
	// Artifact Registry returns 404 when Accept omits the stored manifest type.
	// A second request with */* distinguishes "tag missing" from "wrong Accept".
	return tagRequest(ctx, client, token, manifestURL, "*/*")
}

func tagRequest(ctx context.Context, client *http.Client, token, manifestURL, accept string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", accept)
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Status is the result. Drain a short prefix so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}

func gcpAccessToken(ctx context.Context, client *http.Client) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server HTTP %d", resp.StatusCode)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("parse metadata token: %w", err)
	}
	if payload.AccessToken == "" {
		return "", fmt.Errorf("metadata token is empty")
	}
	return payload.AccessToken, nil
}
