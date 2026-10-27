package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
)

const (
	headerBrowserActAs = "X-C9-Act-As"
	actAsUpstreamValue = "1"
	actAsUserIDMaxLen  = 10
)

var errUnknownActAs = errors.New("unknown act-as user")

type userPoolFetcher interface {
	FetchUserPool(ctx context.Context, namespace string) ([]string, error)
}

type poolEntry struct {
	ids   []string
	until time.Time
}

type actAsValidator struct {
	fetch userPoolFetcher
	now   func() time.Time
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]poolEntry
}

func newActAsValidator(fetch userPoolFetcher, now func() time.Time, ttl time.Duration) *actAsValidator {
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &actAsValidator{
		fetch: fetch,
		now:   now,
		ttl:   ttl,
		cache: make(map[string]poolEntry),
	}
}

func (v *actAsValidator) Validate(ctx context.Context, namespace, userID string) error {
	if v == nil || v.fetch == nil {
		return fmt.Errorf("act-as validator unavailable")
	}
	if namespace == "" || userID == "" {
		return errUnknownActAs
	}
	ids, err := v.poolIDs(ctx, namespace)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id == userID {
			return nil
		}
	}
	return errUnknownActAs
}

func (v *actAsValidator) Forget(namespace string) {
	if v == nil || namespace == "" {
		return
	}
	v.mu.Lock()
	delete(v.cache, namespace)
	v.mu.Unlock()
}

func (v *actAsValidator) poolIDs(ctx context.Context, namespace string) ([]string, error) {
	now := v.now()
	v.mu.Lock()
	if ent, ok := v.cache[namespace]; ok && now.Before(ent.until) {
		ids := append([]string(nil), ent.ids...)
		v.mu.Unlock()
		return ids, nil
	}
	v.mu.Unlock()

	ids, err := v.fetch.FetchUserPool(ctx, namespace)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	v.cache[namespace] = poolEntry{
		ids:   append([]string(nil), ids...),
		until: now.Add(v.ttl),
	}
	v.mu.Unlock()
	return ids, nil
}

// browserActAsUserID returns the act-as user id when the header is set.
// Malformed values return errUnknownActAs. An empty header returns ("", nil).
func browserActAsUserID(r *http.Request) (string, error) {
	raw := strings.TrimSpace(r.Header.Get(headerBrowserActAs))
	if raw == "" {
		return "", nil
	}
	if len(raw) > actAsUserIDMaxLen || !isDigitsOnly(raw) {
		return "", errUnknownActAs
	}
	return raw, nil
}

func resolvePlaygroundProxyIdentity(
	ctx context.Context,
	r *http.Request,
	namespace, ownerUserID, proxyPath string,
	v *actAsValidator,
) (seed string, markActAs bool, err error) {
	actAsUser, err := browserActAsUserID(r)
	if err != nil {
		return "", false, err
	}
	requestID := proxyRequestID(r)
	seed = playgroundClientSeed(requestID, ownerUserID, proxyPath)
	if actAsUser == "" {
		return seed, false, nil
	}
	if v == nil {
		return "", false, fmt.Errorf("act-as validator unavailable")
	}
	if err := v.Validate(ctx, namespace, actAsUser); err != nil {
		return "", false, err
	}
	return actAsUser, true, nil
}

type deploymentUserPoolFetcher struct {
	store     deploymentStore
	clientset kubernetes.Interface
}

func (f *deploymentUserPoolFetcher) FetchUserPool(ctx context.Context, namespace string) ([]string, error) {
	if f.clientset == nil {
		return nil, fmt.Errorf("kubernetes client unavailable")
	}
	design := map[string]any{}
	if f.store != nil {
		row, err := f.store.GetDeployment(ctx, namespace)
		if err == nil && row.Design != nil {
			design = row.Design
		}
	}
	return fetchUserPoolViaServiceProxy(ctx, f.clientset, namespace, design)
}

func fetchUserPoolViaServiceProxy(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace string,
	design map[string]any,
) ([]string, error) {
	host, port, err := resolveProxyTarget("app-server", design)
	if err != nil {
		return nil, err
	}
	svcTarget := fmt.Sprintf("%s:%d", host, port)
	result := clientset.CoreV1().RESTClient().
		Get().
		Namespace(namespace).
		Resource("services").
		Name(svcTarget).
		SubResource("proxy").
		Suffix("user-pool").
		Do(ctx)
	statusCode := 0
	result.StatusCode(&statusCode)
	raw, err := result.Raw()
	if err != nil {
		return nil, fmt.Errorf("fetch user pool: %w", err)
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("fetch user pool: status %d: %s", statusCode, strings.TrimSpace(string(raw)))
	}
	return parseUserPoolIDs(raw)
}

func parseUserPoolIDs(raw []byte) ([]string, error) {
	var payload struct {
		UserIDs []json.RawMessage `json:"userIds"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("parse user pool: %w", err)
	}
	if len(payload.UserIDs) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(payload.UserIDs))
	for _, item := range payload.UserIDs {
		id, err := jsonUserIDString(item)
		if err != nil {
			return nil, err
		}
		if id != "" {
			out = append(out, id)
		}
	}
	return out, nil
}

func jsonUserIDString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return strings.TrimSpace(asString), nil
	}
	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return asNumber.String(), nil
	}
	return "", fmt.Errorf("unsupported user id type in pool")
}

// ForgetActAsCache drops cached user-pool data for a deployment namespace.
func (h *Handler) ForgetActAsCache(namespace string) {
	if h != nil && h.actAs != nil {
		h.actAs.Forget(namespace)
	}
}
