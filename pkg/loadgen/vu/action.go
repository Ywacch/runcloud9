package vu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

const maxResponseBytes = 64 << 10

var (
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	idFieldPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)
)

var allowedMethods = map[string]struct{}{
	http.MethodGet:    {},
	http.MethodPost:   {},
	http.MethodPut:    {},
	http.MethodPatch:  {},
	http.MethodDelete: {},
}

// Action is one template-declared HTTP call.
type Action struct {
	Method  string `json:"method"`
	Path    string `json:"path"`
	Body    string `json:"body,omitempty"`
	IDField string `json:"idField,omitempty"`
}

// Vars fills action placeholders.
type Vars struct {
	UserID   string
	ParentID string
	RootID   string
	Seq      int64
	Rand     int
}

// ValidID reports whether s is safe to substitute into an action.
func ValidID(s string) bool {
	return idPattern.MatchString(s)
}

// Validate checks the action's shape and placeholders.
func (a Action) Validate() error {
	method := strings.ToUpper(strings.TrimSpace(a.Method))
	if method == "" {
		return fmt.Errorf("vu: action method is required")
	}
	if _, ok := allowedMethods[method]; !ok {
		return fmt.Errorf("vu: action method %q not supported", a.Method)
	}
	if !strings.HasPrefix(a.Path, "/") {
		return fmt.Errorf("vu: action path %q must start with /", a.Path)
	}
	strip := strings.NewReplacer(
		"{{.UserID}}", "",
		"{{.ParentID}}", "",
		"{{.RootID}}", "",
		"{{.Seq}}", "",
		"{{.Rand}}", "",
	)
	if strings.Contains(strip.Replace(a.Path), "{{") {
		return fmt.Errorf("vu: action path %q has an unknown placeholder", a.Path)
	}
	if strings.Contains(strip.Replace(a.Body), "{{") {
		return fmt.Errorf("vu: action body has an unknown placeholder")
	}
	if a.IDField != "" && !idFieldPattern.MatchString(a.IDField) {
		return fmt.Errorf("vu: action idField %q is invalid", a.IDField)
	}
	return nil
}

func (a Action) render(v Vars) (path, body string) {
	r := strings.NewReplacer(
		"{{.UserID}}", v.UserID,
		"{{.ParentID}}", v.ParentID,
		"{{.RootID}}", v.RootID,
		"{{.Seq}}", strconv.FormatInt(v.Seq, 10),
		"{{.Rand}}", strconv.Itoa(v.Rand),
	)
	return r.Replace(a.Path), r.Replace(a.Body)
}

func doAction(ctx context.Context, vu *VU, name string, vars Vars) (string, error) {
	a, ok := vu.Actions[name]
	if !ok {
		return "", fmt.Errorf("vu: action %q not configured", name)
	}
	for _, id := range []string{vars.UserID, vars.ParentID, vars.RootID} {
		if id != "" && !ValidID(id) {
			return "", fmt.Errorf("vu: unsafe id %q", id)
		}
	}
	path, body := a.render(vars)

	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(a.Method), vu.BaseURL+path, reader)
	if err != nil {
		return "", fmt.Errorf("vu: build %s request: %w", name, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if vu.ApplyIdentity != nil {
		vu.ApplyIdentity(req.Header, vu.UserID)
	}
	req.Header.Set("X-Request-ID", fmt.Sprintf("vu%d-%016x", vu.ID, rand.Uint64()))

	resp, err := vu.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("vu: %s: %w", name, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", fmt.Errorf("vu: %s: read body: %w", name, err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("vu: %s: status %d", name, resp.StatusCode)
	}
	if a.IDField == "" {
		return "", nil
	}
	id, err := extractID(raw, a.IDField)
	if err != nil {
		return "", fmt.Errorf("vu: %s: %w", name, err)
	}
	return id, nil
}

var errMissingID = errors.New("response id missing")

func extractID(raw []byte, field string) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	v, ok := obj[field]
	if !ok || v == nil {
		return "", fmt.Errorf("%w: field %q", errMissingID, field)
	}
	var id string
	switch t := v.(type) {
	case json.Number:
		n, err := strconv.ParseInt(t.String(), 10, 64)
		if err != nil {
			return "", fmt.Errorf("field %q is not an integer id", field)
		}
		id = strconv.FormatInt(n, 10)
	case string:
		id = t
	default:
		return "", fmt.Errorf("field %q has unsupported type", field)
	}
	if !ValidID(id) {
		return "", fmt.Errorf("field %q has unsafe id %q", field, id)
	}
	return id, nil
}
