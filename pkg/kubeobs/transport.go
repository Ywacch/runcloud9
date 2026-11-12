package kubeobs

import (
	"net/http"
	"strings"
	"time"
)

// RequestObserver records Kubernetes API client traffic.
type RequestObserver interface {
	ObserveRequest(verb, resource, caller string, seconds float64)
	ObserveThrottle(waitSeconds float64)
	// ObserveThrottleWait records one client-go rate-limiter wait, including a
	// wait of zero. verb and resource match Classify. caller is already normalized.
	ObserveThrottleWait(verb, resource, caller string, seconds float64)
}

type roundTripper struct {
	base http.RoundTripper
	obs  RequestObserver
}

// Wrap returns an http.RoundTripper that observes each Kubernetes API request.
func Wrap(base http.RoundTripper, obs RequestObserver) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if obs == nil {
		return base
	}
	return &roundTripper{base: base, obs: obs}
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := rt.base.RoundTrip(req)
	if rt.obs != nil {
		verb, resource := Classify(req.Method, req.URL.Path)
		caller := normalizeCaller(CallerFrom(req.Context()))
		rt.obs.ObserveRequest(verb, resource, caller, time.Since(start).Seconds())
	}
	return resp, err
}

// Classify maps an Kubernetes API request method and path to verb and resource labels.
func Classify(method, path string) (verb, resource string) {
	method = strings.ToUpper(strings.TrimSpace(method))
	switch method {
	case http.MethodPost:
		verb = "create"
	case http.MethodPut:
		verb = "update"
	case http.MethodPatch:
		verb = "patch"
	case http.MethodDelete:
		verb = "delete"
	case http.MethodGet:
		verb = "list"
	default:
		verb = strings.ToLower(method)
	}

	segments := splitPath(path)
	nsIdx := indexSegment(segments, "namespaces")
	if nsIdx >= 0 {
		if nsIdx+1 >= len(segments) {
			resource = "namespaces"
			return verb, resource
		}
		if nsIdx+2 >= len(segments) {
			resource = "namespaces"
			if method == http.MethodGet {
				verb = "get"
			}
			return verb, resource
		}
		resource = segments[nsIdx+2]
		hasName := nsIdx+3 < len(segments)
		if method == http.MethodGet {
			if hasName {
				verb = "get"
			} else {
				verb = "list"
			}
		}
		if method == http.MethodDelete && hasName {
			verb = "delete"
		}
		return verb, resource
	}

	if len(segments) == 0 {
		return verb, "unknown"
	}
	resource = segments[len(segments)-1]
	if method == http.MethodGet {
		verb = "list"
	}
	return verb, resource
}

func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func indexSegment(segments []string, want string) int {
	for i, s := range segments {
		if s == want {
			return i
		}
	}
	return -1
}
