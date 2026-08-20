package loadgen

import (
	"fmt"
	"hash/fnv"
	"net/http"
)

const virtualClientPoolSize = 254

// virtualClientIP maps seed to a stable TEST-NET-2 address in 198.51.100.1..198.51.100.254.
func virtualClientIP(seed string) string {
	if seed == "" {
		return "198.51.100.1"
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	n := h.Sum64() % virtualClientPoolSize
	return fmt.Sprintf("198.51.100.%d", 1+n)
}

// ApplyClientIdentityHeaders sets X-Forwarded-For to a sticky virtual client IP derived from seed.
func ApplyClientIdentityHeaders(h http.Header, seed string) {
	h.Set("X-Forwarded-For", virtualClientIP(seed))
}
