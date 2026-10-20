package loadgen

import (
	"fmt"
	"hash/fnv"
	"net/http"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

const virtualClientPoolSize = 254

const defaultVirtualClientIP = "198.18.1.1"

// defaultClientID is the logical identity used when the seed is empty.
// virtualClientIP maps that empty seed to defaultVirtualClientIP.
const defaultClientID = "c9-default"

// virtualClientSubnets are RFC 2544 prefixes (198.18.0.0/15) chosen so nginx ip_hash,
// which hashes only the first three IPv4 octets, maps virtual clients to different
// app-server replicas at 2 and 3 replica counts. The last octet is for limit_req
// ($binary_remote_addr), not for ip_hash routing.
var virtualClientSubnets = []struct{ a, b, c byte }{
	{198, 18, 1},
	{198, 18, 2},
	{198, 18, 0},
}

// virtualClientIP maps seed to a stable virtual client address.
func virtualClientIP(seed string) string {
	if seed == "" {
		return defaultVirtualClientIP
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	n := h.Sum64()
	subnetCount := uint64(len(virtualClientSubnets))
	sub := virtualClientSubnets[n%subnetCount]
	last := 1 + int((n/subnetCount)%virtualClientPoolSize)
	return fmt.Sprintf("%d.%d.%d.%d", sub.a, sub.b, sub.c, last)
}

// ApplyClientIdentityHeaders sets network and logical identity from seed.
// X-Forwarded-For is the virtual client IP. X-C9-Client-ID is the seed itself.
func ApplyClientIdentityHeaders(h http.Header, seed string) {
	h.Set("X-Forwarded-For", virtualClientIP(seed))
	id := seed
	if id == "" {
		id = defaultClientID
	}
	h.Set(trace.HeaderClientID, id)
}
