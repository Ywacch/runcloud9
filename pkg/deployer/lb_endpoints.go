package deployer

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
)

// EndpointView is the clusterstate read path for LB upstream resolution.
// Declared in pkg/deployer so clusterstate does not import deployer.
type EndpointView interface {
	Endpoints(namespace, name string) (*corev1.Endpoints, error)
	EndpointSlices(namespace, serviceName string) ([]*discoveryv1.EndpointSlice, error)
	Pods(namespace string) ([]*corev1.Pod, error)
}

// hostsFromEndpoints builds upstream hosts and snapshots from ready Subsets[].Addresses (IPv4 only).
func hostsFromEndpoints(ep *corev1.Endpoints) (hosts []string, snapshots []LBEndpointSnapshot) {
	if ep == nil || len(ep.Subsets) == 0 {
		return nil, nil
	}
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			if addr.IP == "" {
				continue
			}
			if isIPv4(addr.IP) {
				hosts = append(hosts, addr.IP)
				name := ""
				if addr.TargetRef != nil {
					name = addr.TargetRef.Name
				}
				snapshots = append(snapshots, LBEndpointSnapshot{
					PodName: name,
					PodIP:   addr.IP,
					Ready:   true,
				})
			}
		}
	}
	if len(hosts) == 0 {
		return nil, nil
	}
	return hosts, snapshots
}

func isIPv4(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			return true
		case ':':
			return false
		}
	}
	return false
}

// endpointSliceReady reports whether the endpoint should be treated as ready for LB upstream.
// nil Ready means ready (match core Endpoints ready-only path semantics for this LB).
func endpointSliceReady(c *bool) bool {
	return c == nil || *c
}

// hostsFromEndpointSlices builds hosts from EndpointSlices for serviceName (e.g. app-server).
// Skips addresses when Conditions.Ready is false. Skips IPv6 when AddressType is IPv6.
func hostsFromEndpointSlices(slices []*discoveryv1.EndpointSlice) (hosts []string, snapshots []LBEndpointSnapshot) {
	for _, slice := range slices {
		if slice == nil {
			continue
		}
		skipIPv6 := slice.AddressType == discoveryv1.AddressTypeIPv6
		for _, ep := range slice.Endpoints {
			if !endpointSliceReady(ep.Conditions.Ready) {
				continue
			}
			name := ""
			if ep.TargetRef != nil {
				name = ep.TargetRef.Name
			}
			for _, addr := range ep.Addresses {
				if addr == "" {
					continue
				}
				if skipIPv6 && !isIPv4(addr) {
					continue
				}
				hosts = append(hosts, addr)
				snapshots = append(snapshots, LBEndpointSnapshot{
					PodName: name,
					PodIP:   addr,
					Ready:   true,
				})
			}
		}
	}
	if len(hosts) == 0 {
		return nil, nil
	}
	return hosts, snapshots
}

// resolveAppServerUpstreamFromView resolves upstream using EndpointView (informer cache).
func resolveAppServerUpstreamFromView(view EndpointView, namespace string) (hosts []string, endpoints []LBEndpointSnapshot) {
	if view == nil {
		return nil, nil
	}
	slices, err := view.EndpointSlices(namespace, "app-server")
	if err == nil && len(slices) > 0 {
		hosts, snaps := hostsFromEndpointSlices(slices)
		if len(hosts) > 0 {
			return hosts, snaps
		}
	}
	ep, err := view.Endpoints(namespace, "app-server")
	if err != nil || ep == nil || len(ep.Subsets) == 0 {
		return []string{"app-server"}, nil
	}
	hosts, snaps := hostsFromEndpoints(ep)
	if len(hosts) == 0 {
		return []string{"app-server"}, nil
	}
	return hosts, snaps
}

// readyUpstreamIPsFromView returns sorted ready backend IPs for app-server using view data
// (EndpointSlices first, else Endpoints), for debounce comparison with the API-server path.
func readyUpstreamIPsFromView(view EndpointView, namespace string) []string {
	if view == nil {
		return nil
	}
	slices, err := view.EndpointSlices(namespace, "app-server")
	if err == nil && len(slices) > 0 {
		hosts, _ := hostsFromEndpointSlices(slices)
		sort.Strings(hosts)
		return hosts
	}
	ep, err := view.Endpoints(namespace, "app-server")
	if err != nil || ep == nil {
		return nil
	}
	return readyAddressSet(ep)
}
