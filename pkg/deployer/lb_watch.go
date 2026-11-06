package deployer

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

const defaultLBEndpointsDebounce = 2 * time.Second

// LBEndpointsWatcher keeps nginx upstreams in sync with Ready app-server Endpoints for the session.
type LBEndpointsWatcher struct {
	mu       sync.Mutex
	watches  map[string]*lbEndpointsWatch
	debounce time.Duration
}

// NewLBEndpointsWatcher returns a watcher with the default debounce interval.
func NewLBEndpointsWatcher() *LBEndpointsWatcher {
	return &LBEndpointsWatcher{
		watches:  make(map[string]*lbEndpointsWatch),
		debounce: defaultLBEndpointsDebounce,
	}
}

// SetDebounce overrides the debounce interval (tests only).
func (w *LBEndpointsWatcher) SetDebounce(d time.Duration) {
	if d <= 0 {
		return
	}
	w.mu.Lock()
	w.debounce = d
	w.mu.Unlock()
}

// HandleClusterEndpointsObject reacts to informer events for app-server Endpoints or EndpointSlices
// (CLUSTERSTATE_LB on). No-op if there is no active watch for the namespace or the watch uses API Watch.
func (w *LBEndpointsWatcher) HandleClusterEndpointsObject(obj any) {
	ns, ok := lbNotifyNamespace(obj)
	if !ok {
		return
	}
	w.mu.Lock()
	state := w.watches[ns]
	w.mu.Unlock()
	if state == nil || state.endpointView == nil {
		return
	}
	state.onClusterEndpointsChange()
}

// Start watches app-server Endpoints when the design includes an lb node.
// endpointView non-nil uses the informer cache and skips API Watch; lastSet is derived from cache reads.
func (w *LBEndpointsWatcher) Start(clientset kubernetes.Interface, namespace string, design DeploymentDesign, endpointView EndpointView) {
	if clientset == nil || namespace == "" {
		return
	}
	if _, ok := FindLBNode(design); !ok {
		return
	}

	w.Stop(namespace)

	w.mu.Lock()
	debounce := w.debounce
	w.mu.Unlock()

	var lastSet []string
	if endpointView != nil {
		lastSet = readyUpstreamIPsFromView(endpointView, namespace)
	} else {
		ctx := context.Background()
		if ep, err := clientset.CoreV1().Endpoints(namespace).Get(ctx, "app-server", metav1.GetOptions{}); err == nil {
			lastSet = readyAddressSet(ep)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	state := &lbEndpointsWatch{
		cancel:        cancel,
		clientset:     clientset,
		namespace:     namespace,
		design:        design,
		debounce:      debounce,
		lastSet:       lastSet,
		endpointView:  endpointView,
		reconcileOpts: reconcileOptsForView(endpointView),
	}

	w.mu.Lock()
	w.watches[namespace] = state
	w.mu.Unlock()

	if endpointView == nil {
		go state.run(ctx)
	}
}

func reconcileOptsForView(v EndpointView) *ReconcileLBUpstreamOptions {
	if v == nil {
		return nil
	}
	return &ReconcileLBUpstreamOptions{EndpointView: v}
}

// Stop ends the Endpoints watch for a session namespace.
func (w *LBEndpointsWatcher) Stop(namespace string) {
	w.mu.Lock()
	state := w.watches[namespace]
	delete(w.watches, namespace)
	w.mu.Unlock()
	if state != nil {
		state.stop()
	}
}

type lbEndpointsWatch struct {
	cancel    context.CancelFunc
	clientset kubernetes.Interface
	namespace string
	design    DeploymentDesign
	debounce  time.Duration

	endpointView  EndpointView
	reconcileOpts *ReconcileLBUpstreamOptions

	mu            sync.Mutex
	lastSet       []string
	debounceMu    sync.Mutex
	debounceTimer *time.Timer
}

func (s *lbEndpointsWatch) run(ctx context.Context) {
	if s.endpointView != nil {
		return
	}
	watcher, err := s.clientset.CoreV1().Endpoints(s.namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("lb endpoints watch %s: start watch: %v", s.namespace, err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-watcher.ResultChan():
			if !ok {
				return
			}
			if ev.Type == watch.Error {
				log.Printf("lb endpoints watch %s: watch error", s.namespace)
				return
			}
			ep, ok := ev.Object.(*corev1.Endpoints)
			if !ok || ep.Name != "app-server" {
				continue
			}
			s.onEndpoints(ep)
		}
	}
}

func (s *lbEndpointsWatch) onClusterEndpointsChange() {
	if s.endpointView == nil {
		return
	}
	set := readyUpstreamIPsFromView(s.endpointView, s.namespace)
	s.mu.Lock()
	if addressSetsEqual(set, s.lastSet) {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.scheduleReconcile(set)
}

func (s *lbEndpointsWatch) onEndpoints(ep *corev1.Endpoints) {
	set := readyAddressSet(ep)
	s.mu.Lock()
	if addressSetsEqual(set, s.lastSet) {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.scheduleReconcile(set)
}

func (s *lbEndpointsWatch) scheduleReconcile(pending []string) {
	s.debounceMu.Lock()
	defer s.debounceMu.Unlock()
	if s.debounceTimer != nil {
		s.debounceTimer.Stop()
	}
	pendingCopy := append([]string(nil), pending...)
	s.debounceTimer = time.AfterFunc(s.debounce, func() {
		s.mu.Lock()
		if addressSetsEqual(pendingCopy, s.lastSet) {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := ReconcileLBUpstream(ctx, s.clientset, s.namespace, s.design, s.reconcileOpts); err != nil {
			log.Printf("lb endpoints watch reconcile %s: %v", s.namespace, err)
			return
		}
		s.mu.Lock()
		if s.endpointView != nil {
			s.lastSet = readyUpstreamIPsFromView(s.endpointView, s.namespace)
		} else if ep, err := s.clientset.CoreV1().Endpoints(s.namespace).Get(ctx, "app-server", metav1.GetOptions{}); err == nil {
			s.lastSet = readyAddressSet(ep)
		} else {
			s.lastSet = pendingCopy
		}
		s.mu.Unlock()
	})
}

func (s *lbEndpointsWatch) stop() {
	s.cancel()
	s.debounceMu.Lock()
	if s.debounceTimer != nil {
		s.debounceTimer.Stop()
		s.debounceTimer = nil
	}
	s.debounceMu.Unlock()
}

func lbNotifyNamespace(obj any) (string, bool) {
	switch x := obj.(type) {
	case *corev1.Endpoints:
		if x == nil || x.Name != "app-server" {
			return "", false
		}
		return x.Namespace, true
	case *discoveryv1.EndpointSlice:
		if x == nil || x.Labels[discoveryv1.LabelServiceName] != "app-server" {
			return "", false
		}
		return x.Namespace, true
	default:
		return "", false
	}
}

func readyAddressSet(ep *corev1.Endpoints) []string {
	if ep == nil {
		return nil
	}
	var out []string
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			if addr.IP != "" {
				out = append(out, addr.IP)
			}
		}
	}
	sort.Strings(out)
	return out
}

func addressSetsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
