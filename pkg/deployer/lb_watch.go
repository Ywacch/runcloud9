package deployer

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// Start watches app-server Endpoints when the design includes an lb node.
func (w *LBEndpointsWatcher) Start(clientset kubernetes.Interface, namespace string, design DeploymentDesign) {
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

	ctx, cancel := context.WithCancel(context.Background())
	state := &lbEndpointsWatch{
		cancel:    cancel,
		clientset: clientset,
		namespace: namespace,
		design:    design,
		debounce:  debounce,
	}

	if ep, err := clientset.CoreV1().Endpoints(namespace).Get(ctx, "app-server", metav1.GetOptions{}); err == nil {
		state.lastSet = readyAddressSet(ep)
	}

	w.mu.Lock()
	w.watches[namespace] = state
	w.mu.Unlock()

	go state.run(ctx)
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

	mu            sync.Mutex
	lastSet       []string
	debounceMu    sync.Mutex
	debounceTimer *time.Timer
}

func (s *lbEndpointsWatch) run(ctx context.Context) {
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
		if err := ReconcileLBUpstream(ctx, s.clientset, s.namespace, s.design); err != nil {
			log.Printf("lb endpoints watch reconcile %s: %v", s.namespace, err)
			return
		}
		s.mu.Lock()
		if ep, err := s.clientset.CoreV1().Endpoints(s.namespace).Get(ctx, "app-server", metav1.GetOptions{}); err == nil {
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
