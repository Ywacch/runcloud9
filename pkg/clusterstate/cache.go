// Package clusterstate is a process-wide read cache of Kubernetes objects.
// It does not create, update, or delete objects.
package clusterstate

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/kubeobs"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const resyncPeriod = 0

// InformerMetrics receives informer sync and event signals for control-plane gauges.
type InformerMetrics interface {
	SetInformerQueueDepth(informer string, depth float64)
	NoteInformerEvent(informer string)
	RefreshInformerEventAges()
}

type fixedCallerTransport struct {
	caller string
	base   http.RoundTripper
}

func (t *fixedCallerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.base.RoundTrip(req.WithContext(kubeobs.WithCaller(req.Context(), t.caller)))
}

// Cache is the shared informer store. The zero Cache is not started.
// A nil *Cache means callers use the API server, which is today's behavior.
type Cache struct {
	podInformer cache.SharedIndexInformer
	factory     informers.SharedInformerFactory

	deploymentInformer    cache.SharedIndexInformer
	statefulsetInformer   cache.SharedIndexInformer
	hpaInformer           cache.SharedIndexInformer
	endpointsInformer     cache.SharedIndexInformer
	endpointSliceInformer cache.SharedIndexInformer

	metrics InformerMetrics

	lbNotifyMu sync.Mutex
	lbNotify   func(any)

	cancel context.CancelFunc
}

// SetLBEndpointsNotify registers a callback invoked after Endpoints or EndpointSlice
// informer events (Add/Update/Delete). Used by the LB watcher when CLUSTERSTATE_LB is on.
// Pass nil to clear. Safe to call before Start; handlers attach in newCacheFromClient.
func (c *Cache) SetLBEndpointsNotify(fn func(any)) {
	if c == nil {
		return
	}
	c.lbNotifyMu.Lock()
	c.lbNotify = fn
	c.lbNotifyMu.Unlock()
}

func (c *Cache) invokeLBNotify(obj any) {
	if c == nil {
		return
	}
	c.lbNotifyMu.Lock()
	fn := c.lbNotify
	c.lbNotifyMu.Unlock()
	if fn != nil {
		fn(obj)
	}
}

// New builds informers. It does not start them.
func New(restConfig *rest.Config, obs kubeobs.RequestObserver, metrics InformerMetrics) (*Cache, error) {
	if restConfig == nil {
		return nil, fmt.Errorf("clusterstate: rest config required")
	}
	cfg := rest.CopyConfig(restConfig)
	prev := cfg.WrapTransport
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if prev != nil {
			rt = prev(rt)
		}
		return &fixedCallerTransport{caller: "clusterstate", base: rt}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("clusterstate: clientset: %w", err)
	}
	return newCacheFromClient(client, metrics), nil
}

func newCacheFromClient(client kubernetes.Interface, metrics InformerMetrics) *Cache {
	c := &Cache{metrics: metrics}

	podReq, err := labels.NewRequirement("deployment", selection.Exists, nil)
	if err != nil {
		panic(fmt.Sprintf("clusterstate: pod label selector: %v", err))
	}
	podSelector := labels.NewSelector().Add(*podReq)

	c.podInformer = coreinformers.NewFilteredPodInformer(
		client,
		metav1.NamespaceAll,
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = podSelector.String()
		},
	)
	_ = c.podInformer.SetTransform(transformPod)
	c.attachMetricsHandler("pod", c.podInformer)

	c.factory = informers.NewSharedInformerFactory(client, resyncPeriod)

	c.deploymentInformer = c.factory.Apps().V1().Deployments().Informer()
	_ = c.deploymentInformer.SetTransform(transformDeployment)
	c.attachMetricsHandler("deployment", c.deploymentInformer)

	c.statefulsetInformer = c.factory.Apps().V1().StatefulSets().Informer()
	_ = c.statefulsetInformer.SetTransform(transformStatefulSet)
	c.attachMetricsHandler("statefulset", c.statefulsetInformer)

	c.hpaInformer = c.factory.Autoscaling().V2().HorizontalPodAutoscalers().Informer()
	_ = c.hpaInformer.SetTransform(transformHPA)
	c.attachMetricsHandler("hpa", c.hpaInformer)

	c.endpointsInformer = c.factory.Core().V1().Endpoints().Informer()
	_ = c.endpointsInformer.SetTransform(transformEndpoints)
	c.attachMetricsHandlerWithLB("endpoints", c.endpointsInformer)

	c.endpointSliceInformer = c.factory.Discovery().V1().EndpointSlices().Informer()
	_ = c.endpointSliceInformer.SetTransform(transformEndpointSlice)
	c.attachMetricsHandlerWithLB("endpointslice", c.endpointSliceInformer)

	return c
}

func (c *Cache) attachMetricsHandler(name string, inf cache.SharedIndexInformer) {
	if c.metrics == nil {
		return
	}
	inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { c.metrics.NoteInformerEvent(name) },
		UpdateFunc: func(_, _ any) { c.metrics.NoteInformerEvent(name) },
		DeleteFunc: func(_ any) { c.metrics.NoteInformerEvent(name) },
	})
}

func (c *Cache) attachMetricsHandlerWithLB(name string, inf cache.SharedIndexInformer) {
	inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if c.metrics != nil {
				c.metrics.NoteInformerEvent(name)
			}
			c.invokeLBNotify(obj)
		},
		UpdateFunc: func(_, obj any) {
			if c.metrics != nil {
				c.metrics.NoteInformerEvent(name)
			}
			c.invokeLBNotify(obj)
		},
		DeleteFunc: func(obj any) {
			if c.metrics != nil {
				c.metrics.NoteInformerEvent(name)
			}
			c.invokeLBNotify(obj)
		},
	})
}

// Start runs the informers until ctx is cancelled.
func (c *Cache) Start(ctx context.Context) {
	ctx, c.cancel = context.WithCancel(ctx)
	c.factory.Start(ctx.Done())
	go c.podInformer.Run(ctx.Done())
	go c.runMetricsLoop(ctx)
	c.refreshSyncMetrics()
}

func (c *Cache) runMetricsLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refreshSyncMetrics()
			if c.metrics != nil {
				c.metrics.RefreshInformerEventAges()
			}
		}
	}
}

func (c *Cache) refreshSyncMetrics() {
	if c.metrics == nil {
		return
	}
	for _, entry := range c.syncedEntries() {
		depth := 1.0
		if entry.synced() {
			depth = 0
		}
		c.metrics.SetInformerQueueDepth(entry.name, depth)
	}
}

type syncEntry struct {
	name   string
	synced cache.InformerSynced
}

func (c *Cache) syncedEntries() []syncEntry {
	return []syncEntry{
		{"pod", c.podInformer.HasSynced},
		{"deployment", c.deploymentInformer.HasSynced},
		{"statefulset", c.statefulsetInformer.HasSynced},
		{"hpa", c.hpaInformer.HasSynced},
		{"endpoints", c.endpointsInformer.HasSynced},
		{"endpointslice", c.endpointSliceInformer.HasSynced},
	}
}

// WaitForSync blocks until every informer HasSynced or ctx ends.
func (c *Cache) WaitForSync(ctx context.Context) error {
	syncs := make([]cache.InformerSynced, 0, 6)
	for _, e := range c.syncedEntries() {
		syncs = append(syncs, e.synced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), syncs...) {
		c.refreshSyncMetrics()
		return fmt.Errorf("clusterstate: sync timeout")
	}
	c.refreshSyncMetrics()
	return nil
}

func requireNamespace(namespace string) error {
	if namespace == "" {
		return fmt.Errorf("clusterstate: namespace required")
	}
	return nil
}

func podHasDeploymentLabel(p *corev1.Pod) bool {
	if p == nil || p.Labels == nil {
		return false
	}
	_, ok := p.Labels["deployment"]
	return ok
}

// Pods returns pods in namespace whose labels include key deployment.
func (c *Cache) Pods(namespace string) ([]*corev1.Pod, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	objs, err := c.podInformer.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(objs))
	for _, obj := range objs {
		p, ok := obj.(*corev1.Pod)
		if !ok || !podHasDeploymentLabel(p) {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// Pod returns one pod by namespace and name.
func (c *Cache) Pod(namespace, name string) (*corev1.Pod, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	key := namespace + "/" + name
	obj, exists, err := c.podInformer.GetIndexer().GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "pods"}, name)
	}
	p, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("clusterstate: unexpected pod type %T", obj)
	}
	if !podHasDeploymentLabel(p) {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "pods"}, name)
	}
	return p, nil
}

func listDeployments(namespace string, inf cache.SharedIndexInformer) ([]*appsv1.Deployment, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	objs, err := inf.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil, err
	}
	out := make([]*appsv1.Deployment, 0, len(objs))
	for _, obj := range objs {
		d, ok := obj.(*appsv1.Deployment)
		if !ok {
			return nil, fmt.Errorf("clusterstate: unexpected deployment type %T", obj)
		}
		out = append(out, d)
	}
	return out, nil
}

// Deployments returns every Deployment in namespace.
func (c *Cache) Deployments(namespace string) ([]*appsv1.Deployment, error) {
	return listDeployments(namespace, c.deploymentInformer)
}

// StatefulSets returns every StatefulSet in namespace.
func (c *Cache) StatefulSets(namespace string) ([]*appsv1.StatefulSet, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	objs, err := c.statefulsetInformer.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil, err
	}
	out := make([]*appsv1.StatefulSet, 0, len(objs))
	for _, obj := range objs {
		s, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			return nil, fmt.Errorf("clusterstate: unexpected statefulset type %T", obj)
		}
		out = append(out, s)
	}
	return out, nil
}

// HorizontalPodAutoscalers returns every HPA in namespace.
func (c *Cache) HorizontalPodAutoscalers(namespace string) ([]*autoscalingv2.HorizontalPodAutoscaler, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	objs, err := c.hpaInformer.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil, err
	}
	out := make([]*autoscalingv2.HorizontalPodAutoscaler, 0, len(objs))
	for _, obj := range objs {
		h, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler)
		if !ok {
			return nil, fmt.Errorf("clusterstate: unexpected hpa type %T", obj)
		}
		out = append(out, h)
	}
	return out, nil
}

// Endpoints returns Endpoints by namespace and name.
func (c *Cache) Endpoints(namespace, name string) (*corev1.Endpoints, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	key := namespace + "/" + name
	obj, exists, err := c.endpointsInformer.GetIndexer().GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "endpoints"}, name)
	}
	e, ok := obj.(*corev1.Endpoints)
	if !ok {
		return nil, fmt.Errorf("clusterstate: unexpected endpoints type %T", obj)
	}
	return e, nil
}

// EndpointSlices returns slices in namespace for the given Service name.
func (c *Cache) EndpointSlices(namespace, serviceName string) ([]*discoveryv1.EndpointSlice, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	all, err := c.endpointSlicesList(namespace)
	if err != nil {
		return nil, err
	}
	out := make([]*discoveryv1.EndpointSlice, 0)
	for _, slice := range all {
		if slice.Labels[discoveryv1.LabelServiceName] == serviceName {
			out = append(out, slice)
		}
	}
	return out, nil
}

func (c *Cache) endpointSlicesList(namespace string) ([]*discoveryv1.EndpointSlice, error) {
	if err := requireNamespace(namespace); err != nil {
		return nil, err
	}
	objs, err := c.endpointSliceInformer.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil, err
	}
	out := make([]*discoveryv1.EndpointSlice, 0, len(objs))
	for _, obj := range objs {
		s, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok {
			return nil, fmt.Errorf("clusterstate: unexpected endpointslice type %T", obj)
		}
		out = append(out, s)
	}
	return out, nil
}
