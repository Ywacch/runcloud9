package metrics

// consistencyObserver runs background primary/replica consistency probes per namespace.
type consistencyObserver interface {
	Start(namespace string)
	Stop(namespace string)
}
