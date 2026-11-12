package sessionreg

// Observer receives session-registry telemetry. All methods are no-ops when nil.
type Observer interface {
	SetSessionRegistryRows(n int)
	ObserveSessionRegistryResync(seconds float64)
	IncSessionRegistryFallback(result string)
}
