package introspect

// QueueCapabilities reports stream queue introspection support.
func (d *RedisDriver) QueueCapabilities() []Capability {
	return []Capability{CapQueues, CapPeek}
}
