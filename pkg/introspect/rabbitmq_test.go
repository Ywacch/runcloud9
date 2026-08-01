package introspect

import "testing"

func TestQueueAllowed(t *testing.T) {
	t.Parallel()
	queues := []string{"fanout", "orders"}
	if !QueueAllowed("fanout", queues) {
		t.Fatal("fanout should be allowed")
	}
	if QueueAllowed("other", queues) {
		t.Fatal("other must be rejected")
	}
}

func TestRabbitMQDriver_Capabilities(t *testing.T) {
	t.Parallel()
	d := &RabbitMQDriver{}
	caps := d.Capabilities()
	want := map[Capability]bool{CapQueues: true, CapPeek: true}
	if len(caps) != 2 {
		t.Fatalf("caps len=%d want 2", len(caps))
	}
	for _, c := range caps {
		if !want[c] {
			t.Fatalf("unexpected capability %q", c)
		}
	}
}
