package metrics

import (
	"encoding/json"
	"testing"
)

func TestRabbitOverviewSnapshotDecode(t *testing.T) {
	t.Parallel()
	raw := `{
  "queue_totals": {
    "messages": 12,
    "messages_ready": 3,
    "messages_unacknowledged": 9
  },
  "message_stats": {
    "publish_details": { "rate": 1.5 },
    "deliver_get_details": { "rate": 2.25 }
  }
}`
	var snap rabbitOverviewSnapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.QueueTotals.Messages != 12 || snap.QueueTotals.MessagesReady != 3 || snap.QueueTotals.MessagesUnacked != 9 {
		t.Fatalf("queue_totals: %#v", snap.QueueTotals)
	}
	if snap.MessageStats.PublishDetails.Rate != 1.5 || snap.MessageStats.DeliverGetDetails.Rate != 2.25 {
		t.Fatalf("message_stats rates: %#v", snap.MessageStats)
	}
}
