package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/metrics/sidework"
)

func TestBeginSideWorkScrape_NoViewerSkips(t *testing.T) {
	t.Parallel()
	c := NewMetricsCollector("", nil, nil, nil, nil)
	before := c.sideWork.Inflight()
	_, ok := c.beginSideWorkScrape(context.Background(), "ns-no-viewer")
	if ok {
		t.Fatal("expected skip without live publisher")
	}
	if c.sideWork.Inflight() != before {
		t.Fatalf("Acquire ran with no viewer: inflight %d", c.sideWork.Inflight())
	}
}

func TestBeginSideWorkScrape_RespectsCadence(t *testing.T) {
	t.Parallel()
	c := NewMetricsCollector("", nil, nil, nil, nil)
	c.sideWorkCadence = time.Hour
	c.EnsurePublisher("ns-cadence", "url-shortener", TopologyFlags{})
	release, ok := c.beginSideWorkScrape(context.Background(), "ns-cadence")
	if !ok {
		t.Fatal("expected first scrape slot")
	}
	release()
	_, ok = c.beginSideWorkScrape(context.Background(), "ns-cadence")
	if ok {
		t.Fatal("expected cadence to block second scrape")
	}
	c.StopPublisher("ns-cadence")
}

func TestSideWorkPool_CapsMatchEnvDefault(t *testing.T) {
	t.Parallel()
	c := NewMetricsCollector("", nil, nil, nil, nil)
	if c.SideWorkPool().Slots() != sidework.SlotsFromEnv() {
		t.Fatalf("slots=%d env=%d", c.SideWorkPool().Slots(), sidework.SlotsFromEnv())
	}
}
