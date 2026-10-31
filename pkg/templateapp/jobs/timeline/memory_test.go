package timeline_test

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline/timelinetest"
)

func TestMemoryStore_Conformance(t *testing.T) {
	t.Parallel()
	timelinetest.RunConformance(t, func(t *testing.T) (timeline.Store, func()) {
		t.Helper()
		return timeline.NewMemory(1000), func() {}
	})
}
