package storetest

import (
	"testing"

	"github.com/ywacch/cloud9/images/social-feed/socialstore"
)

func TestFakeStore_Conformance(t *testing.T) {
	RunConformance(t, func(t *testing.T) (socialstore.Store, func()) {
		t.Helper()
		return NewFake(), func() {}
	})
}
