package storetest

import (
	"testing"

	"github.com/ywacch/cloud9/images/url-shortener/urlstore"
)

func TestFakeStore_Conformance(t *testing.T) {
	RunConformance(t, func(t *testing.T) (urlstore.Store, func()) {
		t.Helper()
		return NewFake(), func() {}
	})
}
