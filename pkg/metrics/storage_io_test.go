package metrics

import (
	"strings"
	"testing"
)

func TestLookupStorageProvider_PostgreSQL(t *testing.T) {
	t.Parallel()
	prov, ok := LookupStorageProvider(StorageEnginePostgreSQL)
	if !ok {
		t.Fatal("expected postgresql provider")
	}
	if prov.Unit != "rows/s" || prov.Source != "postgres_exporter" {
		t.Fatalf("provider: %+v", prov)
	}
	q := prov.ReadQuery("ns1")
	if !strings.Contains(q, `namespace="ns1"`) || !strings.Contains(q, "tup_returned") {
		t.Fatalf("read query: %s", q)
	}
}

func TestApplyStorageProfile(t *testing.T) {
	t.Parallel()
	var cm ComponentMetrics
	ApplyStorageProfile(&cm, StorageEnginePostgreSQL, StorageRolePrimary)
	if cm.StorageEngine != "postgresql" || cm.StorageRole != StorageRolePrimary {
		t.Fatalf("metadata: engine=%q role=%q", cm.StorageEngine, cm.StorageRole)
	}
	if cm.StorageIO == nil || cm.StorageIO.Unit != "rows/s" {
		t.Fatalf("storageIo: %+v", cm.StorageIO)
	}
	if cm.StorageIO.ReadRate != 0 || cm.StorageIO.WriteRate != 0 {
		t.Fatalf("phase 1 stub should have zero rates, got read=%v write=%v", cm.StorageIO.ReadRate, cm.StorageIO.WriteRate)
	}
}

func TestApplyStorageProfile_UnknownEngine(t *testing.T) {
	t.Parallel()
	var cm ComponentMetrics
	ApplyStorageProfile(&cm, StorageEngineID("unknown-db"), "primary")
	if cm.StorageEngine != "unknown-db" || cm.StorageIO != nil {
		t.Fatalf("unknown engine: io=%+v engine=%q", cm.StorageIO, cm.StorageEngine)
	}
}

func TestMaxStoragePeaksFromComponents(t *testing.T) {
	t.Parallel()
	components := map[string]ComponentMetrics{
		"db": {
			StorageIO: &StorageIO{
				ReadRate:      100,
				WriteRate:     20,
				ReadLatency:   0.01,
				WriteLatency:  0.05,
				BytesUsed:     800,
				BytesLimit:    1000,
				ErrorRate:     0.5,
			},
		},
		"cache": {
			StorageIO: &StorageIO{WriteRate: 30},
		},
	}
	r, w, rl, wl, u, e := maxStoragePeaksFromComponents(components)
	if r != 100 || w != 30 || rl != 10 || wl != 50 || u != 80 || e != 0.5 {
		t.Fatalf("peaks: r=%v w=%v rl=%v wl=%v u=%v e=%v", r, w, rl, wl, u, e)
	}
}
