package engines

import (
	"testing"
)

func TestDefaultCatalog_Valid(t *testing.T) {
	t.Parallel()
	if err := validate(Default()); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultCatalog_PairLookup(t *testing.T) {
	t.Parallel()
	c := Default()
	p, ok := c.Pair("hot_cache", "redis")
	if !ok {
		t.Fatal("expected hot_cache/redis pair")
	}
	if p.Fit != FitNatural || p.Build != BuildAvailable {
		t.Fatalf("pair: %+v", p)
	}
	if _, ok := c.Pair("main_db", "redis"); ok {
		t.Fatal("unexpected main_db/redis pair")
	}
}

func TestValidate_Rejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mut  func(*Catalog)
	}{
		{
			name: "unknown engine in pair",
			mut: func(c *Catalog) {
				c.Pairs = append(c.Pairs, Pair{
					Job: "main_db", Engine: "unknown",
					Fit: FitNatural, Build: BuildPlanned,
					Reason: "x", Fidelity: FidelityReal,
				})
			},
		},
		{
			name: "poor without demo",
			mut: func(c *Catalog) {
				c.Pairs = append(c.Pairs, Pair{
					Job: "hot_cache", Engine: "postgresql",
					Fit: FitPoor, Build: BuildNotBuilt,
					Reason: "x", Fidelity: FidelityReal,
				})
			},
		},
		{
			name: "bad fit enum",
			mut: func(c *Catalog) {
				c.Pairs[0].Fit = Fit("bogus")
			},
		},
		{
			name: "verified entry without chaos",
			mut: func(c *Catalog) {
				c.Guarantees[0].Entries[0].Status = "verified"
				c.Guarantees[0].Entries[0].Chaos = ""
				c.Guarantees[0].Entries[0].Check = "some_rule"
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := buildDefaultCatalog()
			tc.mut(c)
			err := validate(c)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
