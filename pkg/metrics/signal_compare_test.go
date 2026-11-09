package metrics

import "testing"

func TestPromValuesMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		signalVal, oldVal float64
		signalOK, oldOK   bool
		want              bool
	}{
		{"both absent", 0, 0, false, false, true},
		{"one absent", 1, 1, true, false, false},
		{"exact", 3, 3, true, true, true},
		{"within relative", 100, 104, true, true, true},
		{"outside tolerance", 1, 10, true, true, false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := promValuesMatch(tt.signalVal, tt.oldVal, tt.signalOK, tt.oldOK); got != tt.want {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestSignalCompareEnabled(t *testing.T) {
	t.Parallel()

	for env, want := range map[string]bool{
		"":      false,
		"0":     false,
		"1":     true,
		"true":  true,
		"FALSE": false,
		"on":    true,
	} {
		if got := SignalCompareEnabled(env); got != want {
			t.Fatalf("env %q: got %v want %v", env, got, want)
		}
	}
}
