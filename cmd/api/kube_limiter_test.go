package main

import (
	"testing"
)

func TestKubeClientLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		qpsRaw    string
		burstRaw  string
		wantQPS   float32
		wantBurst int
	}{
		{
			name:      "unset uses defaults",
			qpsRaw:    "",
			burstRaw:  "",
			wantQPS:   400,
			wantBurst: 800,
		},
		{
			name:      "explicit 200 and 400",
			qpsRaw:    "200",
			burstRaw:  "400",
			wantQPS:   200,
			wantBurst: 400,
		},
		{
			name:      "invalid qps falls back",
			qpsRaw:    "nope",
			burstRaw:  "100",
			wantQPS:   defaultKubeClientQPS,
			wantBurst: 100,
		},
		{
			name:      "zero qps falls back",
			qpsRaw:    "0",
			burstRaw:  "50",
			wantQPS:   defaultKubeClientQPS,
			wantBurst: 50,
		},
		{
			name:      "negative qps falls back",
			qpsRaw:    "-1",
			burstRaw:  "80",
			wantQPS:   defaultKubeClientQPS,
			wantBurst: 80,
		},
		{
			name:      "invalid burst falls back",
			qpsRaw:    "150",
			burstRaw:  "nope",
			wantQPS:   150,
			wantBurst: defaultKubeClientBurst,
		},
		{
			name:      "zero burst falls back",
			qpsRaw:    "50",
			burstRaw:  "0",
			wantQPS:   50,
			wantBurst: defaultKubeClientBurst,
		},
		{
			name:      "negative burst falls back",
			qpsRaw:    "50",
			burstRaw:  "-1",
			wantQPS:   50,
			wantBurst: defaultKubeClientBurst,
		},
		{
			name:      "burst lower than qps allowed",
			qpsRaw:    "300",
			burstRaw:  "50",
			wantQPS:   300,
			wantBurst: 50,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotQPS, gotBurst := kubeClientLimits(tc.qpsRaw, tc.burstRaw)
			if gotBurst != tc.wantBurst {
				t.Fatalf("burst = %d want %d", gotBurst, tc.wantBurst)
			}
			if gotQPS != tc.wantQPS {
				t.Fatalf("qps = %v want %v", gotQPS, tc.wantQPS)
			}
		})
	}
}
