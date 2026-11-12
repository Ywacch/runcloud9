package sessionreg

import "testing"

func TestEnabled(t *testing.T) {
	tests := []struct {
		env  string
		want bool
	}{
		{env: "", want: false},
		{env: "1", want: true},
		{env: "true", want: true},
		{env: "on", want: true},
		{env: "0", want: false},
		{env: "false", want: false},
		{env: "off", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			if got := Enabled(tt.env); got != tt.want {
				t.Fatalf("Enabled(%q) = %v want %v", tt.env, got, tt.want)
			}
		})
	}
}
