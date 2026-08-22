package introspect

import "testing"

func TestIsStudentTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want bool
	}{
		{name: "urls", want: true},
		{name: "_c9_probe", want: false},
		{name: "_c9_other", want: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsStudentTable(tt.name); got != tt.want {
				t.Fatalf("IsStudentTable(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
