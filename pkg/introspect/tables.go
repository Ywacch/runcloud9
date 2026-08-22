package introspect

import "strings"

const platformTablePrefix = "_c9_"

// IsStudentTable reports whether a Postgres table should appear in student-facing introspect.
// Cloud9 probe tables use the _c9_ prefix and are excluded.
func IsStudentTable(name string) bool {
	return !strings.HasPrefix(name, platformTablePrefix)
}
