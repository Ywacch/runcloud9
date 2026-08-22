package introspect

import "strings"

const platformTablePrefix = "_c9_"

// IsStudentCacheKey reports whether a Redis key should appear in student-facing introspect.
// Cloud9 probe keys use the _c9_ prefix and are excluded.
func IsStudentCacheKey(name string) bool {
	return !strings.HasPrefix(name, platformTablePrefix)
}

// IsStudentTable reports whether a Postgres table should appear in student-facing introspect.
// Cloud9 probe tables use the _c9_ prefix and are excluded.
func IsStudentTable(name string) bool {
	return IsStudentCacheKey(name)
}
