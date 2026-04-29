package diagnostics

import "regexp"

var redactionPatterns = []*regexp.Regexp{
	regexp.MustCompile(`Bearer [A-Za-z0-9._\-]+`),
	regexp.MustCompile(`token=[A-Za-z0-9._\-]+`),
	regexp.MustCompile(`password=\S+`),
	regexp.MustCompile(`eyJ[A-Za-z0-9_\-]{20,}\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+`),
}

// RedactString removes common secret/token patterns from a free-form string.
func RedactString(s string) string {
	out := s
	out = redactionPatterns[0].ReplaceAllString(out, "Bearer [REDACTED]")
	out = redactionPatterns[1].ReplaceAllString(out, "token=[REDACTED]")
	out = redactionPatterns[2].ReplaceAllString(out, "password=[REDACTED]")
	out = redactionPatterns[3].ReplaceAllString(out, "[JWT_REDACTED]")
	return out
}
