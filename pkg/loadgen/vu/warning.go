package vu

// ReplyGapWarning returns a warning when reply timing is unlikely to produce
// reply-before-parent given replica stagger. Empty delays returns "".
func ReplyGapWarning(replyWithinMs int, delays []int) string {
	if len(delays) == 0 {
		return ""
	}
	minD, maxD := delays[0], delays[0]
	for _, d := range delays[1:] {
		if d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	stagger := maxD - minD
	if stagger-200 <= replyWithinMs {
		return "reply gap too wide for the configured stagger; reply-before-parent will be rare"
	}
	return ""
}

// ConfigReplyGapWarning applies ReplyGapWarning using the replier's replyWithinMs.
// Returns "" when cfg has no replier behavior.
func ConfigReplyGapWarning(cfg Config, delays []int) string {
	ms := ParseReplierReplyWithinMs(cfg)
	if ms == 0 {
		return ""
	}
	return ReplyGapWarning(ms, delays)
}

// StaggerMs returns max(delays)-min(delays), or 0 when delays is empty.
func StaggerMs(delays []int) int {
	if len(delays) == 0 {
		return 0
	}
	minD, maxD := delays[0], delays[0]
	for _, d := range delays[1:] {
		if d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	return maxD - minD
}
