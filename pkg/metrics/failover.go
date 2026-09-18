package metrics

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ywacch/cloud9/pkg/rules"
)

// RuleFailoverWriteLoss is emitted when promotion promotes a standby behind the fenced primary.
const RuleFailoverWriteLoss = rules.IDFailoverWriteLoss

// FailoverWriteLossEvent builds a failover data-loss invariant when walBytesLost is positive.
// Zero loss returns nil so the UI renders from chaos_inject detail instead.
func FailoverWriteLossEvent(
	now time.Time,
	scenario, promotedPod, syncState string,
	walBytesLost int64,
	replicationMode, syncCommitLevel string,
) *Event {
	if walBytesLost <= 0 {
		return nil
	}
	desc := fmt.Sprintf(
		"Promoted %s was %s bytes behind the fenced primary; commits the client already saw may be gone",
		promotedPod,
		strconv.FormatInt(walBytesLost, 10),
	)
	return &Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "chaos",
		Component:   "db",
		RuleID:      RuleFailoverWriteLoss,
		Audience:    "student",
		Title:       "Failover write loss",
		Description: desc,
		Detail: map[string]string{
			"scenario":        scenario,
			"promotedPod":     promotedPod,
			"syncState":       syncState,
			"walBytesLost":    strconv.FormatInt(walBytesLost, 10),
			"replicationMode": replicationMode,
			"syncCommitLevel": syncCommitLevel,
		},
	}
}
