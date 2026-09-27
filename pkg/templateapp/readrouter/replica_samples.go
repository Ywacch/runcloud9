package readrouter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/consistency"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

const replicaViolationSampleCap = 8

func (r *ReadRouter) ReplicaSamples() []consistency.ReplicaSample {
	if r == nil {
		return nil
	}
	r.sampleMu.RLock()
	defer r.sampleMu.RUnlock()
	out := make([]consistency.ReplicaSample, len(r.violationSamples))
	copy(out, r.violationSamples)
	return out
}

func (r *ReadRouter) recordViolationSample(
	ctx context.Context,
	kind string,
	clientID, replicaHost, tableOrKey string,
	write, observed ConsistencyToken,
) {
	if r == nil || kind == "" {
		return
	}
	sample := consistency.ReplicaSample{
		Kind:          kind,
		TableOrKey:    tableOrKey,
		ReplicaHost:   replicaHost,
		WriteToken:    tokenString(write),
		ObservedToken: tokenString(observed),
		LagBytes:      lagBytesBetween(write, observed),
		At:            time.Now().UTC(),
		RequestID:     trace.RequestID(ctx),
		ClientIDHash:  hashClientID(clientID),
	}
	r.sampleMu.Lock()
	defer r.sampleMu.Unlock()
	r.violationSamples = append([]consistency.ReplicaSample{sample}, r.violationSamples...)
	if len(r.violationSamples) > replicaViolationSampleCap {
		r.violationSamples = r.violationSamples[:replicaViolationSampleCap]
	}
}

func tokenString(t ConsistencyToken) string {
	if t == nil {
		return ""
	}
	return t.String()
}

func lagBytesBetween(write, observed ConsistencyToken) int64 {
	w, okW := write.(lsnToken)
	o, okO := observed.(lsnToken)
	if !okW || !okO {
		return 0
	}
	if uint64(w) <= uint64(o) {
		return 0
	}
	return int64(uint64(w) - uint64(o))
}

func hashClientID(clientID string) string {
	if clientID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(clientID))
	return hex.EncodeToString(sum[:8])
}
