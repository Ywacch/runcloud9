package vu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const (
	actionReadThread         = "read_thread"
	behaviorThreadObserver   = "thread_observer"
	observerRequestHeader    = "X-C9-Observer"
	observerRequestHeaderVal = "1"

	defaultReadsPerSec     = 2.0
	minReadsPerSec         = 0.1
	maxReadsPerSec         = 10.0
	minObserverMaxAgeMs    = 100
	maxObserverMaxAgeMs    = 10000
	maxObservationExamples = 5
)

type observerParams struct {
	ReadsPerSec    float64 `json:"readsPerSec"`
	MaxThreadAgeMs int     `json:"maxThreadAgeMs"`
}

// threadPost is the platform thread-read contract: each post has id and reply_to_id.
type threadPost struct {
	ID        int  `json:"id"`
	ReplyToID *int `json:"reply_to_id"`
}

type threadReadResponse struct {
	RootID   int          `json:"rootId"`
	Replies  []threadPost `json:"replies"`
	Parents  []threadPost `json:"parents"`
	ServedBy struct {
		Replies string `json:"replies"`
		Parents string `json:"parents"`
	} `json:"servedBy"`
}

// orphan is a reply whose reply_to_id is not present among parent posts.
type orphan struct {
	Reply    threadPost
	ParentID int
}

// FindOrphans returns replies whose reply_to_id is not among parent ids.
func FindOrphans(replies, parents []threadPost) []orphan {
	parentIDs := make(map[int]struct{}, len(parents))
	for _, p := range parents {
		parentIDs[p.ID] = struct{}{}
	}
	var out []orphan
	for _, r := range replies {
		if r.ReplyToID == nil {
			continue
		}
		pid := *r.ReplyToID
		if _, ok := parentIDs[pid]; !ok {
			out = append(out, orphan{Reply: r, ParentID: pid})
		}
	}
	return out
}

// ApplyObserverStaggerMaxAge sets maxThreadAgeMs from staggerMs when an observer
// behavior leaves it at zero (API resolution before the loadgen pod starts).
func ApplyObserverStaggerMaxAge(cfg Config, staggerMs int) Config {
	out := cfg
	if len(cfg.Behaviors) == 0 {
		return out
	}
	behaviors := make([]BehaviorConfig, len(cfg.Behaviors))
	for i, b := range cfg.Behaviors {
		behaviors[i] = b
		if b.Name != behaviorThreadObserver {
			continue
		}
		p := observerParams{ReadsPerSec: defaultReadsPerSec}
		if len(b.Params) > 0 {
			_ = json.Unmarshal(b.Params, &p)
		}
		if p.MaxThreadAgeMs != 0 {
			continue
		}
		p.MaxThreadAgeMs = staggerMs
		raw, err := json.Marshal(p)
		if err != nil {
			continue
		}
		behaviors[i].Params = raw
	}
	out.Behaviors = behaviors
	return out
}

type threadObserver struct {
	interval time.Duration
	maxAge   time.Duration
}

func (threadObserver) Name() string { return behaviorThreadObserver }

func newThreadObserver(params json.RawMessage) (Behavior, error) {
	p := observerParams{ReadsPerSec: defaultReadsPerSec}
	if len(params) > 0 {
		dec := json.NewDecoder(bytes.NewReader(params))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("vu: thread_observer params: %w", err)
		}
	}
	if p.ReadsPerSec <= 0 {
		p.ReadsPerSec = defaultReadsPerSec
	}
	if p.ReadsPerSec < minReadsPerSec || p.ReadsPerSec > maxReadsPerSec {
		return nil, fmt.Errorf("vu: thread_observer readsPerSec out of range")
	}
	if p.MaxThreadAgeMs != 0 && (p.MaxThreadAgeMs < minObserverMaxAgeMs || p.MaxThreadAgeMs > maxObserverMaxAgeMs) {
		return nil, fmt.Errorf("vu: thread_observer maxThreadAgeMs out of range")
	}
	interval := time.Duration(float64(time.Second) / p.ReadsPerSec)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	return threadObserver{
		interval: interval,
		maxAge:   time.Duration(p.MaxThreadAgeMs) * time.Millisecond,
	}, nil
}

func (b threadObserver) Run(ctx context.Context, vu *VU) error {
	const name = behaviorThreadObserver
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := b.tick(ctx, vu, name); err != nil {
				return err
			}
		}
	}
}

func (b threadObserver) tick(ctx context.Context, vu *VU, name string) error {
	if vu.Shared == nil || vu.Shared.Roots == nil {
		return nil
	}
	now := vu.Now()
	inWindow := false
	var root RootPost
	young := vu.Shared.Roots.Recent(b.maxAge, now)
	if len(young) > 0 {
		root = young[vu.RNG.Intn(len(young))]
		inWindow = true
	} else {
		r, ok := vu.Shared.Roots.Newest()
		if !ok {
			return nil
		}
		root = r
	}

	resp, reqID, err := readThread(ctx, vu, root.ID)
	if err != nil {
		vu.Stats.IncObserverError()
		vu.Stats.IncError(name)
		return nil
	}
	vu.Stats.IncRequest(name)
	vu.Stats.IncObserverRead(inWindow)

	orphans := FindOrphans(resp.Replies, resp.Parents)
	if len(orphans) == 0 {
		return nil
	}
	vu.Stats.IncObserverViolation()
	o := orphans[0]
	ex := ObservationExample{
		At:        now,
		RootID:    resp.RootID,
		ReplyID:   o.Reply.ID,
		ParentID:  o.ParentID,
		RequestID: reqID,
	}
	ex.ServedBy.Replies = resp.ServedBy.Replies
	ex.ServedBy.Parents = resp.ServedBy.Parents
	vu.Stats.PrependObserverExample(ex)
	return nil
}

func readThread(ctx context.Context, vu *VU, rootID string) (threadReadResponse, string, error) {
	a, ok := vu.Actions[actionReadThread]
	if !ok {
		return threadReadResponse{}, "", fmt.Errorf("vu: action %q not configured", actionReadThread)
	}
	if !ValidID(rootID) {
		return threadReadResponse{}, "", fmt.Errorf("vu: unsafe root id %q", rootID)
	}
	path, body := a.render(Vars{RootID: rootID})
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(a.Method), vu.BaseURL+path, reader)
	if err != nil {
		return threadReadResponse{}, "", fmt.Errorf("vu: build read_thread request: %w", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(observerRequestHeader, observerRequestHeaderVal)
	reqID := fmt.Sprintf("vu%d-%016x", vu.ID, rand.Uint64())
	req.Header.Set("X-Request-ID", reqID)
	if vu.ApplyIdentity != nil {
		vu.ApplyIdentity(req.Header, vu.UserID)
	}

	httpResp, err := vu.Client.Do(req)
	if err != nil {
		return threadReadResponse{}, "", fmt.Errorf("vu: read_thread: %w", err)
	}
	defer httpResp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(httpResp.Body, maxResponseBytes))
	if err != nil {
		return threadReadResponse{}, "", fmt.Errorf("vu: read_thread: read body: %w", err)
	}
	if httpResp.StatusCode < http.StatusOK || httpResp.StatusCode >= http.StatusMultipleChoices {
		return threadReadResponse{}, "", fmt.Errorf("vu: read_thread: status %d", httpResp.StatusCode)
	}
	var out threadReadResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return threadReadResponse{}, "", fmt.Errorf("vu: read_thread: decode: %w", err)
	}
	return out, reqID, nil
}
