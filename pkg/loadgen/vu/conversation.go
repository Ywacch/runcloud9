package vu

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const (
	actionCreateRoot  = "create_root"
	actionCreateReply = "create_reply"

	behaviorConversationStarter = "conversation_starter"
	behaviorReplier             = "replier"
)

type starterParams struct {
	ThinkMs int `json:"thinkMs"`
}

type replierParams struct {
	ReplyWithinMs int `json:"replyWithinMs"`
	ThinkMs       int `json:"thinkMs"`
}

type conversationStarter struct {
	think time.Duration
}

func (conversationStarter) Name() string { return behaviorConversationStarter }

func (b conversationStarter) Run(ctx context.Context, vu *VU) error {
	const name = behaviorConversationStarter
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		vu.seq++
		id, err := doAction(ctx, vu, actionCreateRoot, Vars{
			UserID: vu.UserID,
			Seq:    vu.seq,
			Rand:   vu.RNG.Intn(1_000_000),
		})
		if err != nil {
			vu.Stats.IncError(name)
		} else {
			vu.Stats.IncRequest(name)
			if vu.Shared != nil && vu.Shared.Roots != nil {
				vu.Shared.Roots.Publish(RootPost{ID: id, PostedAt: vu.Now()})
			}
		}
		if err := thinkWithJitter(ctx, vu, b.think); err != nil {
			return err
		}
	}
}

type replier struct {
	replyWithin time.Duration
	think       time.Duration
}

func (replier) Name() string { return behaviorReplier }

func (b replier) Run(ctx context.Context, vu *VU) error {
	const name = behaviorReplier
	// Do not wait for the parent to be visible on a replica; that would push the
	// reply gap past the configured stagger and hide reply-before-parent.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if vu.Shared == nil || vu.Shared.Roots == nil {
			if err := vu.Sleep(ctx, 50*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		root, ok := vu.Shared.Roots.TakeForReply()
		if !ok {
			if err := vu.Sleep(ctx, 50*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		delay := time.Duration(vu.RNG.Int63n(int64(b.replyWithin)))
		if delay > 0 {
			if err := vu.Sleep(ctx, delay); err != nil {
				return err
			}
		}
		_, err := doAction(ctx, vu, actionCreateReply, Vars{
			UserID:   vu.UserID,
			ParentID: root.ID,
			Rand:     vu.RNG.Intn(1_000_000),
		})
		if err != nil {
			vu.Stats.IncError(name)
		} else {
			vu.Stats.IncRequest(name)
			vu.Stats.ObserveGap(vu.Now().Sub(root.PostedAt))
		}
		if err := thinkWithJitter(ctx, vu, b.think); err != nil {
			return err
		}
	}
}

func newConversationStarter(params json.RawMessage) (Behavior, error) {
	p := starterParams{ThinkMs: 2000}
	if len(params) > 0 {
		dec := json.NewDecoder(bytesReader(params))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("vu: conversation_starter params: %w", err)
		}
	}
	if p.ThinkMs < 200 || p.ThinkMs > 20000 {
		return nil, fmt.Errorf("vu: conversation_starter thinkMs out of range")
	}
	return conversationStarter{think: time.Duration(p.ThinkMs) * time.Millisecond}, nil
}

func newReplier(params json.RawMessage) (Behavior, error) {
	p := replierParams{ReplyWithinMs: 300, ThinkMs: 1000}
	if len(params) > 0 {
		dec := json.NewDecoder(bytesReader(params))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("vu: replier params: %w", err)
		}
	}
	if p.ReplyWithinMs < 50 || p.ReplyWithinMs > 2000 {
		return nil, fmt.Errorf("vu: replier replyWithinMs out of range")
	}
	if p.ThinkMs < 100 || p.ThinkMs > 10000 {
		return nil, fmt.Errorf("vu: replier thinkMs out of range")
	}
	return replier{
		replyWithin: time.Duration(p.ReplyWithinMs) * time.Millisecond,
		think:       time.Duration(p.ThinkMs) * time.Millisecond,
	}, nil
}

func bytesReader(b []byte) *jsonReader {
	return &jsonReader{b: b}
}

type jsonReader struct {
	b []byte
}

func (r *jsonReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

// DefaultReplyWithinMs is the replier default used by ConfigReplyGapWarning.
const DefaultReplyWithinMs = 300

// ParseReplierReplyWithinMs returns replyWithinMs from cfg, or DefaultReplyWithinMs when no replier is configured.
func ParseReplierReplyWithinMs(cfg Config) int {
	for _, b := range cfg.Behaviors {
		if b.Name != behaviorReplier {
			continue
		}
		p := replierParams{ReplyWithinMs: DefaultReplyWithinMs, ThinkMs: 1000}
		if len(b.Params) > 0 {
			dec := json.NewDecoder(bytesReader(b.Params))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&p); err != nil {
				return DefaultReplyWithinMs
			}
		}
		return p.ReplyWithinMs
	}
	return 0
}
