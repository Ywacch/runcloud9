package api

import (
	"context"

	"github.com/ywacch/cloud9/pkg/inference"
)

// inferenceService calls runcloud9-ai for reel captions and post-mortems.
type inferenceService interface {
	ReelCaptions(ctx context.Context, req inference.ReelCaptionsRequest) (*inference.ReelCaptionsResponse, error)
	Postmortem(ctx context.Context, aiContext any) (*inference.PostmortemResponse, error)
}
