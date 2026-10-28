package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
	"github.com/ywacch/cloud9/pkg/templates"
)

const chatMaxUserMessageRunes = 4000

// handleChatStream proxies inspector chat over WebSocket. Go injects fresh AIContext
// each turn; claims are verifier-gated before they reach the client.
func (h *Handler) handleChatStream(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	row := h.requireOwnership(w, r, namespace)
	if row == nil {
		return
	}
	if row.Status == "terminated" {
		writeError(w, http.StatusConflict, "session ended")
		return
	}
	if !h.inferenceEnabledForUser(r.Context(), row.UserID) {
		writeError(w, http.StatusForbidden, "inference is limited to internal accounts")
		return
	}
	if h.inferenceClient == nil {
		writeError(w, http.StatusServiceUnavailable, "inference unavailable")
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("chat ws upgrade %s: %v", namespace, err)
		return
	}
	defer conn.Close()

	ctx, cancel := cancelWhenSocketCloses(r.Context(), conn)
	defer cancel()

	h.sendChatSessionState(conn, namespace)

	var history []inference.ChatMessage
	for {
		if ctx.Err() != nil {
			return
		}
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var inbound chatClientMessage
		if err := json.Unmarshal(payload, &inbound); err != nil {
			_ = writeChatFrame(conn, chatErrorFrame("invalid_message", "message must be json"))
			continue
		}
		switch inbound.Type {
		case "ping":
			_ = writeChatFrame(conn, map[string]string{"type": "pong"})
			continue
		case "user_message":
			text := strings.TrimSpace(inbound.Text)
			if text == "" {
				_ = writeChatFrame(conn, chatErrorFrame("empty_message", "message text is required"))
				continue
			}
			if utf8.RuneCountInString(text) > chatMaxUserMessageRunes {
				_ = writeChatFrame(conn, chatErrorFrame("message_too_long", "message exceeds size limit"))
				continue
			}
			if h.inferenceTokenBudget != nil && h.inferenceTokenBudget.Remaining(namespace) <= 0 {
				_ = writeChatFrame(conn, chatErrorFrame("budget_exhausted", "session inference token budget exhausted"))
				continue
			}
			history = append(history, inference.ChatMessage{Role: "user", Content: text})
			history, err = h.runChatTurn(ctx, conn, namespace, row, history)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("chat %s: %v", namespace, err)
				_ = writeChatFrame(conn, chatErrorFrame("inference_error", "chat request failed"))
				if len(history) > 0 {
					history = history[:len(history)-1]
				}
			}
		default:
			_ = writeChatFrame(conn, chatErrorFrame("unknown_type", "unsupported message type"))
		}
	}
}

func (h *Handler) sendChatSessionState(conn *websocket.Conn, namespace string) {
	limit := h.inferenceTokenBudget.Limit()
	spent := h.inferenceTokenBudget.Spent(namespace)
	remaining := h.inferenceTokenBudget.Remaining(namespace)
	_ = writeChatFrame(conn, map[string]any{
		"type":            "session",
		"budgetLimit":     limit,
		"budgetSpent":     spent,
		"budgetRemaining": remaining,
		"budgetExhausted": remaining <= 0,
	})
}

func (h *Handler) runChatTurn(
	ctx context.Context,
	conn *websocket.Conn,
	namespace string,
	row *store.DeploymentRow,
	history []inference.ChatMessage,
) ([]inference.ChatMessage, error) {
	design, _ := deployer.DesignFromMap(row.Design)
	if design == nil {
		design = &deployer.DeploymentDesign{}
	}
	dc := deployer.DesignConfigFromDesign(design)
	var tmpl *templates.TemplateSpec
	if row.Template != "" {
		tmpl = templates.GetTemplate(row.Template)
	}

	aiCtx, ledger, fired := h.liveChatAIContext(namespace, row.Template, dc, design, tmpl)

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	resp, err := h.inferenceClient.Chat(reqCtx, inference.ChatRequest{
		Context:  aiCtx,
		Messages: history,
	})
	if err != nil {
		return history, err
	}

	tokens := resp.Usage.TotalTokens
	if tokens <= 0 {
		tokens = resp.Usage.PromptTokens + resp.Usage.CompletionTokens
	}
	if tokens > 0 && !h.inferenceTokenBudget.TrySpend(namespace, tokens) {
		_ = writeChatFrame(conn, chatErrorFrame("budget_exhausted", "session inference token budget exhausted"))
		return history, nil
	}

	reply := strings.TrimSpace(resp.Reply)
	if reply == "" && resp.Status == "skipped" {
		reply = "Inference is not configured for this environment."
	}

	verified := FilterVerifiedPostmortemClaims(resp.Claims, ledger, dc, fired)

	_ = writeChatFrame(conn, map[string]string{"type": "assistant_start"})
	streamChatReply(conn, reply)
	for _, claim := range verified {
		_ = writeChatFrame(conn, map[string]any{
			"type":  "assistant_claim",
			"claim": claim,
		})
	}
	_ = writeChatFrame(conn, map[string]any{
		"type":            "assistant_end",
		"reply":           reply,
		"claims":          verified,
		"status":          resp.Status,
		"budgetSpent":     h.inferenceTokenBudget.Spent(namespace),
		"budgetRemaining": h.inferenceTokenBudget.Remaining(namespace),
	})

	if reply != "" {
		history = append(history, inference.ChatMessage{Role: "assistant", Content: reply})
	}
	return history, nil
}

// liveChatAIContext builds AIContext and verifier inputs for an active session.
func (h *Handler) liveChatAIContext(
	namespace, template string,
	dc deployer.DesignConfig,
	design *deployer.DeploymentDesign,
	tmpl *templates.TemplateSpec,
) (AIContext, *metrics.Ledger, []string) {
	var hist []metrics.DeploymentMetrics
	var latest *metrics.DeploymentMetrics
	var traceSamples []trace.JourneySample
	if h.metricsCollector != nil {
		hist = h.metricsCollector.DownsampledSessionHistory(namespace)
		if len(hist) == 0 {
			hist = h.metricsCollector.SessionHistory(namespace)
		}
		latest = h.metricsCollector.Latest(namespace)
		traceSamples = h.metricsCollector.TraceSamplesFor(namespace)
	}
	final := finalSnapshot(hist, latest)
	violations := metrics.BuildViolations(hist, dc, tmpl)
	fired := metrics.FiredRuleIDs(hist, violations)
	ledger := metrics.BuildLedger(metrics.LedgerInput{
		DesignConfig: dc,
		Design:       design,
		Hist:         hist,
		Final:        final,
		Template:     tmpl,
		TraceSamples: traceSamples,
	})
	aiCtx := BuildAIContext(AIContextInput{
		SessionID:    namespace,
		DesignConfig: dc,
		Design:       design,
		Hist:         hist,
		Final:        final,
		Template:     tmpl,
		TraceSamples: traceSamples,
	})
	return aiCtx, ledger, fired
}

func streamChatReply(conn *websocket.Conn, reply string) {
	const chunkRunes = 32
	runes := []rune(reply)
	for i := 0; i < len(runes); i += chunkRunes {
		end := i + chunkRunes
		if end > len(runes) {
			end = len(runes)
		}
		part := string(runes[i:end])
		if part == "" {
			continue
		}
		_ = writeChatFrame(conn, map[string]string{
			"type": "assistant_delta",
			"text": part,
		})
	}
}

type chatClientMessage struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func chatErrorFrame(code, message string) map[string]string {
	return map[string]string{
		"type":    "error",
		"code":    code,
		"message": message,
	}
}

func writeChatFrame(conn *websocket.Conn, v any) error {
	return conn.WriteJSON(v)
}
