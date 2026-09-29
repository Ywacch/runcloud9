package inference

// ChatMessage is one turn in the inspector chat history loop.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the POST /chat body from cloud9-api.
type ChatRequest struct {
	Context  any           `json:"context"`
	Messages []ChatMessage `json:"messages"`
}

// ChatResponse is the runcloud9-ai response for a chat turn.
type ChatResponse struct {
	Status        string      `json:"status"`
	Reply         string      `json:"reply"`
	Claims        []WireClaim `json:"claims"`
	Usage         TokenUsage  `json:"usage"`
	PromptVersion string      `json:"promptVersion"`
	Model         string      `json:"model"`
}
