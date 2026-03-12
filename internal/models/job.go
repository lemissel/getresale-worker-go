package models

import "encoding/json"

type Job struct {
	JobId    string          `json:"jobId"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Metadata json.RawMessage `json:"metadata"`
}

type LLMPayload struct {
	Prompt   string `json:"prompt"`
	Model    string `json:"model"`
	Format   string `json:"format"`
	Context  string `json:"context,omitempty"`   // For raw text context to be cached
	Messages []struct {                          // For structured messages to be cached/minified
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages,omitempty"`
	CacheKey string `json:"cacheKey,omitempty"` // remoteJid or unique ID for caching
}

type Usage struct {
	PromptTokens     int     `json:"prompt_tokens"`
	CandidateTokens  int     `json:"candidate_tokens"`
	TotalTokens      int     `json:"total_tokens"`
	CachedTokens     int     `json:"cached_tokens"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd"`
	Model            string  `json:"model,omitempty"`
}

type JobResult struct {
	JobId    string          `json:"jobId"`
	Type     string          `json:"type"`
	Result   interface{}     `json:"result"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
	Error    string          `json:"error,omitempty"`
	Usage    *Usage          `json:"usage,omitempty"`
}
