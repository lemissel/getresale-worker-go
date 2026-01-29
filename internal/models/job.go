package models

import "encoding/json"

type Job struct {
	JobId    string          `json:"jobId"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Metadata json.RawMessage `json:"metadata"`
}

type LLMPayload struct {
	Prompt string `json:"prompt"`
	Model  string `json:"model"`
	Format string `json:"format"`
}

type JobResult struct {
	JobId    string          `json:"jobId"`
	Type     string          `json:"type"`
	Result   interface{}     `json:"result"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
	Error    string          `json:"error,omitempty"`
}
