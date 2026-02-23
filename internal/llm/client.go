package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client interface {
	Generate(ctx context.Context, model, prompt, format string) (string, error)
}

// OllamaClient
type OllamaClient struct {
	BaseURL string
	HTTP    *http.Client
}

func NewOllamaClient(baseURL string) *OllamaClient {
	return &OllamaClient{
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 5 * time.Minute},
	}
}

type ollamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
	Format string `json:"format,omitempty"`
}

type ollamaResponse struct {
	Response string `json:"response"`
}

func (c *OllamaClient) Generate(ctx context.Context, model, prompt, format string) (string, error) {
	reqBody := ollamaRequest{
		Model:  model,
		Prompt: prompt,
		Stream: false,
		Format: format,
	}
	data, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/api/generate", c.BaseURL), bytes.NewBuffer(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ollama error (status %d): %s", resp.StatusCode, string(body))
	}

	var res ollamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}

	return res.Response, nil
}

// GeminiClient
type GeminiClient struct {
	APIKey       string
	BaseURL      string
	DefaultModel string
	HTTP         *http.Client
}

func NewGeminiClient(apiKey, defaultModel string) *GeminiClient {
	if defaultModel == "" {
		defaultModel = "gemini-3-flash-preview"
	}
	return &GeminiClient{
		APIKey:       apiKey,
		BaseURL:      "https://generativelanguage.googleapis.com/v1beta",
		DefaultModel: defaultModel,
		HTTP:         &http.Client{Timeout: 5 * time.Minute},
	}
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiGenerationConfig struct {
	ResponseMimeType string `json:"responseMimeType,omitempty"`
}

type geminiRequest struct {
	Contents         []geminiContent         `json:"contents"`
	GenerationConfig *geminiGenerationConfig `json:"generationConfig,omitempty"`
}

type geminiCandidate struct {
	Content geminiContent `json:"content"`
}

type geminiResponse struct {
	Candidates []geminiCandidate `json:"candidates"`
}

func (c *GeminiClient) Generate(ctx context.Context, model, prompt, format string) (string, error) {
	// Use default model if not provided or if it's a legacy Llama model name
	if model == "" || model == "llama3.2" || model == "llama2" {
		model = c.DefaultModel
	}

	reqBody := geminiRequest{
		Contents: []geminiContent{
			{
				Parts: []geminiPart{
					{Text: prompt},
				},
			},
		},
	}

	if format == "json" {
		reqBody.GenerationConfig = &geminiGenerationConfig{
			ResponseMimeType: "application/json",
		}
	}

	data, _ := json.Marshal(reqBody)

	url := fmt.Sprintf("%s/models/%s:generateContent?key=%s", c.BaseURL, model, c.APIKey)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		// Redact key from URL for logging
		redactedURL := strings.ReplaceAll(url, c.APIKey, "REDACTED")
		return "", fmt.Errorf("gemini error (status %d) at %s: %s", resp.StatusCode, redactedURL, string(body))
	}

	var res geminiResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}

	if len(res.Candidates) == 0 || len(res.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("gemini returned no content")
	}

	return res.Candidates[0].Content.Parts[0].Text, nil
}

// OpenAIClient (Simple HTTP implementation to avoid extra dependencies for now)
type OpenAIClient struct {
	APIKey  string
	BaseURL string
	HTTP    *http.Client
}

func NewOpenAIClient(apiKey, baseURL string) *OpenAIClient {
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	return &OpenAIClient{
		APIKey:  apiKey,
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 5 * time.Minute},
	}
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRequest struct {
	Model    string          `json:"model"`
	Messages []openAIMessage `json:"messages"`
}

type openAIChoice struct {
	Message openAIMessage `json:"message"`
}

type openAIResponse struct {
	Choices []openAIChoice `json:"choices"`
}

func (c *OpenAIClient) Generate(ctx context.Context, model, prompt, format string) (string, error) {
	reqBody := openAIRequest{
		Model: model,
		Messages: []openAIMessage{
			{Role: "user", Content: prompt},
		},
	}
	data, _ := json.Marshal(reqBody)

	req, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/chat/completions", c.BaseURL), bytes.NewBuffer(data))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.APIKey))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("openai error (status %d): %s", resp.StatusCode, string(body))
	}

	var res openAIResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}

	if len(res.Choices) == 0 {
		return "", fmt.Errorf("openai returned no choices")
	}

	return res.Choices[0].Message.Content, nil
}
