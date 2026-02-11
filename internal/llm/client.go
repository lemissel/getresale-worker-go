package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
