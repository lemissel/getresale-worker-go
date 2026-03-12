package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"getresale-worker-go/internal/llm"
	"getresale-worker-go/internal/models"
	"getresale-worker-go/internal/queue"
)

type Worker struct {
	Queue     *queue.RedisManager
	Gemini    llm.Client
	OpenAI    llm.Client
	Semaphore chan struct{}
	Wg        sync.WaitGroup
}

func NewWorker(queueManager *queue.RedisManager, gemini, openai llm.Client, maxConcurrency int) *Worker {
	return &Worker{
		Queue:     queueManager,
		Gemini:    gemini,
		OpenAI:    openai,
		Semaphore: make(chan struct{}, maxConcurrency),
	}
}

func (w *Worker) Start(ctx context.Context) {
	log.Println("Worker started, waiting for messages...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Worker stopping...")
			w.Wg.Wait()
			return
		default:
			messages, err := w.Queue.ReceiveMessages(ctx)
			if err != nil {
				log.Printf("Error receiving messages: %v\n", err)
				continue
			}

			for _, msg := range messages {
				w.Wg.Add(1)
				go w.handleMessage(ctx, msg)
			}
		}
	}
}

func (w *Worker) handleMessage(ctx context.Context, msg queue.Message) {
	defer w.Wg.Done()
	// Acquire semaphore
	w.Semaphore <- struct{}{}
	defer func() { <-w.Semaphore }()

	var job models.Job
	if err := json.Unmarshal([]byte(msg.Body), &job); err != nil {
		log.Printf("Error unmarshaling job: %v\n", err)
		_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, "invalid job payload")
		return
	}

	var payload models.LLMPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		log.Printf("Error unmarshaling payload: %v\n", err)
		_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, "invalid llm payload")
		return
	}

	log.Printf("Processing job %s (%s)...\n", job.JobId, job.Type)

	var result string
	var usage llm.TokenUsage
	var err error

	// Decide which client to use
	if payload.Model == "whisper-1" || payload.Model == "gpt-4o" || payload.Model == "gpt-4" {
		result, usage, err = w.OpenAI.Generate(ctx, payload.Model, payload.Prompt, payload.Format)
	} else {
		// Default to Gemini for everything else
		geminiClient, ok := w.Gemini.(*llm.GeminiClient)
		if !ok {
			// Fallback if not GeminiClient type (should not happen with standard setup)
			result, usage, err = w.Gemini.Generate(ctx, payload.Model, payload.Prompt, payload.Format)
		} else {
			// Context Caching Logic
			var cachedContentName string
			useCache := false

			// Only use cache if CacheKey is provided AND we have content to cache (Messages or Context)
			if payload.CacheKey != "" && (len(payload.Messages) > 0 || payload.Context != "") {
				cacheKey := fmt.Sprintf("llm:cache:%s", payload.CacheKey)
				cachedName, redisErr := w.Queue.Client.Get(ctx, cacheKey).Result()

				if redisErr == nil && cachedName != "" {
					cachedContentName = cachedName
					useCache = true
					log.Printf("Using existing cache for %s: %s", payload.CacheKey, cachedContentName)
				} else {
					// Create Cache
					log.Printf("Creating new cache for %s...", payload.CacheKey)
					
					var contentToCache []llm.GeminiContent
					if len(payload.Messages) > 0 {
						// Minify logic: U: [msg] | A: [resp]
						var sb strings.Builder
						for i, m := range payload.Messages {
							role := "U"
							if m.Role == "model" || m.Role == "assistant" {
								role = "A"
							}
							sb.WriteString(fmt.Sprintf("%s: [%s]", role, m.Content))
							if i < len(payload.Messages)-1 {
								sb.WriteString(" | ")
							}
						}
						
						contentToCache = []llm.GeminiContent{
							{
								Parts: []llm.GeminiPart{{Text: sb.String()}},
								Role:  "user", // Cache content is usually user context
							},
						}
					} else {
						// Use raw Context string
						contentToCache = []llm.GeminiContent{
							{
								Parts: []llm.GeminiPart{{Text: payload.Context}},
								Role:  "user",
							},
						}
					}

					// TTL 300s (5 minutes)
					ttl := 300
					cache, createErr := geminiClient.CreateCache(ctx, payload.Model, payload.CacheKey, contentToCache, ttl)
					if createErr != nil {
						log.Printf("Failed to create cache: %v. Proceeding without cache.", createErr)
					} else {
						cachedContentName = cache.Name
						useCache = true
						// Store in Redis
						w.Queue.Client.Set(ctx, cacheKey, cachedContentName, time.Duration(ttl)*time.Second)
						log.Printf("Cache created: %s", cachedContentName)
					}
				}
			}

			if useCache {
				result, usage, err = geminiClient.GenerateWithCache(ctx, payload.Model, payload.Prompt, payload.Format, cachedContentName)
				// If cache not found (expired/deleted on server but exists in Redis), retry without cache or recreate?
				// GenerateWithCache will return 404 if cache is missing.
				// For now, if it fails, we should ideally fallback to full generation if we have the content.
				// But simpler to just let it fail and let retry logic handle it (or implement fallback here).
				// Given we have the content, we COULD fallback.
				if err != nil && strings.Contains(err.Error(), "404") {
					log.Printf("Cache %s not found (404), falling back to standard generation...", cachedContentName)
					// Invalidate Redis
					w.Queue.Client.Del(ctx, fmt.Sprintf("llm:cache:%s", payload.CacheKey))
					
					// Re-construct full prompt?
					// If we used cache, the 'prompt' passed to GenerateWithCache was just the Task.
					// We need to combine Task + Context for fallback.
					fullPrompt := payload.Prompt
					if len(payload.Messages) > 0 {
						var sb strings.Builder
						for _, m := range payload.Messages {
							sb.WriteString(fmt.Sprintf("%s: %s\n", m.Role, m.Content))
						}
						fullPrompt = fmt.Sprintf("%s\n\nContext:\n%s", payload.Prompt, sb.String())
					} else if payload.Context != "" {
						fullPrompt = fmt.Sprintf("%s\n\nContext:\n%s", payload.Prompt, payload.Context)
					}
					
					result, usage, err = geminiClient.Generate(ctx, payload.Model, fullPrompt, payload.Format)
				}
			} else {
				// Standard Generation (No Cache)
				// Construct full prompt if Messages/Context are provided but caching failed or wasn't used
				fullPrompt := payload.Prompt
				if len(payload.Messages) > 0 {
					var sb strings.Builder
					for _, m := range payload.Messages {
						sb.WriteString(fmt.Sprintf("%s: %s\n", m.Role, m.Content))
					}
					fullPrompt = fmt.Sprintf("%s\n\nContext:\n%s", payload.Prompt, sb.String())
				} else if payload.Context != "" {
					fullPrompt = fmt.Sprintf("%s\n\nContext:\n%s", payload.Prompt, payload.Context)
				}
				
				result, usage, err = geminiClient.Generate(ctx, payload.Model, fullPrompt, payload.Format)
			}
		}
	}

	jobResult := models.JobResult{
		JobId:    job.JobId,
		Type:     job.Type,
		Metadata: job.Metadata,
	}

	if err != nil {
		log.Printf("Error processing job %s: %v\n", job.JobId, err)
		jobResult.Error = err.Error()
		_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, err.Error())
		
		// Send error result as well
		_ = w.Queue.SendResult(ctx, job.JobId, jobResult)
	} else {
		jobResult.Result = result
		jobResult.Usage = &models.Usage{
			PromptTokens:     usage.PromptTokens,
			CandidateTokens:  usage.CandidateTokens,
			TotalTokens:      usage.TotalTokens,
			CachedTokens:     usage.CachedTokens,
			EstimatedCostUSD: usage.EstimatedCostUSD,
			Model:            payload.Model,
		}
		if err := w.Queue.SendResult(ctx, job.JobId, jobResult); err != nil {
			log.Printf("Error sending result for job %s: %v\n", job.JobId, err)
			_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, "failed to enqueue output")
			return
		}
		if err := w.Queue.CompleteJob(ctx, msg.JobID, result); err != nil {
			log.Printf("Error completing job %s: %v\n", job.JobId, err)
		}
		log.Printf("Job %s completed successfully.\n", job.JobId)
	}
}
