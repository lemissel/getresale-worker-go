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

type OpportunityWorker struct {
	Queue     *queue.RedisManager
	Gemini    llm.Client
	ModelName string
	Semaphore chan struct{}
	Wg        sync.WaitGroup
}

func NewOpportunityWorker(queueManager *queue.RedisManager, gemini llm.Client, maxConcurrency int, modelName string) *OpportunityWorker {
	return &OpportunityWorker{
		Queue:     queueManager,
		Gemini:    gemini,
		ModelName: modelName,
		Semaphore: make(chan struct{}, maxConcurrency),
	}
}

func (w *OpportunityWorker) Start(ctx context.Context) {
	log.Println("OpportunityWorker started...")

	// Start delayed job scheduler
	w.Queue.StartDelayedJobScheduler(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("OpportunityWorker stopping...")
			w.Wg.Wait()
			return
		default:
			messages, err := w.Queue.ReceiveMessages(ctx)
			if err != nil {
				// Avoid busy loop on error
				time.Sleep(1 * time.Second)
				continue
			}

			for _, msg := range messages {
				// Acquire semaphore before spawning goroutine to control concurrency
				select {
				case w.Semaphore <- struct{}{}:
				case <-ctx.Done():
					return
				}

				w.Wg.Add(1)
				go w.handleMessage(ctx, msg)
			}
		}
	}
}

func (w *OpportunityWorker) handleMessage(ctx context.Context, msg queue.Message) {
	defer w.Wg.Done()
	defer func() { <-w.Semaphore }() // Release semaphore when done

	// Parse the Job envelope first
	var job models.Job
	if err := json.Unmarshal([]byte(msg.Body), &job); err != nil {
		log.Printf("Error unmarshaling job envelope: %v\n", err)
		_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, "invalid job envelope")
		return
	}

	// Parse Metadata to preserve context (companyId, etc.)
	var metadata map[string]interface{}
	if len(job.Metadata) > 0 {
		if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
			log.Printf("Warning: failed to parse job metadata: %v\n", err)
			metadata = make(map[string]interface{})
		} else if metadata == nil {
			metadata = make(map[string]interface{})
		}
	} else {
		metadata = make(map[string]interface{})
	}

	var payload struct {
		ID       string `json:"id"`
		Content  string `json:"content"`
		Services []struct {
			Name        string  `json:"name"`
			Description string  `json:"description"`
			Price       float64 `json:"price,omitempty"`
		} `json:"services"`
		Context []struct {
			Sender    string `json:"sender"`
			Content   string `json:"content"`
			Timestamp string `json:"timestamp"`
		} `json:"context"`
	}

	// Unmarshal the inner payload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		log.Printf("Error unmarshaling inner payload: %v\n", err)
		_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, "invalid inner payload")
		return
	}

	// Idempotency check using Redis
	lockKey := fmt.Sprintf("processing:opportunity:%s", payload.ID)
	processed, err := w.Queue.Client.SetNX(ctx, lockKey, "1", 1*time.Hour).Result()
	if err != nil {
		log.Printf("Error checking idempotency for ID %s: %v\n", payload.ID, err)
	} else if !processed {
		log.Printf("Skipping duplicate opportunity analysis for ID: %s\n", payload.ID)
		_ = w.Queue.CompleteJob(ctx, msg.JobID, "{\"status\":\"skipped\", \"reason\":\"duplicate\"}")
		return
	}

	log.Printf("Processing opportunity analysis for ID: %s\n", payload.ID)

	// Format Services
	var servicesList []string
	for _, s := range payload.Services {
		priceStr := ""
		if s.Price > 0 {
			priceStr = fmt.Sprintf(" (R$ %.2f)", s.Price)
		}
		servicesList = append(servicesList, fmt.Sprintf("%s%s", s.Name, priceStr))
	}
	servicesStr := strings.Join(servicesList, ", ")

	// New Prompt (Token-Efficient)
	systemPrompt := fmt.Sprintf("Task: Sales Strategist. Identify upsell/cross-sell from 'SERVICES': %s. Output JSON (PT-BR values): {\"executive_summary\":string,\"mood\":string,\"satisfaction\":string,\"notes\":string,\"opportunities_risks\":string,\"recommended_action_title\":string,\"upsell_potential_brl\":float,\"close_prob\":string,\"funnel_stage\":string,\"market_forecast\":string} Rules: sum service prices in 'upsell_potential_brl'. 'close_prob' ex: 'Alta (73%%)'. 'funnel_stage' ex: 'Negociação'. 'market_forecast': trends. JSON only.", servicesStr)

	// Context Caching Logic
	var cachedContentName string
	var cachedMsgCount int
	var contextContent string // Used for fallback or non-cache mode
	useCache := false
	cacheKey := payload.ID // Default to messageId
	if rJid, ok := metadata["remoteJid"].(string); ok && rJid != "" {
		cacheKey = rJid
	}

	geminiClient, ok := w.Gemini.(*llm.GeminiClient)
	if ok && len(payload.Context) > 0 {
		redisCacheKey := fmt.Sprintf("llm:cache:%s", cacheKey)
		cachedData, redisErr := w.Queue.Client.Get(ctx, redisCacheKey).Result()

		if redisErr == nil && cachedData != "" {
			var cacheInfo struct {
				Name  string `json:"name"`
				Count int    `json:"count"`
			}
			if err := json.Unmarshal([]byte(cachedData), &cacheInfo); err == nil && cacheInfo.Name != "" {
				cachedContentName = cacheInfo.Name
				cachedMsgCount = cacheInfo.Count
				useCache = true
				log.Printf("Using existing cache for %s: %s (count: %d)", cacheKey, cachedContentName, cachedMsgCount)
			}
		}

		// If cache is missing or invalid, create new
		if !useCache {
			log.Printf("Creating new cache for %s...", cacheKey)

			// Build context string for cache
			var sb strings.Builder
			for i, msg := range payload.Context {
				role := "U"
				senderLower := strings.ToLower(msg.Sender)
				if strings.Contains(senderLower, "agent") || strings.Contains(senderLower, "atendente") || strings.Contains(senderLower, "me") {
					role = "A"
				}
				sb.WriteString(fmt.Sprintf("%s: [%s]", role, msg.Content))
				if i < len(payload.Context)-1 {
					sb.WriteString(" | ")
				}
			}
			fullContext := sb.String()

			contentToCache := []llm.GeminiContent{
				{
					Parts: []llm.GeminiPart{{Text: fullContext}},
					Role:  "user",
				},
			}
			ttl := 300
			cache, createErr := geminiClient.CreateCache(ctx, w.ModelName, cacheKey, contentToCache, ttl)
			if createErr != nil {
				log.Printf("Failed to create cache: %v. Proceeding without cache.", createErr)
				contextContent = fullContext // Fallback
			} else {
				cachedContentName = cache.Name
				cachedMsgCount = len(payload.Context)
				useCache = true

				cacheInfo := struct {
					Name  string `json:"name"`
					Count int    `json:"count"`
				}{
					Name:  cachedContentName,
					Count: cachedMsgCount,
				}
				data, _ := json.Marshal(cacheInfo)
				w.Queue.Client.Set(ctx, redisCacheKey, string(data), time.Duration(ttl)*time.Second)
				log.Printf("Cache created: %s (count: %d)", cachedContentName, cachedMsgCount)
			}
		}
	} else {
		// No cache possible (not Gemini or no context)
		// Build full context manually
		var sb strings.Builder
		for i, msg := range payload.Context {
			role := "U"
			senderLower := strings.ToLower(msg.Sender)
			if strings.Contains(senderLower, "agent") || strings.Contains(senderLower, "atendente") || strings.Contains(senderLower, "me") {
				role = "A"
			}
			sb.WriteString(fmt.Sprintf("%s: [%s]", role, msg.Content))
			if i < len(payload.Context)-1 {
				sb.WriteString(" | ")
			}
		}
		if sb.Len() > 0 {
			contextContent = sb.String()
		} else {
			contextContent = payload.Content
		}
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	var responseJSON string
	var usage llm.TokenUsage

	if useCache {
		// Calculate delta messages (new since cache)
		var deltaPrompt string
		if len(payload.Context) > cachedMsgCount {
			var sb strings.Builder
			sb.WriteString("\n\nNew Context:\n")
			for i := cachedMsgCount; i < len(payload.Context); i++ {
				msg := payload.Context[i]
				role := "U"
				senderLower := strings.ToLower(msg.Sender)
				if strings.Contains(senderLower, "agent") || strings.Contains(senderLower, "atendente") || strings.Contains(senderLower, "me") {
					role = "A"
				}
				sb.WriteString(fmt.Sprintf("%s: [%s]", role, msg.Content))
				if i < len(payload.Context)-1 {
					sb.WriteString(" | ")
				}
			}
			deltaPrompt = sb.String()
		}

		finalPrompt := systemPrompt + deltaPrompt
		responseJSON, usage, err = geminiClient.GenerateWithCache(timeoutCtx, w.ModelName, finalPrompt, "json", cachedContentName)

		if err != nil && strings.Contains(err.Error(), "404") {
			log.Printf("Cache %s not found (404), falling back...", cachedContentName)
			w.Queue.Client.Del(ctx, fmt.Sprintf("llm:cache:%s", cacheKey))

			// Rebuild full context
			var sb strings.Builder
			for i, msg := range payload.Context {
				role := "U"
				senderLower := strings.ToLower(msg.Sender)
				if strings.Contains(senderLower, "agent") || strings.Contains(senderLower, "atendente") || strings.Contains(senderLower, "me") {
					role = "A"
				}
				sb.WriteString(fmt.Sprintf("%s: [%s]", role, msg.Content))
				if i < len(payload.Context)-1 {
					sb.WriteString(" | ")
				}
			}
			fullContext := sb.String()
			fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s", systemPrompt, fullContext)
			responseJSON, usage, err = w.Gemini.Generate(timeoutCtx, w.ModelName, fullPrompt, "json")
		}
	} else {
		fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s", systemPrompt, contextContent)
		responseJSON, usage, err = w.Gemini.Generate(timeoutCtx, w.ModelName, fullPrompt, "json")
	}

	if err != nil {
		log.Printf("Error generating analysis: %v\n", err)

		if retryErr, ok := err.(*llm.RetryableError); ok {
			log.Printf("Rate limited. Retrying job %s in %v\n", msg.JobID, retryErr.RetryDelay)
			if retryErr := w.Queue.RetryJob(ctx, msg.JobID, msg.Body, retryErr.RetryDelay); retryErr != nil {
				log.Printf("Error queueing retry: %v\n", retryErr)
			}
			return
		}

		errorResult := models.JobResult{
			JobId: job.JobId,
			Type:  "opportunity_analysis",
			Error: err.Error(),
		}
		_ = w.Queue.SendResult(ctx, job.JobId, errorResult)
		_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, err.Error())
		return
	}

	// Validate JSON
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(responseJSON), &result); err != nil {
		log.Printf("Error unmarshaling LLM response: %v\n", err)
		// Clean up markdown if present (basic cleanup)
		cleanJSON := strings.TrimPrefix(responseJSON, "```json")
		cleanJSON = strings.TrimPrefix(cleanJSON, "```")
		cleanJSON = strings.TrimSuffix(cleanJSON, "```")
		if err2 := json.Unmarshal([]byte(cleanJSON), &result); err2 != nil {
			errorResult := models.JobResult{
				JobId: job.JobId,
				Type:  "opportunity_analysis",
				Error: "invalid json from llm",
			}
			_ = w.Queue.SendResult(ctx, job.JobId, errorResult)
			_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, "invalid json from llm")
			return
		}
	}

	// Prepare result
	metadata["messageId"] = payload.ID
	metadataJSON, _ := json.Marshal(metadata)

	jobResult := models.JobResult{
		JobId:    job.JobId,
		Type:     "opportunity_analysis",
		Metadata: metadataJSON,
		Result:   result,
		Usage: &models.Usage{
			PromptTokens:     usage.PromptTokens,
			CandidateTokens:  usage.CandidateTokens,
			TotalTokens:      usage.TotalTokens,
			CachedTokens:     usage.CachedTokens,
			EstimatedCostUSD: usage.EstimatedCostUSD,
			Model:            w.ModelName,
		},
	}

	if err := w.Queue.SendResult(ctx, job.JobId, jobResult); err != nil {
		log.Printf("Error sending result: %v\n", err)
		_ = w.Queue.FailJobWithBody(ctx, msg.JobID, msg.Body, "failed to send result")
		return
	}

	if err := w.Queue.CompleteJob(ctx, msg.JobID, responseJSON); err != nil {
		log.Printf("Error completing job: %v\n", err)
	}

	log.Printf("Job %s completed successfully.\n", job.JobId)
}
