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
	Ollama    llm.Client
	ModelName string
	Semaphore chan struct{}
	Wg        sync.WaitGroup
}

func NewOpportunityWorker(queueManager *queue.RedisManager, ollama llm.Client, maxConcurrency int, modelName string) *OpportunityWorker {
	return &OpportunityWorker{
		Queue:     queueManager,
		Ollama:    ollama,
		ModelName: modelName,
		Semaphore: make(chan struct{}, maxConcurrency),
	}
}

func (w *OpportunityWorker) Start(ctx context.Context) {
	log.Println("OpportunityWorker started...")
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
				w.Wg.Add(1)
				go w.handleMessage(ctx, msg)
			}
		}
	}
}

func (w *OpportunityWorker) handleMessage(ctx context.Context, msg queue.Message) {
	defer w.Wg.Done()

	// Acquire semaphore
	w.Semaphore <- struct{}{}
	defer func() { <-w.Semaphore }()

	// The msg.Body is the raw JSON payload sent from NestJS (BullMQ data).
	// It does NOT have the 'Job' envelope structure from models.Job.
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

	if err := json.Unmarshal([]byte(msg.Body), &payload); err != nil {
		log.Printf("Error unmarshaling payload: %v\n", err)
		_ = w.Queue.FailJob(ctx, msg.JobID, "invalid payload")
		return
	}

	// Idempotency check using Redis
	lockKey := fmt.Sprintf("processing:opportunity:%s", payload.ID)
	// Try to set the key with a TTL (e.g., 1 hour)
	// If SetNX returns true, we acquired the lock/flag. If false, it's already processed/processing.
	// Note: Queue.Client is accessible if we expose it or add a method.
	// Since Queue is *queue.RedisManager, let's assume we can add a method there or access the client.
	// Checking queue/redis_manager.go might be needed, but usually we can add a helper.
	// For now, let's assume we can add SetNX to RedisManager.

	// Ideally we check if it was ALREADY processed successfully.
	// But simply preventing double processing in short term:
	processed, err := w.Queue.Client.SetNX(ctx, lockKey, "1", 1*time.Hour).Result()
	if err != nil {
		log.Printf("Error checking idempotency for ID %s: %v\n", payload.ID, err)
		// Decide whether to fail or proceed. Proceeding is safer if Redis fails temporarily.
	} else if !processed {
		log.Printf("Skipping duplicate opportunity analysis for ID: %s\n", payload.ID)

		// Mark job as completed in BullMQ so it is removed from active queue
		// Use a simple JSON to indicate skipped
		_ = w.Queue.CompleteJob(ctx, msg.JobID, "{\"status\":\"skipped\", \"reason\":\"duplicate\"}")
		return
	}

	log.Printf("Processing opportunity analysis for ID: %s\n", payload.ID)

	systemPrompt := `Act as a Senior Sales Strategist. Analyze the conversation context (history) to identify sales opportunities.
Use the provided 'AVAILABLE SERVICES' (which include prices in BRL) to identify up-selling or cross-selling opportunities based on the client's needs and history.
Calculate 'upsell_potential_brl' by summing the prices of the services you identify as potential opportunities for this client. If no specific service is identified or prices are missing, estimate a reasonable value or use 0.0.

OUTPUT JSON FORMAT: { 'executive_summary': 'string (in Portuguese)', 'mood': 'Neutral/Positive/Negative (in Portuguese)', 'satisfaction': 'Operational/At Risk/High (in Portuguese)', 'notes': 'string (in Portuguese)', 'opportunities_risks': 'string (in Portuguese)', 'recommended_action_title': 'string (in Portuguese)', 'upsell_potential_brl': float64 }`

	servicesJSON, _ := json.Marshal(payload.Services)

	// Build transcript from Context if available
	transcript := payload.Content
	if len(payload.Context) > 0 {
		var sb strings.Builder
		for _, msg := range payload.Context {
			sb.WriteString(fmt.Sprintf("[%s] %s: %s\n", msg.Timestamp, msg.Sender, msg.Content))
		}
		transcript = sb.String()
	}

	fullPrompt := fmt.Sprintf("%s\n\nAVAILABLE SERVICES:\n%s\n\nTRANSCRIPT (Context):\n%s", systemPrompt, string(servicesJSON), transcript)

	timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	model := w.ModelName

	responseJSON, err := w.Ollama.Generate(timeoutCtx, model, fullPrompt, "json")
	if err != nil {
		log.Printf("Error generating analysis: %v\n", err)
		_ = w.Queue.FailJob(ctx, msg.JobID, err.Error())
		return
	}

	// Validate JSON
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(responseJSON), &result); err != nil {
		log.Printf("Error unmarshaling LLM response: %v\n", err)
		_ = w.Queue.FailJob(ctx, msg.JobID, "invalid json from llm")
		return
	}

	// Prepare result for output queue
	// Create metadata with messageId for the consumer
	metadata := map[string]string{
		"messageId": payload.ID,
	}
	metadataJSON, _ := json.Marshal(metadata)

	jobResult := models.JobResult{
		JobId:    msg.JobID, // Use Redis JobID
		Type:     "opportunity_analysis",
		Metadata: metadataJSON,
		Result:   result,
	}

	// Send to output queue (LLM_OUTPUT)
	// IMPORTANT: Use payload.ID (UUID) as the Job ID for the output queue if possible, or a new UUID.
	// But BullMQ expects unique Job IDs. If we use msg.JobID (e.g. "10"), it might conflict if we are not careful?
	// Actually, BullMQ jobs usually have numeric IDs or UUIDs.
	// The user checked "bull:LLM_OUTPUT:32f06a13..." which implies they expect the JobID to be the UUID.
	// But we are passing msg.JobID ("10") to SendResult.
	// So the key created is "bull:LLM_OUTPUT:10".
	// To fix this and match user expectation (and likely avoid conflicts if multiple workers run),
	// we should use the Message UUID as the JobID for the output queue, or a derived one.
	// Let's use payload.ID (the UUID).

	if err := w.Queue.SendResult(ctx, payload.ID, jobResult); err != nil {
		log.Printf("Error sending result: %v\n", err)
		_ = w.Queue.FailJob(ctx, msg.JobID, "failed to send result")
		return
	}

	if err := w.Queue.CompleteJob(ctx, msg.JobID, responseJSON); err != nil {
		log.Printf("Error completing job: %v\n", err)
	}

	log.Printf("Job %s completed successfully.\n", msg.JobID)
}
