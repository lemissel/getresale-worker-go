package worker

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"getresale-worker-go/internal/llm"
	"getresale-worker-go/internal/models"
	"getresale-worker-go/internal/queue"
)

type Worker struct {
	Queue     *queue.RedisManager
	Ollama    llm.Client
	OpenAI    llm.Client
	Semaphore chan struct{}
	Wg        sync.WaitGroup
}

func NewWorker(queueManager *queue.RedisManager, ollama, openai llm.Client, maxConcurrency int) *Worker {
	return &Worker{
		Queue:     queueManager,
		Ollama:    ollama,
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
		_ = w.Queue.FailJob(ctx, msg.JobID, "invalid job payload")
		return
	}

	var payload models.LLMPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		log.Printf("Error unmarshaling payload: %v\n", err)
		_ = w.Queue.FailJob(ctx, msg.JobID, "invalid llm payload")
		return
	}

	log.Printf("Processing job %s (%s)...\n", job.JobId, job.Type)

	var result string
	var err error

	// Decide which client to use
	if payload.Model == "whisper-1" || payload.Model == "gpt-4o" || payload.Model == "gpt-4" {
		result, err = w.OpenAI.Generate(payload.Model, payload.Prompt, payload.Format)
	} else {
		result, err = w.Ollama.Generate(payload.Model, payload.Prompt, payload.Format)
	}

	jobResult := models.JobResult{
		JobId:    job.JobId,
		Type:     job.Type,
		Metadata: job.Metadata,
	}

	if err != nil {
		log.Printf("Error processing job %s: %v\n", job.JobId, err)
		jobResult.Error = err.Error()
		_ = w.Queue.FailJob(ctx, msg.JobID, err.Error())
	} else {
		jobResult.Result = result
		if err := w.Queue.SendResult(ctx, msg.JobID, jobResult); err != nil {
			log.Printf("Error sending result for job %s: %v\n", job.JobId, err)
			_ = w.Queue.FailJob(ctx, msg.JobID, "failed to enqueue output")
			return
		}
		if err := w.Queue.CompleteJob(ctx, msg.JobID, result); err != nil {
			log.Printf("Error completing job %s: %v\n", job.JobId, err)
		}
		log.Printf("Job %s completed successfully.\n", job.JobId)
	}
}
