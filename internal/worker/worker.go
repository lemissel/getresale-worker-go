package worker

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"getresale-worker-go/internal/llm"
	"getresale-worker-go/internal/models"
	"getresale-worker-go/internal/queue"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Worker struct {
	SQS       *queue.SQSManager
	Ollama    llm.Client
	OpenAI    llm.Client
	Semaphore chan struct{}
	Wg        sync.WaitGroup
}

func NewWorker(sqs *queue.SQSManager, ollama, openai llm.Client, maxConcurrency int) *Worker {
	return &Worker{
		SQS:       sqs,
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
			messages, err := w.SQS.ReceiveMessages(ctx)
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

func (w *Worker) handleMessage(ctx context.Context, msg types.Message) {
	defer w.Wg.Done()

	// Acquire semaphore
	w.Semaphore <- struct{}{}
	defer func() { <-w.Semaphore }()

	var job models.Job
	if err := json.Unmarshal([]byte(*msg.Body), &job); err != nil {
		log.Printf("Error unmarshaling job: %v\n", err)
		return
	}

	var payload models.LLMPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		log.Printf("Error unmarshaling payload: %v\n", err)
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
		// We DON'T delete the message from SQS here so it can be retried or sent to DLQ
	} else {
		jobResult.Result = result
		// Send success result
		if err := w.SQS.SendResult(ctx, jobResult); err != nil {
			log.Printf("Error sending result for job %s: %v\n", job.JobId, err)
			return
		}
		// Delete message from SQS
		if err := w.SQS.DeleteMessage(ctx, *msg.ReceiptHandle); err != nil {
			log.Printf("Error deleting message %s: %v\n", job.JobId, err)
		}
		log.Printf("Job %s completed successfully.\n", job.JobId)
	}
}
