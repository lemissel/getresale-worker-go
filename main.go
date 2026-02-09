package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"getresale-worker-go/internal/llm"
	"getresale-worker-go/internal/queue"
	"getresale-worker-go/internal/worker"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/joho/godotenv"
)

func main() {
	log.Println("Initializing GetResale Worker...")

	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	// Inicializar OpenTelemetry
	shutdown, err := InitTracer("getresale-worker-go", "localhost:4317")
	if err != nil {
		log.Printf("Failed to initialize OpenTelemetry: %v", err)
	} else {
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				log.Printf("Failed to shutdown OpenTelemetry: %v", err)
			}
		}()
		log.Println("OpenTelemetry initialized")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load AWS Config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	sqsClient := sqs.NewFromConfig(cfg)

	// Env variables
	inputQueue := os.Getenv("SQS_INPUT_QUEUE_URL")
	outputQueue := os.Getenv("SQS_OUTPUT_QUEUE_URL")
	ollamaURL := os.Getenv("OLLAMA_HOST")
	if ollamaURL == "" {
		ollamaURL = "http://localhost:11434"
	}
	openAIKey := os.Getenv("OPENAI_API_KEY")
	openAIBaseURL := os.Getenv("OPENAI_BASE_URL")

	maxConcurrencyStr := os.Getenv("SQS_MAX_CONCURRENCY")
	maxConcurrency, err := strconv.Atoi(maxConcurrencyStr)
	if err != nil || maxConcurrency <= 0 {
		maxConcurrency = 5 // Default
	}

	if inputQueue == "" || outputQueue == "" {
		log.Fatal("SQS_INPUT_QUEUE_URL and SQS_OUTPUT_QUEUE_URL must be set")
	}

	// Clients
	sqsManager := queue.NewSQSManager(sqsClient, inputQueue, outputQueue)
	ollamaClient := llm.NewOllamaClient(ollamaURL)
	openAIClient := llm.NewOpenAIClient(openAIKey, openAIBaseURL)

	// Worker
	w := worker.NewWorker(sqsManager, ollamaClient, openAIClient, maxConcurrency)

	// Start
	w.Start(ctx)

	log.Println("Worker shut down gracefully.")
}
