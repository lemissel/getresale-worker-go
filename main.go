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

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.Println("Initializing GetResale Worker...")

	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Env variables
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost"
	}
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDBStr := os.Getenv("REDIS_DB")
	redisDB := 0
	if redisDBStr != "" {
		if value, err := strconv.Atoi(redisDBStr); err == nil && value >= 0 {
			redisDB = value
		}
	}

	inputQueue := os.Getenv("REDIS_INPUT_QUEUE")
	outputQueue := os.Getenv("REDIS_OUTPUT_QUEUE")
	ollamaURL := os.Getenv("OLLAMA_HOST")
	if ollamaURL == "" {
		ollamaURL = "http://localhost:11434"
	}
	openAIKey := os.Getenv("OPENAI_API_KEY")
	openAIBaseURL := os.Getenv("OPENAI_BASE_URL")

	maxConcurrencyStr := os.Getenv("REDIS_MAX_CONCURRENCY")
	maxConcurrency, err := strconv.Atoi(maxConcurrencyStr)
	if err != nil || maxConcurrency <= 0 {
		maxConcurrency = 5 // Default
	}

	ollamaModel := os.Getenv("OLLAMA_MODEL")
	if ollamaModel == "" {
		ollamaModel = "llama3.2" // Default to a commonly available model or fallback
	}

	if inputQueue == "" || outputQueue == "" {
		log.Fatal("REDIS_INPUT_QUEUE and REDIS_OUTPUT_QUEUE must be set")
	}

	// Clients
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
	redisPrefix := os.Getenv("REDIS_PREFIX")
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			workerID = "worker"
		} else {
			workerID = hostname
		}
		workerID = workerID + ":" + strconv.Itoa(os.Getpid())
	}
	redisManager := queue.NewRedisManager(redisClient, inputQueue, outputQueue, workerID, redisPrefix)
	ollamaClient := llm.NewOllamaClient(ollamaURL)
	openAIClient := llm.NewOpenAIClient(openAIKey, openAIBaseURL)

	// Worker
	w := worker.NewWorker(redisManager, ollamaClient, openAIClient, maxConcurrency)

	// Opportunity Worker
	oppQueueName := "opportunity_analysis_queue"
	// Output queue for LLM results (consumed by NestJS)
	oppOutputQueue := "LLM_OUTPUT"
	oppRedisManager := queue.NewRedisManager(redisClient, oppQueueName, oppOutputQueue, workerID+"-opp", redisPrefix)

	// Opportunity Worker no longer needs DB, just Ollama
	oppWorker := worker.NewOpportunityWorker(oppRedisManager, ollamaClient, maxConcurrency, ollamaModel)

	// Start Opportunity Worker
	go oppWorker.Start(ctx)

	// Start
	w.Start(ctx)

	log.Println("Worker shut down gracefully.")
}
