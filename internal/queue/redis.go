package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Message struct {
	JobID string
	Body  string
}

type RedisManager struct {
	Client       *redis.Client
	InputQueue   string
	OutputQueue  string
	BlockTimeout time.Duration
	LockTTL      time.Duration
	WorkerID     string
	Prefix       string
}

func NewRedisManager(client *redis.Client, inputQueue, outputQueue string, workerID string, prefix string) *RedisManager {
	return &RedisManager{
		Client:       client,
		InputQueue:   inputQueue,
		OutputQueue:  outputQueue,
		BlockTimeout: 20 * time.Second,
		LockTTL:      5 * time.Minute,
		WorkerID:     workerID,
		Prefix:       prefix,
	}
}

func (m *RedisManager) ReceiveMessages(ctx context.Context) ([]Message, error) {
	// Simple BLPop from the input list
	// BLPop returns [key, value]
	result, err := m.Client.BLPop(ctx, m.BlockTimeout, m.InputQueue).Result()
	if err == redis.Nil {
		return []Message{}, nil
	}
	if err != nil {
		return nil, err
	}

	// result[0] is key (queue name), result[1] is value (payload)
	payload := result[1]

	// We don't have a separate JobID in simple list mode, unless it's in the payload.
	// We'll generate a temporary ID for logging purposes or try to extract it if needed.
	// For now, let's use a placeholder or hash of payload.
	jobID := "list-job"

	return []Message{{JobID: jobID, Body: payload}}, nil
}

func (m *RedisManager) SendResult(ctx context.Context, jobID string, result interface{}) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	// Simple RPush to the output list
	return m.Client.RPush(ctx, m.OutputQueue, data).Err()
}

func (m *RedisManager) CompleteJob(ctx context.Context, jobID string, returnValue string) error {
	// In simple list mode, completion is just sending the result (handled by SendResult).
	// This method might be redundant or can be used for logging.
	return nil
}

func (m *RedisManager) FailJob(ctx context.Context, jobID string, reason string) error {
	// In simple list mode, we might want to push to a failed queue or just log.
	// For now, let's just log.
	fmt.Printf("Job %s failed: %s\n", jobID, reason)
	return nil
}

func (m *RedisManager) addJob(ctx context.Context, queueName string, jobID string, data []byte) error {
	jobKey := m.jobKey(queueName, jobID)
	payload := string(data)
	if err := m.Client.HSet(ctx, jobKey,
		"name", "default",
		"data", payload,
		"opts", "{}",
		"timestamp", time.Now().UnixMilli(),
	).Err(); err != nil {
		return err
	}
	return m.Client.RPush(ctx, m.queueKey(queueName, "wait"), jobID).Err()
}

func (m *RedisManager) lockJob(ctx context.Context, queueName string, jobID string) error {
	lockKey := m.queueKey(queueName, jobID+":lock")
	return m.Client.Set(ctx, lockKey, m.WorkerID, m.LockTTL).Err()
}

func (m *RedisManager) unlockJob(ctx context.Context, queueName string, jobID string) error {
	lockKey := m.queueKey(queueName, jobID+":lock")
	return m.Client.Del(ctx, lockKey).Err()
}

func (m *RedisManager) queueKey(queueName string, suffix string) string {
	return fmt.Sprintf("%s:%s:%s", m.Prefix, queueName, suffix)
}

func (m *RedisManager) jobKey(queueName string, jobID string) string {
	return fmt.Sprintf("%s:%s:%s", m.Prefix, queueName, jobID)
}
