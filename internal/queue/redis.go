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
	if prefix == "" {
		prefix = "bull"
	}
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
	waitKey := m.queueKey(m.InputQueue, "wait")
	activeKey := m.queueKey(m.InputQueue, "active")
	jobID, err := m.Client.BRPopLPush(ctx, waitKey, activeKey, m.BlockTimeout).Result()
	if err == redis.Nil {
		return []Message{}, nil
	}
	if err != nil {
		return nil, err
	}
	data, err := m.Client.HGet(ctx, m.jobKey(m.InputQueue, jobID), "data").Result()
	if err == redis.Nil {
		return []Message{}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := m.lockJob(ctx, m.InputQueue, jobID); err != nil {
		return nil, err
	}
	if err := m.Client.HSet(ctx, m.jobKey(m.InputQueue, jobID), "processedOn", time.Now().UnixMilli()).Err(); err != nil {
		return nil, err
	}
	return []Message{{JobID: jobID, Body: data}}, nil
}

func (m *RedisManager) CompleteJob(ctx context.Context, jobID string, returnValue string) error {
	jobKey := m.jobKey(m.InputQueue, jobID)
	if err := m.Client.HSet(ctx, jobKey, "finishedOn", time.Now().UnixMilli(), "returnvalue", returnValue).Err(); err != nil {
		return err
	}
	if err := m.Client.ZAdd(ctx, m.queueKey(m.InputQueue, "completed"), redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: jobID,
	}).Err(); err != nil {
		return err
	}
	if err := m.Client.LRem(ctx, m.queueKey(m.InputQueue, "active"), 0, jobID).Err(); err != nil {
		return err
	}
	return m.unlockJob(ctx, m.InputQueue, jobID)
}

func (m *RedisManager) FailJob(ctx context.Context, jobID string, reason string) error {
	jobKey := m.jobKey(m.InputQueue, jobID)
	if err := m.Client.HSet(ctx, jobKey, "finishedOn", time.Now().UnixMilli(), "failedReason", reason).Err(); err != nil {
		return err
	}
	if err := m.Client.ZAdd(ctx, m.queueKey(m.InputQueue, "failed"), redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: jobID,
	}).Err(); err != nil {
		return err
	}
	if err := m.Client.LRem(ctx, m.queueKey(m.InputQueue, "active"), 0, jobID).Err(); err != nil {
		return err
	}
	return m.unlockJob(ctx, m.InputQueue, jobID)
}

func (m *RedisManager) SendResult(ctx context.Context, jobID string, result interface{}) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if err := m.addJob(ctx, m.OutputQueue, jobID, data); err != nil {
		return err
	}
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
