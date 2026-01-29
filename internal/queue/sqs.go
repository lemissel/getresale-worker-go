package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSManager struct {
	Client      *sqs.Client
	InputQueue  string
	OutputQueue string
}

func NewSQSManager(client *sqs.Client, inputQueue, outputQueue string) *SQSManager {
	return &SQSManager{
		Client:      client,
		InputQueue:  inputQueue,
		OutputQueue: outputQueue,
	}
}

func (m *SQSManager) ReceiveMessages(ctx context.Context) ([]types.Message, error) {
	output, err := m.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(m.InputQueue),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20,
		VisibilityTimeout:   300, // 5 minutes
	})
	if err != nil {
		return nil, err
	}
	return output.Messages, nil
}

func (m *SQSManager) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := m.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.InputQueue),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

func (m *SQSManager) SendResult(ctx context.Context, result interface{}) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}

	_, err = m.Client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(m.OutputQueue),
		MessageBody: aws.String(string(data)),
	})
	if err != nil {
		return fmt.Errorf("failed to send message to output queue: %w", err)
	}
	return nil
}
