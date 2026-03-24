package redisc

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

var sonicJSON = sonic.ConfigFastest

type RedisJSON struct {
	RequestID string `json:"request_id"`
	Payload   any    `json:"payload"`
}

type Client struct {
	client *redis.Client
}

func New(addr string) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %v", err)
	}

	return &Client{client: client}, nil
}

func (client *Client) Set(ctx context.Context, key string, value interface{}) error {
	return client.client.Set(ctx, key, value, 0).Err()
}

func (client *Client) Get(ctx context.Context, key string) (string, error) {
	return client.client.Get(ctx, key).Result()
}

func (client *Client) ToRedisJSON(ctx context.Context, data interface{}) ([]byte, error) {
	_ = ctx

	requestID := uuid.New().String()

	redisJSON := RedisJSON{
		RequestID: requestID,
		Payload:   data,
	}

	json, err := sonicJSON.Marshal(redisJSON)
	if err != nil {
		return nil, err
	}

	return json, nil
}

func (client *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	return client.client.Publish(ctx, channel, message).Err()
}

func (client *Client) Subscribe(ctx context.Context, pattern string) *redis.PubSub {
	return client.client.PSubscribe(ctx, pattern)
}

func (client *Client) Ping(ctx context.Context) error {
	return client.client.Ping(ctx).Err()
}

func (client *Client) Close() error {
	return client.client.Close()
}
