// Package queue passes job IDs from the API to workers. Job state lives in
// Postgres.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Key is the Redis list both the API and the workers use.
const Key = "queueforge:jobs"

// ErrEmpty means nothing arrived before the wait timed out.
var ErrEmpty = errors.New("queue is empty")

// Redis is a FIFO queue on a Redis list (LPUSH / BRPOP). BRPOP removes the ID
// on delivery, so on its own this is at-most-once; the sweeper covers losses.
type Redis struct {
	client *redis.Client
	key    string
}

// blockTimeout also bounds how long a worker takes to notice shutdown.
const blockTimeout = time.Second

// NewRedis connects to the Redis server at url and checks it is reachable.
func NewRedis(ctx context.Context, url, key string) (*Redis, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parsing redis url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("connecting to redis: %w", err)
	}
	return &Redis{client: client, key: key}, nil
}

func (q *Redis) Enqueue(ctx context.Context, id string) error {
	if err := q.client.LPush(ctx, q.key, id).Err(); err != nil {
		return fmt.Errorf("enqueueing %s: %w", id, err)
	}
	return nil
}

// Dequeue removes and returns the oldest ID, waiting up to blockTimeout.
func (q *Redis) Dequeue(ctx context.Context) (string, error) {
	res, err := q.client.BRPop(ctx, blockTimeout, q.key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrEmpty
	}
	if err != nil {
		return "", fmt.Errorf("dequeueing: %w", err)
	}
	return res[1], nil // BRPOP replies with [key, value]
}

// Len is how many IDs are waiting. Used for the queue-depth gauge.
func (q *Redis) Len(ctx context.Context) (int64, error) {
	n, err := q.client.LLen(ctx, q.key).Result()
	if err != nil {
		return 0, fmt.Errorf("queue length: %w", err)
	}
	return n, nil
}

// Ping checks Redis is reachable. Used by readiness checks.
func (q *Redis) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

func (q *Redis) Close() error {
	return q.client.Close()
}

// Memory is an in-process queue with the same behavior, for tests that
// don't need Redis.
type Memory struct {
	ch      chan string
	timeout time.Duration
}

func NewMemory(capacity int, timeout time.Duration) *Memory {
	return &Memory{ch: make(chan string, capacity), timeout: timeout}
}

func (q *Memory) Enqueue(_ context.Context, id string) error {
	select {
	case q.ch <- id:
		return nil
	default:
		return errors.New("memory queue is full")
	}
}

func (q *Memory) Dequeue(ctx context.Context) (string, error) {
	timer := time.NewTimer(q.timeout)
	defer timer.Stop()
	select {
	case id := <-q.ch:
		return id, nil
	case <-timer.C:
		return "", ErrEmpty
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (q *Memory) Len(context.Context) (int64, error) {
	return int64(len(q.ch)), nil
}
