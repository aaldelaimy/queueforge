package queue

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

type testQueue interface {
	Enqueue(ctx context.Context, id string) error
	Dequeue(ctx context.Context) (string, error)
}

// testQueueContract runs the same behavioral tests against any queue.
// newQueue must return an empty queue.
func testQueueContract(t *testing.T, newQueue func(t *testing.T) testQueue) {
	tests := []struct {
		name string
		fn   func(t *testing.T, q testQueue)
	}{
		{"RoundTrip", testRoundTrip},
		{"FIFO", testFIFO},
		{"EmptyReturnsErrEmpty", testEmptyReturnsErrEmpty},
		{"ConcurrentConsumersGetEachIDOnce", testConcurrentConsumersGetEachIDOnce},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newQueue(t)) })
	}
}

func mustEnqueue(t *testing.T, q testQueue, id string) {
	t.Helper()
	if err := q.Enqueue(t.Context(), id); err != nil {
		t.Fatalf("Enqueue(%s): %v", id, err)
	}
}

func testRoundTrip(t *testing.T, q testQueue) {
	mustEnqueue(t, q, "job-1")
	got, err := q.Dequeue(t.Context())
	if err != nil || got != "job-1" {
		t.Errorf("got %q, %v; want job-1", got, err)
	}
}

func testFIFO(t *testing.T, q testQueue) {
	for _, id := range []string{"a", "b", "c"} {
		mustEnqueue(t, q, id)
	}
	for _, want := range []string{"a", "b", "c"} {
		if got, err := q.Dequeue(t.Context()); err != nil || got != want {
			t.Errorf("got %q, %v; want %q", got, err, want)
		}
	}
}

func testEmptyReturnsErrEmpty(t *testing.T, q testQueue) {
	if _, err := q.Dequeue(t.Context()); !errors.Is(err, ErrEmpty) {
		t.Errorf("got %v, want ErrEmpty", err)
	}
}

// Four consumers drain 100 IDs. Every ID must be delivered, and none twice.
func testConcurrentConsumersGetEachIDOnce(t *testing.T, q testQueue) {
	const n = 100
	for i := range n {
		mustEnqueue(t, q, fmt.Sprint(i))
	}

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				id, err := q.Dequeue(t.Context())
				if errors.Is(err, ErrEmpty) {
					return
				}
				if err != nil {
					t.Errorf("Dequeue: %v", err)
					return
				}
				mu.Lock()
				seen[id]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(seen) != n {
		t.Errorf("got %d distinct IDs, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("ID %s delivered %d times", id, count)
		}
	}
}

func TestMemoryQueue(t *testing.T) {
	testQueueContract(t, func(t *testing.T) testQueue {
		return NewMemory(1000, 50*time.Millisecond)
	})
}

func TestMemoryQueueLen(t *testing.T) {
	q := NewMemory(10, time.Millisecond)
	if n, err := q.Len(t.Context()); err != nil || n != 0 {
		t.Fatalf("empty: %d, %v", n, err)
	}
	mustEnqueue(t, q, "a")
	mustEnqueue(t, q, "b")
	if n, err := q.Len(t.Context()); err != nil || n != 2 {
		t.Fatalf("after enqueue: %d, %v", n, err)
	}
	if _, err := q.Dequeue(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n, err := q.Len(t.Context()); err != nil || n != 1 {
		t.Fatalf("after dequeue: %d, %v", n, err)
	}
}

// Redis tests need a Redis they may write to. Each test uses its own key
// and deletes it afterwards.
const testRedisEnv = "QUEUEFORGE_TEST_REDIS_URL"

func newTestRedis(t *testing.T, key string) *Redis {
	t.Helper()
	url := os.Getenv(testRedisEnv)
	if url == "" {
		t.Skipf("%s not set; skipping Redis tests", testRedisEnv)
	}
	q, err := NewRedis(t.Context(), url, key)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(func() {
		q.client.Del(context.Background(), key)
		q.Close()
	})
	return q
}

func testKey() string { return "queueforge:test:" + rand.Text() }

func TestRedisQueue(t *testing.T) {
	testQueueContract(t, func(t *testing.T) testQueue {
		return newTestRedis(t, testKey())
	})
}

func TestRedisQueueLen(t *testing.T) {
	q := newTestRedis(t, testKey())
	if n, err := q.Len(t.Context()); err != nil || n != 0 {
		t.Fatalf("empty: %d, %v", n, err)
	}
	mustEnqueue(t, q, "a")
	mustEnqueue(t, q, "b")
	if n, err := q.Len(t.Context()); err != nil || n != 2 {
		t.Fatalf("after enqueue: %d, %v", n, err)
	}
}

// The API and worker are separate processes with separate connections. An ID
// pushed on one connection must be available on another.
func TestRedisIDSurvivesNewConnection(t *testing.T) {
	key := testKey()
	producer := newTestRedis(t, key)
	mustEnqueue(t, producer, "job-1")
	producer.Close()

	consumer := newTestRedis(t, key)
	if got, err := consumer.Dequeue(t.Context()); err != nil || got != "job-1" {
		t.Errorf("got %q, %v; want job-1", got, err)
	}
}
