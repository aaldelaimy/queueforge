package job

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is a Store backed by the jobs table.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

const jobColumns = `id, type, payload, status, attempts, max_attempts, result, error,
	run_after, created_at, updated_at, enqueued_at, lease_expires_at, idempotency_key, trace_parent`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var runAfter, leaseExpiresAt *time.Time
	var idempotencyKey *string
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.Result, &j.Error, &runAfter, &j.CreatedAt, &j.UpdatedAt, &j.EnqueuedAt, &leaseExpiresAt,
		&idempotencyKey, &j.TraceParent)
	if runAfter != nil {
		j.RunAfter = runAfter.UTC()
	}
	if leaseExpiresAt != nil {
		j.LeaseExpiresAt = leaseExpiresAt.UTC()
	}
	j.CreatedAt = j.CreatedAt.UTC()
	j.UpdatedAt = j.UpdatedAt.UTC()
	j.EnqueuedAt = j.EnqueuedAt.UTC()
	if idempotencyKey != nil {
		j.IdempotencyKey = *idempotencyKey
	}
	return j, err
}

func (s *PostgresStore) Create(ctx context.Context, n NewJob) (Job, bool, error) {
	var key any
	if n.IdempotencyKey != "" {
		key = n.IdempotencyKey
	}

	// The unique constraint decides between concurrent inserts with the same
	// key; the loser gets no row and reads the winner.
	row := s.pool.QueryRow(ctx,
		`INSERT INTO jobs (id, type, payload, status, idempotency_key, trace_parent)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING `+jobColumns,
		rand.Text(), n.Type, n.Payload, string(StatusQueued), key, n.TraceParent)
	j, err := scanJob(row)
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("inserting job: %w", err)
	}

	existing, err := s.getByIdempotencyKey(ctx, n.IdempotencyKey)
	if err != nil {
		return Job{}, false, err
	}
	if !sameRequest(existing.Type, existing.Payload, n.Type, n.Payload) {
		return Job{}, false, fmt.Errorf("%w: %q", ErrIdempotencyConflict, n.IdempotencyKey)
	}
	return existing, false, nil
}

func (s *PostgresStore) getByIdempotencyKey(ctx context.Context, key string) (Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, key)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("getting job by idempotency key: %w", err)
	}
	return j, nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("getting job %s: %w", id, err)
	}
	return j, nil
}

func (s *PostgresStore) List(ctx context.Context, status Status) ([]Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs`
	var args []any
	if status != "" {
		query += ` WHERE status = $1`
		args = append(args, string(status))
	}
	query += ` ORDER BY created_at, id`
	jobs, err := s.queryJobs(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing jobs: %w", err)
	}
	return jobs, nil
}

func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, id); err != nil {
		return fmt.Errorf("deleting job %s: %w", id, err)
	}
	return nil
}

func (s *PostgresStore) Claim(ctx context.Context, id string, lease time.Duration) (Job, error) {
	return s.transition(ctx, id, StatusRunning,
		`attempts = attempts + 1, lease_expires_at = now() + make_interval(secs => $4)`,
		lease.Seconds())
}

func (s *PostgresStore) Complete(ctx context.Context, id string, result json.RawMessage) (Job, error) {
	return s.transition(ctx, id, StatusCompleted,
		`result = $4, error = '', lease_expires_at = NULL, run_after = NULL`,
		result)
}

func (s *PostgresStore) Fail(ctx context.Context, id string, reason string) (Job, error) {
	return s.transition(ctx, id, StatusFailed,
		`error = $4, lease_expires_at = NULL`,
		reason)
}

func (s *PostgresStore) DeadLetter(ctx context.Context, id string, reason string) (Job, error) {
	return s.transition(ctx, id, StatusDeadLettered,
		`error = $4, lease_expires_at = NULL`,
		reason)
}

func (s *PostgresStore) Reopen(ctx context.Context, id string) (Job, error) {
	// Only failed / dead_lettered, not retrying. PromoteDue handles backoff.
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs SET
			status = 'queued', attempts = 0, enqueued_at = now(),
			lease_expires_at = NULL, run_after = NULL, updated_at = now()
		WHERE id = $1 AND status = ANY($2)
		RETURNING `+jobColumns,
		id, []string{string(StatusFailed), string(StatusDeadLettered)})
	j, err := scanJob(row)
	if err == nil {
		return j, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("reopening job %s: %w", id, err)
	}
	var current string
	err = s.pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, id).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("reading job %s: %w", id, err)
	}
	return Job{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current, StatusQueued)
}

func (s *PostgresStore) Retry(ctx context.Context, id string, reason string, delay time.Duration) (Job, error) {
	return s.transition(ctx, id, StatusRetrying,
		`error = $4, lease_expires_at = NULL, run_after = now() + make_interval(secs => $5)`,
		reason, delay.Seconds())
}

// PromoteDue is a single UPDATE, so concurrent sweepers never return the
// same job.
func (s *PostgresStore) PromoteDue(ctx context.Context) ([]string, error) {
	return s.queryIDs(ctx, `
		UPDATE jobs SET status = 'queued', run_after = NULL, enqueued_at = now(), updated_at = now()
		WHERE status = 'retrying' AND run_after <= now()
		RETURNING id`)
}

func (s *PostgresStore) ExpiredLeases(ctx context.Context) ([]Job, error) {
	jobs, err := s.queryJobs(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE status = 'running' AND lease_expires_at < now()`)
	if err != nil {
		return nil, fmt.Errorf("finding expired leases: %w", err)
	}
	return jobs, nil
}

func (s *PostgresStore) RequeueStale(ctx context.Context, olderThan time.Duration) ([]string, error) {
	return s.queryIDs(ctx, `
		UPDATE jobs SET enqueued_at = now()
		WHERE status = 'queued' AND enqueued_at < now() - make_interval(secs => $1)
		RETURNING id`, olderThan.Seconds())
}

// transition changes status with one conditional UPDATE, so the state check
// and the write are atomic: of concurrent callers, exactly one wins.
// set is always a constant from this file; its values go in args ($4, $5...).
func (s *PostgresStore) transition(ctx context.Context, id string, next Status, set string, args ...any) (Job, error) {
	query := `UPDATE jobs SET status = $2, updated_at = now(), ` + set + `
		WHERE id = $1 AND status = ANY($3)
		RETURNING ` + jobColumns
	row := s.pool.QueryRow(ctx, query, append([]any{id, string(next), statusesBefore(next)}, args...)...)
	j, err := scanJob(row)
	if err == nil {
		return j, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("updating job %s: %w", id, err)
	}

	// No row matched. This read only picks which error to return.
	var current string
	err = s.pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, id).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("reading job %s: %w", id, err)
	}
	return Job{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current, next)
}

func (s *PostgresStore) queryJobs(ctx context.Context, sql string, args ...any) ([]Job, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) {
		return scanJob(row)
	})
	if err != nil {
		return nil, err
	}
	if jobs == nil {
		jobs = []Job{}
	}
	return jobs, nil
}

func (s *PostgresStore) queryIDs(ctx context.Context, sql string, args ...any) ([]string, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// statusesBefore inverts the transitions map for the WHERE clause.
func statusesBefore(next Status) []string {
	var from []string
	for s, allowed := range transitions {
		if slices.Contains(allowed, next) {
			from = append(from, string(s))
		}
	}
	return from
}
