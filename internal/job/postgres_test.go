package job

import (
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aaldelaimy/queueforge/internal/db"
)

// These tests need a disposable database; they delete every job in it.
const testDatabaseEnv = "QUEUEFORGE_TEST_DATABASE_URL"

// newTestPool connects to the test database, applies migrations, and
// empties the jobs table.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(testDatabaseEnv)
	if url == "" {
		t.Skipf("%s not set; skipping Postgres tests", testDatabaseEnv)
	}

	pool, err := db.Connect(t.Context(), url)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := db.Migrate(t.Context(), pool); err != nil {
		t.Fatalf("migrating: %v", err)
	}
	if _, err := pool.Exec(t.Context(), `TRUNCATE jobs`); err != nil {
		t.Fatalf("truncating: %v", err)
	}
	return pool
}

func TestPostgresStore(t *testing.T) {
	testStoreContract(t, func(t *testing.T) Store {
		return NewPostgresStore(newTestPool(t))
	})
}

// A new process means new connections. The job must still be there.
func TestPostgresJobSurvivesNewConnection(t *testing.T) {
	first := newTestPool(t)
	created := mustCreate(t, NewPostgresStore(first), "t", nil)
	first.Close()

	second, err := db.Connect(t.Context(), os.Getenv(testDatabaseEnv))
	if err != nil {
		t.Fatalf("reconnecting: %v", err)
	}
	defer second.Close()

	got, err := NewPostgresStore(second).Get(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Get after reconnect: %v", err)
	}
	if got.ID != created.ID || got.Status != StatusQueued {
		t.Errorf("got %+v", got)
	}
}

func TestMigrateIsRepeatable(t *testing.T) {
	pool := newTestPool(t) // already migrated once
	countMigrations := func() (n int) {
		pool.QueryRow(t.Context(), `SELECT count(*) FROM schema_migrations`).Scan(&n)
		return n
	}
	before := countMigrations()

	if err := db.Migrate(t.Context(), pool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if after := countMigrations(); after != before || after == 0 {
		t.Errorf("schema_migrations rows: %d before, %d after; want equal and non-zero", before, after)
	}
}

// The CHECK constraints are a second line of defense: even code that skips
// the store and writes SQL directly cannot save an impossible job.
func TestDatabaseRejectsInvalidRows(t *testing.T) {
	pool := newTestPool(t)

	cases := map[string]string{
		"unknown status":    `INSERT INTO jobs (id, type, status) VALUES ('x1', 't', 'bogus')`,
		"negative attempts": `INSERT INTO jobs (id, type, status, attempts) VALUES ('x2', 't', 'queued', -1)`,
		"zero max attempts": `INSERT INTO jobs (id, type, status, max_attempts) VALUES ('x3', 't', 'queued', 0)`,
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), sql)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" { // check_violation
				t.Errorf("got %v, want a check_violation", err)
			}
		})
	}
}

func TestDatabaseRejectsDuplicateIdempotencyKey(t *testing.T) {
	pool := newTestPool(t)
	_, err := pool.Exec(t.Context(), `INSERT INTO jobs (id, type, status, idempotency_key) VALUES ('a', 't', 'queued', 'k')`)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err = pool.Exec(t.Context(), `INSERT INTO jobs (id, type, status, idempotency_key) VALUES ('b', 't', 'queued', 'k')`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" { // unique_violation
		t.Errorf("got %v, want a unique_violation", err)
	}
}
