package psql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

func TestIsRetryableTxError(t *testing.T) {
	for code, want := range map[string]bool{
		pgerrcode.SerializationFailure: true,
		pgerrcode.DeadlockDetected:     true,
		pgerrcode.UniqueViolation:      false,
	} {
		if got := IsRetryableTxError(ErrCommit(&PgError{Code: code})); got != want {
			t.Errorf("IsRetryableTxError(%s) = %v, want %v", code, got, want)
		}
	}
	if IsRetryableTxError(errors.New("plain")) {
		t.Error("plain error must not be retryable")
	}
}

func TestTxRetryDelayBounded(t *testing.T) {
	for attempt := range 80 {
		if d := txRetryDelay(attempt); d <= 0 || d > maxTxRetryDelay {
			t.Fatalf("txRetryDelay(%d) = %v", attempt, d)
		}
	}
}

// TestIntegration shares one embedded PostgreSQL between the subtests: startup
// dominates the runtime of this package.
func TestIntegration(t *testing.T) {
	dsn := startEmbeddedPostgres(t)
	ctx := context.Background()

	newClient := func(t *testing.T, opts ...Option) *Client {
		t.Helper()
		cfg, err := NewConfig(dsn, append([]Option{WithIntervalCheck(20 * time.Millisecond)}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		c, err := NewClient(connectCtx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		return c
	}

	db := newClient(t, WithPoolName("it-main"), WithApplicationName("psql-it"))
	if _, err := db.Exec(ctx, `CREATE TABLE tx_test (id int PRIMARY KEY, v int NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	count := func(t *testing.T) int {
		t.Helper()
		var n int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM tx_test`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("runtime params reach the server", func(t *testing.T) {
		var app string
		if err := db.QueryRow(ctx, `SHOW application_name`).Scan(&app); err != nil {
			t.Fatal(err)
		}
		if app != "psql-it" {
			t.Fatalf("application_name = %q", app)
		}
	})

	t.Run("statement timeout", func(t *testing.T) {
		c := newClient(t, WithPoolName("it-timeout"), WithStatementTimeout(50*time.Millisecond))
		_, err := c.Exec(ctx, `SELECT pg_sleep(1)`)
		var pgErr *PgError
		if !errors.As(err, &pgErr) || pgErr.Code != pgerrcode.QueryCanceled {
			t.Fatalf("expected query_canceled, got %v", err)
		}
	})

	t.Run("WithTx commits", func(t *testing.T) {
		err := db.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO tx_test VALUES (1, 1)`)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if count(t) != 1 {
			t.Fatal("row not committed")
		}
	})

	t.Run("WithTx rolls back on error", func(t *testing.T) {
		boom := errors.New("boom")
		err := db.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `INSERT INTO tx_test VALUES (2, 2)`); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if count(t) != 1 {
			t.Fatal("row must be rolled back")
		}
	})

	t.Run("WithTx rolls back on panic", func(t *testing.T) {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("panic must propagate")
				}
			}()
			_ = db.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
				_, _ = tx.Exec(ctx, `INSERT INTO tx_test VALUES (3, 3)`)
				panic("boom")
			})
		}()
		if count(t) != 1 {
			t.Fatal("row must be rolled back after panic")
		}
		if db.Stat().AcquiredConns() != 0 {
			t.Fatal("connection leaked after panic")
		}
	})

	t.Run("WithTx retries serialization failures", func(t *testing.T) {
		c := newClient(t, WithPoolName("it-retry"), WithTxMaxAttempts(3))
		calls := 0
		err := c.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			calls++
			if calls < 3 {
				return &PgError{Code: pgerrcode.SerializationFailure}
			}
			_, err := tx.Exec(ctx, `UPDATE tx_test SET v = v + 1 WHERE id = 1`)
			return err
		})
		if err != nil || calls != 3 {
			t.Fatalf("err = %v, calls = %d", err, calls)
		}

		calls = 0
		err = c.WithTx(ctx, pgx.TxOptions{}, func(pgx.Tx) error {
			calls++
			return &PgError{Code: pgerrcode.DeadlockDetected}
		})
		if !IsRetryableTxError(err) || calls != 3 {
			t.Fatalf("must give up after 3 attempts: err = %v, calls = %d", err, calls)
		}

		calls = 0
		_ = c.WithTx(ctx, pgx.TxOptions{}, func(pgx.Tx) error {
			calls++
			return &PgError{Code: pgerrcode.UniqueViolation}
		})
		if calls != 1 {
			t.Fatalf("non-retryable errors must not be retried: calls = %d", calls)
		}
	})

	t.Run("WithTx real serialization conflict", func(t *testing.T) {
		// Two serializable transactions read then write the same row; the
		// loser must be retried and both increments must land.
		ser := pgx.TxOptions{IsoLevel: pgx.Serializable}
		var before int
		if err := db.QueryRow(ctx, `SELECT v FROM tx_test WHERE id = 1`).Scan(&before); err != nil {
			t.Fatal(err)
		}

		read := make(chan struct{})
		proceed := make(chan struct{})
		errCh := make(chan error, 1)
		go func() {
			first := true
			errCh <- db.WithTx(ctx, ser, func(tx pgx.Tx) error {
				var v int
				if err := tx.QueryRow(ctx, `SELECT v FROM tx_test WHERE id = 1`).Scan(&v); err != nil {
					return err
				}
				if first {
					first = false
					close(read)
					<-proceed
				}
				_, err := tx.Exec(ctx, `UPDATE tx_test SET v = $1 WHERE id = 1`, v+1)
				return err
			})
		}()

		<-read
		err := db.WithTx(ctx, ser, func(tx pgx.Tx) error {
			var v int
			if err := tx.QueryRow(ctx, `SELECT v FROM tx_test WHERE id = 1`).Scan(&v); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE tx_test SET v = $1 WHERE id = 1`, v+1)
			return err
		})
		close(proceed)
		if err != nil {
			t.Fatal(err)
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}

		var after int
		if err := db.QueryRow(ctx, `SELECT v FROM tx_test WHERE id = 1`).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after != before+2 {
			t.Fatalf("lost update: v = %d, want %d", after, before+2)
		}
	})

	t.Run("query metrics and pool stats", func(t *testing.T) {
		qctx := WithQueryName(ctx, "it.probe")
		before := histogramCount(t, "it-main", "it.probe", "ok")
		if _, err := db.Exec(qctx, `SELECT 1`); err != nil {
			t.Fatal(err)
		}
		if histogramCount(t, "it-main", "it.probe", "ok") != before+1 {
			t.Fatal("query latency not recorded")
		}

		reg := prometheus.NewPedanticRegistry()
		reg.MustRegister(defaultMetrics().pool)
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		var acquires float64
		for _, f := range families {
			if f.GetName() != "postgres_pool_acquires_total" {
				continue
			}
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "pool" && l.GetValue() == "it-main" {
						acquires = m.GetCounter().GetValue()
					}
				}
			}
		}
		if acquires == 0 {
			t.Fatal("pool stats for it-main not exported")
		}
	})

	t.Run("cluster routes reads to a healthy replica", func(t *testing.T) {
		replica := newClient(t, WithPoolName("it-replica"))
		cluster, err := NewCluster(db, replica)
		if err != nil {
			t.Fatal(err)
		}
		if cluster.Replica() != replica {
			t.Fatal("expected the healthy replica")
		}
		replica.Close()
		if cluster.Replica() != db {
			t.Fatal("closed replica must fall back to the primary")
		}
	})

	t.Run("saturated pool keeps the client healthy", func(t *testing.T) {
		checker := &mockHealthChecker{}
		c := newClient(t,
			WithPoolName("it-saturated"),
			WithConnectionPool(1, 0, time.Hour),
			WithHealthChecker("db", checker),
			WithFailureThreshold(1),
		)
		conn, err := c.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()

		details := func() interface{} {
			checker.mu.Lock()
			defer checker.mu.Unlock()
			return checker.details
		}
		deadline := time.Now().Add(10 * time.Second)
		for details() != "pool saturated, status unchanged" {
			if time.Now().After(deadline) {
				t.Fatalf("saturation not detected, last details: %v", details())
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !c.Healthy() || !checker.getStatus() {
			t.Fatal("a saturated pool must not be reported unhealthy")
		}
	})

	t.Run("replication lag on a primary reads as zero", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		replica := newClient(t, WithPoolName("it-lag"), WithMaxReplicationLag(time.Second), WithRegisterer(reg))
		replica.lag.Store(int64(time.Hour)) // must be overwritten by the next check

		deadline := time.Now().Add(5 * time.Second)
		for replica.ReplicationLag() != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("lag not measured: %v", replica.ReplicationLag())
			}
			time.Sleep(20 * time.Millisecond)
		}
		cluster, err := NewCluster(db, replica)
		if err != nil {
			t.Fatal(err)
		}
		if cluster.Replica() != replica {
			t.Fatal("a replica within the lag limit must serve reads")
		}

		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, f := range families {
			names[f.GetName()] = true
		}
		for _, n := range []string{"postgres_replication_lag_seconds", "postgres_pool_acquires_total", "postgres_availability"} {
			if !names[n] {
				t.Fatalf("%s missing from the custom registerer: %v", n, names)
			}
		}
	})

	t.Run("health pings stay out of query metrics", func(t *testing.T) {
		newClient(t, WithPoolName("it-quiet"))
		time.Sleep(200 * time.Millisecond) // ~10 health ticks
		if n := histogramCount(t, "it-quiet", "other", "ok"); n != 0 {
			t.Fatalf("health pings recorded as queries: %d", n)
		}
	})
}
