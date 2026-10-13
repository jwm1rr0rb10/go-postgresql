package psql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type recordingHealthChecker struct {
	mu       sync.Mutex
	statuses []bool
}

func (r *recordingHealthChecker) SetStatus(_ string, status bool, _ interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statuses = append(r.statuses, status)
}

func (r *recordingHealthChecker) snapshot() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.statuses...)
}

func TestMonitorTurnsUnhealthyAfterThreshold(t *testing.T) {
	// pgxpool connects lazily, so the pool exists while every ping fails.
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	rec := &recordingHealthChecker{}
	c := &Client{
		Pool:          pool,
		healthChecker: rec,
		healthName:    "db",
		labels:        availabilityLabels{host: "monitor-threshold-test"},
		metrics:       defaultMetrics(),
	}
	c.healthy.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.monitor(ctx, 5*time.Millisecond, 3)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for len(rec.snapshot()) < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	got := rec.snapshot()
	if len(got) < 4 {
		t.Fatalf("expected at least 4 reports, got %v", got)
	}
	want := []bool{true, true, false, false}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("reports = %v, want prefix %v", got, want)
		}
	}
}

func TestClientCloseIdempotent(t *testing.T) {
	c := &Client{cancelHealth: func() {}, healthDone: make(chan struct{}), metrics: defaultMetrics()}
	close(c.healthDone)
	shared := availabilityLabels{host: "close-idempotent-test"}
	c.labels = shared

	m := defaultMetrics()
	m.acquireAvailability(shared) // another live client on the same database
	m.acquireAvailability(shared) // c itself
	m.setAvailability(shared, true)

	c.Close()
	c.Close()
	if availabilityValue(t, shared) != 1 {
		t.Fatal("a second Close must not release another client's availability")
	}
	m.releaseAvailability(shared)
}
