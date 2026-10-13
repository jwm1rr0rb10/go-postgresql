package psql

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

func startEmbeddedPostgres(t testing.TB) string {
	t.Helper()

	port := uint32(15432 + (time.Now().UnixNano() % 1000))
	db := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(port).
			Username("postgres").
			Password("postgres").
			Database("postgres"),
	)
	if err := db.Start(); err != nil {
		t.Skipf("embedded postgres unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Stop()
	})

	return "postgres://postgres:postgres@127.0.0.1:" + strconv.FormatUint(uint64(port), 10) + "/postgres?sslmode=disable"
}

func TestNewClientValidation(t *testing.T) {
	_, err := NewClient(context.Background(), nil)
	if !errors.Is(err, ErrNilConfig) {
		t.Fatalf("expected ErrNilConfig, got %v", err)
	}
}

func TestClientLifecycle(t *testing.T) {
	dsn := startEmbeddedPostgres(t)
	checker := &mockHealthChecker{}

	cfg, err := NewConfig(dsn,
		WithHealthChecker("db", checker),
		WithIntervalCheck(20*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	client, err := NewClient(connectCtx, cfg)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var value int
	if err := client.QueryRow(context.Background(), "SELECT 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("value = %d, want 1", value)
	}

	time.Sleep(60 * time.Millisecond)
	if checker.getCalls() == 0 {
		t.Fatal("expected health checker updates")
	}
	if !checker.getStatus() {
		t.Fatal("expected healthy status")
	}
}
