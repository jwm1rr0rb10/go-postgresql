package psql

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestNewClientConnectionFailure(t *testing.T) {
	cfg, err := NewConfig(
		"postgres://postgres:postgres@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1",
		WithMaxAttempts(2),
		WithMaxDelay(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = NewClient(ctx, cfg)
	if err == nil {
		t.Fatal("expected connection failure")
	}
}

func TestClientCloseNil(t *testing.T) {
	var client *Client
	client.Close()
}

func TestHealthCheckerReportsUnhealthyAfterDatabaseStops(t *testing.T) {
	dsn := startEmbeddedPostgres(t)
	checker := &mockHealthChecker{}

	cfg, err := NewConfig(dsn,
		WithHealthChecker("db", checker),
		WithIntervalCheck(20*time.Millisecond),
		WithMaxAttempts(2),
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

	// Simulate backend outage by closing the pool.
	client.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if checker.getCalls() > 0 && !checker.getStatus() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected unhealthy status after close, calls=%d status=%v", checker.getCalls(), checker.getStatus())
}

func TestNewClientValidationEmptyDSN(t *testing.T) {
	valid, err := NewConfig("postgres://user:pass@localhost:5432/app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewClient(context.Background(), &Config{pgxConfig: valid.pgxConfig})
	if !errors.Is(err, ErrEmptyDSN) {
		t.Fatalf("expected ErrEmptyDSN, got %v", err)
	}
}

func TestNewClientAttemptTimeoutDoesNotHang(t *testing.T) {
	// A listener that accepts but never speaks the protocol: without a
	// per-attempt deadline the startup ping would block on the parent context.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	cfg, err := NewConfig(
		"postgres://u:p@"+ln.Addr().String()+"/db?sslmode=disable&connect_timeout=1",
		WithMaxAttempts(2),
		WithMaxDelay(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_, err = NewClient(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected connection failure")
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("startup took %v, attempts are not bounded", elapsed)
	}
}

func TestClientCloseMarksUnavailable(t *testing.T) {
	dsn := startEmbeddedPostgres(t)
	cfg, err := NewConfig(dsn, WithIntervalCheck(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	client, err := NewClient(connectCtx, cfg)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	l := labelsFromConfig(cfg)
	if availabilityValue(t, l) != 1 {
		t.Fatal("expected available after connect")
	}

	client.Close()
	if availabilityValue(t, l) != 0 {
		t.Fatal("expected unavailable after close")
	}
}
