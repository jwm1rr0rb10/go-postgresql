package psql

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"
)

func TestDoWithAttemptsSuccess(t *testing.T) {
	attempts := 0
	r := rand.New(rand.NewPCG(1, 0))

	err := DoWithAttempts(context.Background(), r, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary")
		}
		return nil
	}, 5, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestDoWithAttemptsExhausted(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 0))
	root := errors.New("persistent")

	err := DoWithAttempts(context.Background(), r, func() error {
		return root
	}, 3, 10*time.Millisecond)

	if !errors.Is(err, root) {
		t.Fatalf("expected wrapped root error, got %v", err)
	}
}

func TestDoWithAttemptsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := rand.New(rand.NewPCG(3, 0))
	err := DoWithAttempts(ctx, r, func() error {
		return errors.New("fail")
	}, 5, time.Second)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestAddJitter(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 0))
	base := 100 * time.Millisecond

	for range 20 {
		got := addJitter(base, r, jitterFactor)
		if got < time.Duration(float64(base)*(1-jitterFactor/2)) ||
			got > time.Duration(float64(base)*(1+jitterFactor/2)) {
			t.Fatalf("jitter out of bounds: %v", got)
		}
	}
}

func TestDoWithAttemptsBackoffNoOverflow(t *testing.T) {
	// With an unbounded 2^i the delay overflows int64 past ~40 attempts and
	// turns negative, so retries stop waiting entirely.
	r := rand.New(rand.NewPCG(5, 0))
	const attempts = 70
	maxDelay := time.Millisecond

	start := time.Now()
	_ = DoWithAttempts(context.Background(), r, func() error {
		return errors.New("fail")
	}, attempts, maxDelay)

	minTotal := time.Duration(attempts-1) * time.Duration(float64(maxDelay)*(1-jitterFactor/2))
	if elapsed := time.Since(start); elapsed < minTotal {
		t.Fatalf("elapsed %v < %v: some backoffs were skipped", elapsed, minTotal)
	}
}
