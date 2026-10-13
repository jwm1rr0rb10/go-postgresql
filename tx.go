package psql

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/jwm1rr0rb10/go-errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
)

const (
	// rollbackTimeout bounds a rollback that runs after the caller's context
	// is already done, so a dead connection cannot block forever.
	rollbackTimeout = 5 * time.Second
	maxTxRetryDelay = time.Second
)

// IsRetryableTxError reports errors after which the whole transaction may be
// retried as is: serialization failures and deadlocks.
func IsRetryableTxError(err error) bool {
	var pgErr *PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == pgerrcode.SerializationFailure || pgErr.Code == pgerrcode.DeadlockDetected
}

// WithTx runs fn in a transaction: it commits when fn returns nil and rolls
// back on error or panic. Serialization failures and deadlocks (common under
// RepeatableRead/Serializable and hot rows) restart the whole transaction up to
// WithTxMaxAttempts times, so fn must be safe to re-run: keep side effects
// outside the database (HTTP calls, channel sends) out of it.
func (c *Client) WithTx(ctx context.Context, opts pgx.TxOptions, fn func(pgx.Tx) error) error {
	maxAttempts := c.txMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultTxMaxAttempts
	}

	for attempt := 0; ; attempt++ {
		err := c.runTx(ctx, opts, fn)
		if err == nil || !IsRetryableTxError(err) || attempt+1 >= maxAttempts {
			return err
		}

		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(txRetryDelay(attempt)):
		}
	}
}

func txRetryDelay(attempt int) time.Duration {
	d := maxTxRetryDelay
	if attempt < maxBackoffShift {
		if b := baseRetryDelay << attempt; b < d {
			d = b
		}
	}
	// Full jitter: concurrent conflicting transactions must not retry in step.
	return time.Duration(rand.Int64N(int64(d)) + 1)
}

func (c *Client) runTx(ctx context.Context, opts pgx.TxOptions, fn func(pgx.Tx) error) (err error) {
	tx, err := c.BeginTx(ctx, opts)
	if err != nil {
		return ErrCreateTx(err)
	}

	rollback := func() error {
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()
		if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return ErrRollback(rbErr)
		}
		return nil
	}

	defer func() {
		if p := recover(); p != nil {
			_ = rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := rollback(); rbErr != nil {
			return errors.Join(err, rbErr)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrCommit(err)
	}
	return nil
}
