package psql

import (
	"fmt"

	"github.com/jwm1rr0rb10/go-errors"
)

var (
	ErrNilConfig     = errors.New("postgresql config is nil")
	ErrEmptyDSN      = errors.New("postgresql dsn is empty")
	ErrInvalidConfig = errors.New("postgresql config is invalid")
	ErrNilPrimary    = errors.New("postgresql cluster primary is nil")

	// ErrOperationTimeout and ErrOverloaded are matched with errors.Is on
	// results of ErrTimeout and ErrOverload.
	ErrOperationTimeout = errors.New("db operation timeout")
	ErrOverloaded       = errors.New("db overload")
)

func ErrCommit(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("commit transaction: %w", err)
}

func ErrRollback(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("rollback transaction: %w", err)
}

func ErrCreateTx(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("create transaction: %w", err)
}

func ErrCreateQuery(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("build SQL query: %w", err)
}

func ErrScan(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("scan row: %w", err)
}

func ErrDoQuery(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("execute query: %w", err)
}

func ErrTimeout(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrOperationTimeout, err)
}

func ErrOverload(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrOverloaded, err)
}
