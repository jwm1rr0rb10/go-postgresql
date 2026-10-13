package psql

import (
	"errors"
	"testing"
)

func TestErrorWrappersNilSafe(t *testing.T) {
	wrappers := []func(error) error{
		ErrCommit,
		ErrRollback,
		ErrCreateTx,
		ErrCreateQuery,
		ErrScan,
		ErrDoQuery,
		ErrTimeout,
		ErrOverload,
	}

	for i, wrap := range wrappers {
		if wrap(nil) != nil {
			t.Fatalf("wrapper %d should return nil for nil input", i)
		}
	}
}

func TestErrorWrappersWrap(t *testing.T) {
	root := errors.New("connection reset")

	wrappers := []struct {
		name string
		fn   func(error) error
		want string
	}{
		{name: "commit", fn: ErrCommit, want: "commit transaction: connection reset"},
		{name: "rollback", fn: ErrRollback, want: "rollback transaction: connection reset"},
		{name: "create tx", fn: ErrCreateTx, want: "create transaction: connection reset"},
		{name: "create query", fn: ErrCreateQuery, want: "build SQL query: connection reset"},
		{name: "scan", fn: ErrScan, want: "scan row: connection reset"},
		{name: "do query", fn: ErrDoQuery, want: "execute query: connection reset"},
		{name: "timeout", fn: ErrTimeout, want: "db operation timeout: connection reset"},
		{name: "overload", fn: ErrOverload, want: "db overload: connection reset"},
	}

	for _, tt := range wrappers {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn(root)
			if !errors.Is(err, root) {
				t.Fatalf("expected wrapped root error")
			}
			if err.Error() != tt.want {
				t.Fatalf("unexpected message: %s", err.Error())
			}
		})
	}
}

func TestErrorWrapperSentinels(t *testing.T) {
	root := errors.New("boom")
	if err := ErrTimeout(root); !errors.Is(err, ErrOperationTimeout) || !errors.Is(err, root) {
		t.Fatalf("ErrTimeout must match ErrOperationTimeout and root: %v", err)
	}
	if err := ErrOverload(root); !errors.Is(err, ErrOverloaded) || !errors.Is(err, root) {
		t.Fatalf("ErrOverload must match ErrOverloaded and root: %v", err)
	}
}
