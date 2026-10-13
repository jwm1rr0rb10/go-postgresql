package psql

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is the query surface shared by *Client, pgx.Tx and *pgx.Conn.
// Depend on it in repositories so the same code runs inside or outside a
// transaction and can be replaced by a mock (e.g. pgxmock) in tests.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// TxRunner runs a function inside a retried transaction (see Client.WithTx).
type TxRunner interface {
	WithTx(ctx context.Context, opts pgx.TxOptions, fn func(pgx.Tx) error) error
}

var (
	_ Querier  = (*Client)(nil)
	_ Querier  = (pgx.Tx)(nil)
	_ Querier  = (*pgx.Conn)(nil)
	_ TxRunner = (*Client)(nil)
)
