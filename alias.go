package psql

import (
	"github.com/jackc/pgx/v5/pgconn"
)

// PgError aliases pgconn.PgError to decouple from external library.
// Used for centralized PostgreSQL error handling.
type PgError = pgconn.PgError
