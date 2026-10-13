package psql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgerrcode"
)

// PrettySQL collapses runs of whitespace (spaces, tabs, CR, LF) into a single
// space for compact logging. Whitespace inside single-quoted string literals
// and double-quoted identifiers is preserved.
func PrettySQL(q string) string {
	var b strings.Builder
	b.Grow(len(q))

	var quote rune
	lastSpace := false
	for _, r := range q {
		if quote != 0 {
			b.WriteRune(r)
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case ' ', '\t', '\n', '\r':
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
			continue
		case '\'', '"':
			quote = r
		}
		b.WriteRune(r)
		lastSpace = false
	}
	return b.String()
}

// IsErrUniqueViolation checks if the error is a PostgreSQL unique violation error.
func IsErrUniqueViolation(err error) (*PgError, bool) {
	var pgErr *PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		return pgErr, true
	}
	return nil, false
}

// pgDetailError renders PostgreSQL error details while keeping the original
// error in the chain.
type pgDetailError struct {
	pgErr *PgError
	err   error
}

func (e *pgDetailError) Error() string {
	return fmt.Sprintf(
		"postgres error: code=%s message=%q detail=%q where=%q",
		e.pgErr.Code,
		e.pgErr.Message,
		e.pgErr.Detail,
		e.pgErr.Where,
	)
}

func (e *pgDetailError) Unwrap() error { return e.err }

// ParsePgError formats details from a PostgreSQL error into the message while
// keeping the original error in the chain, so errors.As(err, &pgErr) still works.
// Returns the original error unchanged if it's not a *PgError.
func ParsePgError(err error) error {
	var pgErr *PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	return &pgDetailError{pgErr: pgErr, err: err}
}
