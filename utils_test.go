package psql

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgerrcode"
)

func TestPrettySQL(t *testing.T) {
	input := "SELECT id\n\tFROM users   WHERE\nstatus = $1"
	got := PrettySQL(input)
	want := "SELECT id FROM users WHERE status = $1"
	if got != want {
		t.Fatalf("PrettySQL() = %q, want %q", got, want)
	}
}

func TestPrettySQLPreservesQuotedText(t *testing.T) {
	input := "SELECT 'a  b\n c', \"Col  Name\"\r\nFROM  t WHERE x = 'it''s  ok'"
	got := PrettySQL(input)
	want := "SELECT 'a  b\n c', \"Col  Name\" FROM t WHERE x = 'it''s  ok'"
	if got != want {
		t.Fatalf("PrettySQL() = %q, want %q", got, want)
	}
}

func TestIsErrUniqueViolation(t *testing.T) {
	pgErr := &PgError{Code: pgerrcode.UniqueViolation}
	wrapped := fmt.Errorf("insert failed: %w", pgErr)

	got, ok := IsErrUniqueViolation(wrapped)
	if !ok || got.Code != pgerrcode.UniqueViolation {
		t.Fatalf("expected unique violation, got ok=%v err=%v", ok, got)
	}

	if _, ok := IsErrUniqueViolation(errors.New("other")); ok {
		t.Fatal("expected false for non-pg error")
	}
}

func TestParsePgError(t *testing.T) {
	pgErr := &PgError{
		Code:    pgerrcode.UniqueViolation,
		Message: "duplicate key",
		Detail:  "Key (id)=(1) already exists.",
		Where:   "PL/pgSQL",
	}
	got := ParsePgError(pgErr)
	if !strings.Contains(got.Error(), "postgres error") {
		t.Fatalf("unexpected message: %s", got.Error())
	}

	var target *PgError
	if !errors.As(got, &target) || target.Code != pgerrcode.UniqueViolation {
		t.Fatal("ParsePgError must keep *PgError in the chain")
	}
	if !strings.Contains(got.Error(), "code="+pgerrcode.UniqueViolation) {
		t.Fatalf("message must carry the SQLSTATE: %s", got.Error())
	}

	plain := errors.New("network")
	if ParsePgError(plain) != plain {
		t.Fatal("expected original error for non-pg error")
	}
}

func TestToPostgresDSN(t *testing.T) {
	dsn := (&DSNConfig{
		Username: "user",
		Password: "p@ss:word",
		Host:     "db.example.com",
		Port:     "5432",
		Database: "mydb",
	}).ToPostgresDSN()

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	user, pass := parsed.User.Username(), ""
	if parsed.User != nil {
		pass, _ = parsed.User.Password()
	}
	if user != "user" || pass != "p@ss:word" {
		t.Fatalf("decoded credentials = %q/%q", user, pass)
	}
	if parsed.Hostname() != "db.example.com" || parsed.Path != "/mydb" {
		t.Fatalf("unexpected host/path: %s%s", parsed.Host, parsed.Path)
	}
}
