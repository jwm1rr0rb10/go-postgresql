# PSQL
**Production-Ready PostgreSQL Client with Retries, Health Checks and High-Load Optimizations for Go Services**

`psql` is a lightweight, battle-tested wrapper around [`github.com/jackc/pgx/v5/pgxpool`](https://github.com/jackc/pgx) that provides a clean, configurable and highly reliable PostgreSQL client.  
It includes smart connection pooling, automatic retries with jitter, dual health monitoring, centralized error handling and production-grade defaults — everything you need for high-load backends without boilerplate.

It supports:
- High-performance connection pool with sensible defaults
- Retry logic with exponential backoff + jitter
- Dual health checking (generic `HealthChecker` + Prometheus-style metrics)
- Centralized PostgreSQL error wrapping and helpers
- Graceful shutdown
- Pretty SQL logging
- Safe DSN builder
- Binary query execution mode

Perfect for REST APIs, gRPC services, microservices or any backend that needs stable and observable PostgreSQL access.

## Features
- **Zero boilerplate** configuration with powerful `Option` pattern
- Production-optimized defaults (50/10 connections, 1h lifetime, etc.)
- Automatic retries on connection failures with jitter
- Two-level health monitoring (custom interface + metrics)
- Centralized error wrappers (`ErrCommit`, `ErrDoQuery`, etc.)
- Built-in unique violation detection and error parsing
- `PrettySQL` utility for clean query logging
- Safe DSN construction (proper escaping)
- Thread-safe and context-aware
- Full `pgxpool.Pool` compatibility (embedded)
- 100% English godoc documentation

## Installation
```bash
go get github.com/jwm1rr0rb10/libraries/backend/golang/postgresql
```

---

The library depends on:

- github.com/jackc/pgx/v5
- github.com/jackc/pgx/v5/pgxpool
- (optional) your own HealthChecker and metrics implementation

After adding the module, run:
```bash
go mod tidy
```

---

## Quick Start
### 1. Basic usage (recommended)
```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jwm1rr0rb10/libraries/backend/golang/postgresql"
)

func main() {
	ctx := context.Background()

	// Create config with options
	cfg, err := psql.NewConfig("postgres://user:pass@localhost:5432/mydb?sslmode=disable",
		psql.WithMaxAttempts(7),
		psql.WithQueryExecMode(pgx.QueryExecModeCacheDescribe), // only behind PgBouncer
		psql.WithConnectionPool(100, 20, 2*time.Hour),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create client
	client, err := psql.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// You can use client exactly like *pgxpool.Pool
	rows, err := client.Query(ctx, "SELECT id, name FROM users WHERE status = $1", "active")
	// ... your normal pgx code
}
```

### 2. Using DSN builder

```go
dsnCfg := &psql.DSNConfig{
	Username: "user",
	Password: "super@secret:pass",
	Host:     "db.example.com",
	Port:     "5432",
	Database: "mydb",
}
dsn := dsnCfg.ToPostgresDSN()

cfg, _ := psql.NewConfig(dsn)
```

### 3. Health checking + metrics
```go
// Example with custom health checker
type MyHealth struct{ /* ... */ }
func (h *MyHealth) SetStatus(name string, status bool, details interface{}) { /* ... */ }

cfg, _ := psql.NewConfig(dsn,
	psql.WithHealthChecker("postgres-main", &MyHealth{}),
	psql.WithIntervalCheck(15*time.Second),
)
```

---

## Core Concepts
### Config + Option pattern
All configuration is done through `NewConfig(dsn, options...)`.
You can override retries, pool settings, health checks, binary mode, etc.

### HealthChecker interface

```go
type HealthChecker interface {
	SetStatus(dependencyName string, status bool, details interface{})
}
```

Used by the library for background health reporting.

### Error handling
```go
if pgErr, ok := psql.IsErrUniqueViolation(err); ok {
	// handle unique violation
}

err = psql.ParsePgError(err) // nice formatted error
```

### PrettySQL

```go
clean := psql.PrettySQL(`
    SELECT * FROM users 
    WHERE id = $1
`)
```

---

## API Overview
### Main functions & types

| Function / Type            | Description                             |
|:---------------------------|:----------------------------------------|
| "NewConfig(dsn, opts...)"  | Parse DSN and apply options             |
| "NewClient(ctx, cfg)"      | Create ready-to-use connection pool     |
| (*Client).Close()          | Graceful shutdown                       |
| WithMaxAttempts            | Set retry attempts                      |
| WithMaxDelay               | Set maximum backoff delay               |
| WithQueryExecMode          | pgx query exec mode (CacheDescribe/Exec for PgBouncer) |
| WithHealthChecker          | Attach custom health reporter           |
| WithIntervalCheck          | Set health check interval               |
| WithConnectionPool         | Configure pool size and lifetime        |
| "ErrCommit, ErrDoQuery..." | Error wrappers with context             |
| IsErrUniqueViolation       | Check for unique violation              |
| ParsePgError               | PostgreSQL error details; keeps *PgError in the chain |
| Config.RedactedDSN()       | DSN with the password masked, safe for logs |
| PrettySQL                  | Collapse whitespace for logs            |
| DSNConfig.ToPostgresDSN()  | Safe DSN builder                        |

`Client` embeds `*pgxpool.Pool`, so you get all `pgx methods` + the extra helpers.

---

## Complete Example (REST service)
```go
func initDB() (*psql.Client, error) {
	cfg, err := psql.NewConfig(os.Getenv("DATABASE_URL"),
		psql.WithMaxAttempts(10),
		psql.WithQueryExecMode(pgx.QueryExecModeCacheDescribe), // only behind PgBouncer
		psql.WithHealthChecker("postgres", healthChecker),
		psql.WithIntervalCheck(10*time.Second),
	)
	if err != nil {
		return nil, err
	}

	return psql.NewClient(context.Background(), cfg)
}

// In your handler:
rows, err := db.Query(ctx, "SELECT * FROM orders WHERE user_id = $1", userID)
```

---

## Testing

Tests use **embedded PostgreSQL** — no Docker required.

```bash
GOTOOLCHAIN=go1.26.2 go test -cover ./...
```

Coverage includes config options, retry logic, metrics availability, health checks, error wrappers, and client lifecycle.

---

## Production / highload

### Pool defaults
`max=20`, `min=0` per instance (no idle connections pinned by every replica of a
service), lifetime 1h with 10% jitter so connections are not recycled at once.
Precedence: package defaults < `pool_*` DSN parameters < options.

```go
cfg, _ := psql.NewConfig(dsn,
	psql.WithPoolName("primary"),                 // "pool" label in metrics/traces
	psql.WithConnectionPool(40, 0, time.Hour),
	psql.WithMaxConnIdleTime(5*time.Minute),
	psql.WithStatementTimeout(5*time.Second),     // server-side backstop
	psql.WithApplicationName("kline-service"),    // visible in pg_stat_activity
)
```

### Observability
Built in, no extra setup:
- `postgres_query_duration_seconds{pool,query,status}`: label queries with
  `psql.WithQueryName(ctx, "kline.save")`, otherwise the SQL verb is used.
- `postgres_pool_*`: `pgxpool.Stat` (acquired/idle conns, empty acquires,
  acquire wait, destroyed conns). Rising `postgres_pool_empty_acquires_total`
  means the pool is too small.
- OpenTelemetry client spans for queries, batches and COPY via the global
  TracerProvider. `WithTracing(false)`, `WithTraceSQL(false)` to tune;
  `WithQueryTracer` adds your own pgx tracer.

### Transactions
```go
err := client.WithTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
	// must be safe to re-run: retried on 40001 / 40P01 (WithTxMaxAttempts, default 3)
	_, err := tx.Exec(ctx, "UPDATE ...")
	return err
})
```

### Read replicas
```go
cluster, _ := psql.NewCluster(primary, replica1, replica2)
cluster.Replica().Query(ctx, ...) // healthy replica round-robin, falls back to primary
cluster.Primary().Exec(ctx, ...)  // writes and read-your-writes
```
Replication lag is not tracked.

### Testability
Depend on `psql.Querier` (satisfied by `*Client`, `pgx.Tx`, `*pgx.Conn`) and
`psql.TxRunner` instead of `*psql.Client` to mock the database.

### Benchmarks
`go test -run '^$' -bench . ./pkg/golang/postgresql` (`-short` skips the
embedded-PostgreSQL round-trip benchmark).

## Changelog

### 1.2.0

- Breaking: pool defaults `max=20`, `min=0` (were 50/10); `Config.DSN()` replaced by `RedactedDSN()`; `NewClient` no longer accepts a nil context
- OpenTelemetry spans, query latency histogram, `pgxpool.Stat` metrics
- `WithTx` with serialization/deadlock retry, `Cluster` for read replicas, `Querier` interface
- Pool tuning, `statement_timeout`, `application_name`, TLS and connection hooks options
- Fixed: DSN `pool_*` params ignored, `ParsePgError` dropped the cause, duplicate health pings

### 1.1.0

- Expanded tests: config options, metrics, connection failure paths, all error wrappers
- Embedded PostgreSQL tests (no Docker required for CI)
- README install path fixed (`postgresql`, not `psql`)
- Coverage: **94%+**

### 1.0.5

- Initial production release

---

## License

[MIT License](https://github.com/jwm1rr0rb10/libraries/blob/main/backend/golang/LICENSE) – © Raman Zaitsau [@jwm1rrr0rb10](https://github.com/jwm1rr0rb10)

---

### 🙌 Contributions

PRs and suggestions welcome! Feel free 




