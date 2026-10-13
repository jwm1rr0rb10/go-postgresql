package psql

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace/noop"
)

var benchSQL = `-- load the latest candles
SELECT open_time, open, high, low, close, volume
FROM   klines
WHERE  symbol = $1
  AND  interval = $2
ORDER  BY open_time DESC
LIMIT  $3`

func BenchmarkPrettySQL(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = PrettySQL(benchSQL)
	}
}

func BenchmarkSQLVerb(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = sqlVerb(benchSQL)
	}
}

// BenchmarkObserver measures the per-query overhead the built-in tracer adds
// on top of the round-trip, without a database.
func BenchmarkObserver(b *testing.B) {
	end := pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag("SELECT 100")}
	start := pgx.TraceQueryStartData{SQL: benchSQL}

	cases := []struct {
		name    string
		tracing bool
	}{
		{"metrics only", false},
		{"metrics+noop spans", true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			o := &observer{
				pool:     "bench",
				tracer:   noop.NewTracerProvider().Tracer("bench"),
				tracing:  tc.tracing,
				traceSQL: true,
			}
			ctx := WithQueryName(context.Background(), "kline.latest")
			b.ReportAllocs()
			for b.Loop() {
				qctx := o.TraceQueryStart(ctx, nil, start)
				o.TraceQueryEnd(qctx, nil, end)
			}
		})
	}
}

func BenchmarkClusterReplica(b *testing.B) {
	c, _ := NewCluster(healthyClient("p", true),
		healthyClient("r1", true), healthyClient("r2", false), healthyClient("r3", true))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.Replica()
		}
	})
}

// BenchmarkQueryRoundTrip compares a real SELECT with and without spans.
// It starts an embedded PostgreSQL, so it is skipped with -short.
func BenchmarkQueryRoundTrip(b *testing.B) {
	if testing.Short() {
		b.Skip("needs embedded PostgreSQL")
	}
	dsn := startEmbeddedPostgres(b)

	for _, tracing := range []bool{false, true} {
		name := "tracing=off"
		if tracing {
			name = "tracing=on"
		}
		b.Run(name, func(b *testing.B) {
			cfg, err := NewConfig(dsn, WithTracing(tracing), WithPoolName("bench-"+name))
			if err != nil {
				b.Fatal(err)
			}
			c, err := NewClient(context.Background(), cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()

			ctx := context.Background()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var v int
				for pb.Next() {
					if err := c.QueryRow(ctx, "SELECT $1::int", 1).Scan(&v); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
