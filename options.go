package psql

import (
	"context"
	"crypto/tls"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// WithPoolName names the pool in metrics and traces ("pool" label). Give each
// client a distinct name when one process talks to several databases or to a
// primary and its replicas, e.g. "primary", "replica-1".
func WithPoolName(name string) Option {
	return func(cfg *Config) {
		cfg.poolName = name
	}
}

// WithMaxConnIdleTime closes connections idle for longer than d, letting the
// pool shrink back after a burst. pgx defaults to 30 minutes.
func WithMaxConnIdleTime(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.pgxConfig.MaxConnIdleTime = d
	}
}

// WithHealthCheckPeriod sets how often pgx checks idle connections and
// enforces lifetime/idle limits. pgx defaults to 1 minute.
func WithHealthCheckPeriod(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.pgxConfig.HealthCheckPeriod = d
	}
}

// WithMaxConnLifetimeJitter spreads connection recycling over [lifetime,
// lifetime+d) so connections opened together are not all closed at once.
// Defaults to 10% of the max connection lifetime.
func WithMaxConnLifetimeJitter(d time.Duration) Option {
	return func(cfg *Config) {
		cfg.poolSettings.maxConnLifetimeJitter = d
	}
}

// WithRuntimeParam sets a server run-time parameter in the startup message of
// every new connection. PgBouncer rejects unknown startup parameters unless
// they are listed in its ignore_startup_parameters.
func WithRuntimeParam(key, value string) Option {
	return func(cfg *Config) {
		if cfg.pgxConfig.ConnConfig.RuntimeParams == nil {
			cfg.pgxConfig.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.pgxConfig.ConnConfig.RuntimeParams[key] = value
	}
}

// WithStatementTimeout makes the server abort any statement running longer
// than d. It is the backstop for queries whose context carries no deadline.
func WithStatementTimeout(d time.Duration) Option {
	return WithRuntimeParam("statement_timeout", strconv.FormatInt(d.Milliseconds(), 10))
}

// WithApplicationName tags connections in pg_stat_activity and server logs.
func WithApplicationName(name string) Option {
	return WithRuntimeParam("application_name", name)
}

// WithTLSConfig forces TLS with the given config (e.g. mTLS client
// certificates). It overrides the DSN sslmode: plaintext fallbacks are
// dropped and ServerName defaults to each host.
func WithTLSConfig(tlsCfg *tls.Config) Option {
	return func(cfg *Config) {
		if tlsCfg == nil {
			return
		}
		cc := cfg.pgxConfig.ConnConfig
		cc.TLSConfig = tlsForHost(tlsCfg, cc.Host)

		fallbacks := cc.Fallbacks[:0]
		for _, fb := range cc.Fallbacks {
			if fb.TLSConfig == nil {
				continue
			}
			fb.TLSConfig = tlsForHost(tlsCfg, fb.Host)
			fallbacks = append(fallbacks, fb)
		}
		cc.Fallbacks = fallbacks
	}
}

func tlsForHost(base *tls.Config, host string) *tls.Config {
	c := base.Clone()
	if c.ServerName == "" {
		c.ServerName = host
	}
	return c
}

// WithAfterConnect runs fn on every new connection before it joins the pool,
// e.g. to register custom types. An error discards the connection.
func WithAfterConnect(fn func(context.Context, *pgx.Conn) error) Option {
	return func(cfg *Config) {
		cfg.pgxConfig.AfterConnect = fn
	}
}

// WithPrepareConn runs fn before a connection is handed out by Acquire.
// Returning false (or an error) destroys the connection and acquires another.
func WithPrepareConn(fn func(context.Context, *pgx.Conn) (bool, error)) Option {
	return func(cfg *Config) {
		cfg.pgxConfig.PrepareConn = fn
	}
}

// WithTracing toggles OpenTelemetry spans for queries, batches and COPY.
// Query latency metrics are recorded either way. Enabled by default; spans go
// to the global TracerProvider and cost nothing until one is installed.
func WithTracing(enabled bool) Option {
	return func(cfg *Config) {
		cfg.tracing = enabled
	}
}

// WithTraceSQL toggles the db.query.text span attribute. Queries are
// parameterized so values are not included, but disable it when the SQL text
// itself is sensitive or very large. Enabled by default.
func WithTraceSQL(enabled bool) Option {
	return func(cfg *Config) {
		cfg.traceSQL = enabled
	}
}

// WithQueryTracer adds a pgx tracer (e.g. tracelog) next to the built-in one.
func WithQueryTracer(t pgx.QueryTracer) Option {
	return func(cfg *Config) {
		if t != nil {
			cfg.tracers = append(cfg.tracers, t)
		}
	}
}

// WithTxMaxAttempts sets how many times WithTx runs a transaction that fails
// with a serialization failure or deadlock. Defaults to 3.
func WithTxMaxAttempts(n int) Option {
	return func(cfg *Config) {
		cfg.txMaxAttempts = n
	}
}
