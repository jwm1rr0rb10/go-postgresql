package psql

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const optionsTestDSN = "postgres://user:pass@localhost:5432/app?sslmode=disable"

func TestPoolDefaults(t *testing.T) {
	cfg, err := NewConfig(optionsTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.pgxConfig
	if p.MinConns != 0 || p.MaxConns != defaultMaxConnections {
		t.Fatalf("min/max = %d/%d", p.MinConns, p.MaxConns)
	}
	if p.MaxConnLifetimeJitter != defaultConnLifetime/10 {
		t.Fatalf("lifetime jitter = %v", p.MaxConnLifetimeJitter)
	}
	if cfg.poolName != defaultPoolName || cfg.txMaxAttempts != defaultTxMaxAttempts {
		t.Fatalf("pool name / tx attempts = %q / %d", cfg.poolName, cfg.txMaxAttempts)
	}
	if !cfg.tracing || !cfg.traceSQL {
		t.Fatal("tracing must be on by default")
	}
}

func TestLifetimeJitterPrecedence(t *testing.T) {
	cfg, err := NewConfig(optionsTestDSN, WithConnectionPool(10, 0, 20*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.MaxConnLifetimeJitter != 2*time.Minute {
		t.Fatalf("default jitter must follow lifetime: %v", cfg.pgxConfig.MaxConnLifetimeJitter)
	}

	cfg, err = NewConfig(optionsTestDSN + "&pool_max_conn_lifetime_jitter=0s")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.MaxConnLifetimeJitter != 0 {
		t.Fatalf("explicit DSN jitter=0 overwritten: %v", cfg.pgxConfig.MaxConnLifetimeJitter)
	}

	cfg, err = NewConfig(optionsTestDSN+"&pool_max_conn_lifetime_jitter=1m", WithMaxConnLifetimeJitter(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.MaxConnLifetimeJitter != 5*time.Second {
		t.Fatalf("option must override DSN jitter: %v", cfg.pgxConfig.MaxConnLifetimeJitter)
	}
}

func TestPoolTuningOptions(t *testing.T) {
	afterConnect := func(context.Context, *pgx.Conn) error { return nil }
	prepare := func(context.Context, *pgx.Conn) (bool, error) { return true, nil }

	cfg, err := NewConfig(optionsTestDSN,
		WithPoolName("replica-1"),
		WithMaxConnIdleTime(3*time.Minute),
		WithHealthCheckPeriod(15*time.Second),
		WithStatementTimeout(1500*time.Millisecond),
		WithApplicationName("kalipso"),
		WithRuntimeParam("lock_timeout", "2000"),
		WithAfterConnect(afterConnect),
		WithPrepareConn(prepare),
		WithTracing(false),
		WithTraceSQL(false),
		WithTxMaxAttempts(7),
	)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.pgxConfig
	if p.MaxConnIdleTime != 3*time.Minute || p.HealthCheckPeriod != 15*time.Second {
		t.Fatalf("idle/health = %v/%v", p.MaxConnIdleTime, p.HealthCheckPeriod)
	}
	rp := p.ConnConfig.RuntimeParams
	if rp["statement_timeout"] != "1500" || rp["application_name"] != "kalipso" || rp["lock_timeout"] != "2000" {
		t.Fatalf("runtime params = %v", rp)
	}
	if p.AfterConnect == nil || p.PrepareConn == nil {
		t.Fatal("hooks not set")
	}
	if cfg.poolName != "replica-1" || cfg.tracing || cfg.traceSQL || cfg.txMaxAttempts != 7 {
		t.Fatalf("cfg = name %q tracing %v sql %v tx %d", cfg.poolName, cfg.tracing, cfg.traceSQL, cfg.txMaxAttempts)
	}
}

func TestWithTLSConfigDropsPlaintextFallbacks(t *testing.T) {
	custom := &tls.Config{MinVersion: tls.VersionTLS13}
	cfg, err := NewConfig(
		"postgres://u:p@db1.example.com:5432,db2.example.com:5432/app?sslmode=prefer",
		WithTLSConfig(custom),
	)
	if err != nil {
		t.Fatal(err)
	}
	cc := cfg.pgxConfig.ConnConfig
	if cc.TLSConfig == nil || cc.TLSConfig.MinVersion != tls.VersionTLS13 || cc.TLSConfig.ServerName != "db1.example.com" {
		t.Fatalf("primary TLS = %+v", cc.TLSConfig)
	}
	if len(cc.Fallbacks) == 0 {
		t.Fatal("TLS fallbacks for other hosts must remain")
	}
	for _, fb := range cc.Fallbacks {
		if fb.TLSConfig == nil {
			t.Fatalf("plaintext fallback to %s kept", fb.Host)
		}
		if fb.TLSConfig.ServerName != fb.Host || fb.TLSConfig.MinVersion != tls.VersionTLS13 {
			t.Fatalf("fallback TLS for %s = %+v", fb.Host, fb.TLSConfig)
		}
	}
	if custom.ServerName != "" {
		t.Fatal("caller's tls.Config must not be mutated")
	}

	if _, err := NewConfig(optionsTestDSN, WithTLSConfig(nil)); err != nil {
		t.Fatal(err)
	}
}
