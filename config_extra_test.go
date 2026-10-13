package psql

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestConfigOptionSetters(t *testing.T) {
	cfg, err := NewConfig("postgres://user:pass@localhost:5432/app?sslmode=disable",
		WithMaxDelay(5*time.Second),
		WithBinaryExecMode(true),
		WithConnectionPool(25, 5, 30*time.Minute),
		WithMaxAttempts(7),
	)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.maxDelay != 5*time.Second {
		t.Fatalf("maxDelay = %v", cfg.maxDelay)
	}
	if cfg.maxAttempts != 7 {
		t.Fatalf("maxAttempts = %d", cfg.maxAttempts)
	}
	if cfg.poolSettings.maxConns != 25 || cfg.poolSettings.minConns != 5 {
		t.Fatalf("pool settings = %+v", cfg.poolSettings)
	}
	if cfg.pgxConfig.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeCacheDescribe {
		t.Fatal("expected binary exec mode")
	}
}

func TestNewConfigInvalidDSN(t *testing.T) {
	_, err := NewConfig("not-a-valid-dsn")
	if err == nil {
		t.Fatal("expected config parse error")
	}
}

func TestApplyDefaultsNilConfig(t *testing.T) {
	var cfg *Config
	cfg.applyDefaults()
}

func TestNilConfigHelpers(t *testing.T) {
	var cfg *Config
	if cfg.RedactedDSN() != "" {
		t.Fatal("nil config dsn should be empty")
	}
	if cfg.HealthCheckInterval() != defaultIntervalCheck {
		t.Fatal("nil config should return default interval")
	}
}

func TestNewConfigPoolSettingsPrecedence(t *testing.T) {
	const base = "postgres://user:pass@localhost:5432/app?sslmode=disable"

	cfg, err := NewConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.MaxConns != defaultMaxConnections || cfg.pgxConfig.MinConns != defaultMinConnections {
		t.Fatalf("defaults not applied: max=%d min=%d", cfg.pgxConfig.MaxConns, cfg.pgxConfig.MinConns)
	}

	cfg, err = NewConfig(base + "&pool_max_conns=7&pool_min_conns=2&pool_max_conn_lifetime=10m")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.MaxConns != 7 || cfg.pgxConfig.MinConns != 2 || cfg.pgxConfig.MaxConnLifetime != 10*time.Minute {
		t.Fatalf("dsn pool params overwritten: max=%d min=%d lifetime=%v",
			cfg.pgxConfig.MaxConns, cfg.pgxConfig.MinConns, cfg.pgxConfig.MaxConnLifetime)
	}

	cfg, err = NewConfig("host=localhost dbname=app pool_max_conns = 9")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.MaxConns != 9 {
		t.Fatalf("keyword dsn pool_max_conns ignored: %d", cfg.pgxConfig.MaxConns)
	}

	cfg, err = NewConfig(base+"&pool_max_conns=7", WithConnectionPool(30, 3, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.MaxConns != 30 || cfg.pgxConfig.MinConns != 3 {
		t.Fatalf("options must override dsn: max=%d min=%d", cfg.pgxConfig.MaxConns, cfg.pgxConfig.MinConns)
	}
}

func TestWithQueryExecMode(t *testing.T) {
	cfg, err := NewConfig("postgres://user:pass@localhost:5432/app", WithQueryExecMode(pgx.QueryExecModeExec))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.pgxConfig.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeExec {
		t.Fatalf("exec mode = %v", cfg.pgxConfig.ConnConfig.DefaultQueryExecMode)
	}
}

func TestRedactedDSN(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{
			dsn:  "postgres://user:s3cret@localhost:5432/app?sslmode=disable",
			want: "postgres://user:xxxxx@localhost:5432/app?sslmode=disable",
		},
		{
			dsn:  "postgres://user@localhost:5432/app?password=s3cret&sslmode=disable",
			want: "postgres://user@localhost:5432/app?password=xxxxx&sslmode=disable",
		},
		{
			dsn:  "host=localhost user=u password=s3cret dbname=app",
			want: "host=localhost user=u password=xxxxx dbname=app",
		},
		{
			dsn:  "host=localhost password = 'with space\\'s' dbname=app",
			want: "host=localhost password = xxxxx dbname=app",
		},
	}
	for _, tt := range tests {
		cfg, err := NewConfig(tt.dsn)
		if err != nil {
			t.Fatal(err)
		}
		got := cfg.RedactedDSN()
		if got != tt.want {
			t.Fatalf("RedactedDSN() = %q, want %q", got, tt.want)
		}
		if strings.Contains(got, "s3cret") || strings.Contains(got, "with space") {
			t.Fatalf("password leaked: %q", got)
		}
	}
}
