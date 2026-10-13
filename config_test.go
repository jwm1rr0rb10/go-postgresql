package psql

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type mockHealthChecker struct {
	mu      sync.Mutex
	name    string
	status  bool
	details interface{}
	calls   int
}

func (m *mockHealthChecker) SetStatus(name string, status bool, details interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.name = name
	m.status = status
	m.details = details
	m.calls++
}

func TestConfigValidate(t *testing.T) {
	valid, err := NewConfig("postgres://user:pass@localhost:5432/app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		cfg     *Config
		wantErr error
	}{
		{name: "nil config", cfg: nil, wantErr: ErrNilConfig},
		{name: "empty dsn", cfg: &Config{pgxConfig: valid.pgxConfig}, wantErr: ErrEmptyDSN},
		{name: "invalid config", cfg: &Config{dsn: "postgres://localhost/db"}, wantErr: ErrInvalidConfig},
		{name: "valid config", cfg: valid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	checker := &mockHealthChecker{}
	cfg, err := NewConfig("postgres://user:pass@localhost:5432/app?sslmode=disable",
		WithHealthChecker("", checker),
		WithIntervalCheck(0),
		WithMaxAttempts(0),
	)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.maxAttempts != defaultMaxAttempts {
		t.Fatalf("maxAttempts = %d", cfg.maxAttempts)
	}
	if cfg.health.name != defaultHealthCheckName {
		t.Fatalf("health name = %q", cfg.health.name)
	}
	if cfg.HealthCheckInterval() != defaultIntervalCheck {
		t.Fatalf("interval = %v", cfg.HealthCheckInterval())
	}
	if cfg.RedactedDSN() == "" {
		t.Fatal("expected dsn")
	}
}

func TestWithIntervalCheck(t *testing.T) {
	cfg, err := NewConfig("postgres://user:pass@localhost:5432/app?sslmode=disable",
		WithIntervalCheck(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HealthCheckInterval() != 3*time.Second {
		t.Fatalf("interval = %v", cfg.HealthCheckInterval())
	}
}

func (m *mockHealthChecker) getName() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.name
}

func (m *mockHealthChecker) getStatus() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

func (m *mockHealthChecker) getCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}
