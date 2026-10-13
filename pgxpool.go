package psql

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Pool defaults are sized for many replicas of a service sharing one
	// PostgreSQL: no idle connections are pinned per instance (min=0) and the
	// per-instance ceiling stays modest. Size explicitly for heavy workloads.
	defaultMaxConnections   = 20
	defaultMinConnections   = 0
	defaultPoolName         = "default"
	defaultTxMaxAttempts    = 3
	defaultMaxRetryDelay    = 30 * time.Second
	defaultConnLifetime     = time.Hour
	defaultMaxAttempts      = 5
	defaultIntervalCheck    = 10 * time.Second
	defaultFailureThreshold = 3
	defaultHealthCheckName  = "postgresql"
	baseRetryDelay          = 100 * time.Millisecond
	jitterFactor            = 0.2
	pingTimeout             = 2 * time.Second
	// connectAttemptTimeout bounds a single startup ping when the DSN sets no
	// connect_timeout, so one attempt cannot hang for the whole parent context.
	connectAttemptTimeout = 5 * time.Second
	// maxBackoffShift keeps baseRetryDelay<<i far below the int64 overflow point.
	maxBackoffShift = 30
)

// HealthChecker defines an interface for dependency health reporting.
// Implementations must be safe for concurrent use from multiple goroutines.
type HealthChecker interface {
	// SetStatus updates component status with optional details.
	SetStatus(dependencyName string, status bool, details interface{})
}

// Config holds PostgreSQL client configuration.
type Config struct {
	pgxConfig   *pgxpool.Config
	dsn         string
	maxAttempts int
	maxDelay    time.Duration
	health      struct {
		checker          HealthChecker
		intervalCheck    time.Duration
		failureThreshold int
		name             string
	}
	poolSettings struct {
		maxConns        int32
		minConns        int32
		maxConnLifetime time.Duration
		// maxConnLifetimeJitter < 0 means unset: defaults to 10% of the lifetime
		// so connections opened together are not all recycled at once.
		maxConnLifetimeJitter time.Duration
	}
	poolName      string
	tracing       bool
	traceSQL      bool
	tracers       []pgx.QueryTracer
	txMaxAttempts int
	registerer    prometheus.Registerer
	maxLag        time.Duration
}

// Option configures the PostgreSQL client.
type Option func(*Config)

// WithMaxAttempts sets the maximum number of retry attempts for database operations.
func WithMaxAttempts(attempts int) Option {
	return func(cfg *Config) {
		cfg.maxAttempts = attempts
	}
}

// WithMaxDelay sets the maximum delay between retry attempts.
func WithMaxDelay(delay time.Duration) Option {
	return func(cfg *Config) {
		cfg.maxDelay = delay
	}
}

// WithQueryExecMode sets the default pgx query execution mode.
// pgx defaults to QueryExecModeCacheStatement (prepared statements cached per
// connection), which is the fastest mode against a direct PostgreSQL
// connection. Use QueryExecModeCacheDescribe or QueryExecModeExec behind
// PgBouncer in transaction pooling mode.
func WithQueryExecMode(mode pgx.QueryExecMode) Option {
	return func(cfg *Config) {
		cfg.pgxConfig.ConnConfig.DefaultQueryExecMode = mode
	}
}

// WithBinaryExecMode switches to QueryExecModeCacheDescribe when binary is true
// and does nothing otherwise.
//
// Deprecated: the name is misleading — every cached pgx mode already uses the
// binary protocol, and CacheDescribe is a PgBouncer-compatibility mode that is
// usually slower than the pgx default. Use WithQueryExecMode.
func WithBinaryExecMode(binary bool) Option {
	return func(cfg *Config) {
		if binary {
			cfg.pgxConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
		}
	}
}

// WithHealthChecker configures health monitoring for the database connection.
func WithHealthChecker(name string, hc HealthChecker) Option {
	return func(cfg *Config) {
		cfg.health.checker = hc
		cfg.health.name = name
	}
}

// WithIntervalCheck sets the health check interval for background monitoring.
func WithIntervalCheck(interval time.Duration) Option {
	return func(cfg *Config) {
		cfg.health.intervalCheck = interval
	}
}

// WithConnectionPool configures connection pool parameters.
func WithConnectionPool(maxConns, minConns int32, maxLifetime time.Duration) Option {
	return func(cfg *Config) {
		cfg.poolSettings.maxConns = maxConns
		cfg.poolSettings.minConns = minConns
		cfg.poolSettings.maxConnLifetime = maxLifetime
	}
}

// NewConfig parses DSN and applies options.
func NewConfig(dsn string, options ...Option) (*Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("config parse error: %w", err)
	}

	config := &Config{
		pgxConfig:     cfg,
		dsn:           dsn,
		maxAttempts:   defaultMaxAttempts,
		maxDelay:      defaultMaxRetryDelay,
		poolName:      defaultPoolName,
		tracing:       true,
		traceSQL:      true,
		txMaxAttempts: defaultTxMaxAttempts,
		registerer:    prometheus.DefaultRegisterer,
	}

	// Precedence: package defaults < pool_* parameters in the DSN < options.
	config.poolSettings.maxConns = defaultMaxConnections
	config.poolSettings.minConns = defaultMinConnections
	config.poolSettings.maxConnLifetime = defaultConnLifetime
	config.poolSettings.maxConnLifetimeJitter = -1
	if dsnHasParam(dsn, "pool_max_conn_lifetime_jitter") {
		config.poolSettings.maxConnLifetimeJitter = cfg.MaxConnLifetimeJitter
	}
	if dsnHasParam(dsn, "pool_max_conns") {
		config.poolSettings.maxConns = cfg.MaxConns
	}
	if dsnHasParam(dsn, "pool_min_conns") {
		config.poolSettings.minConns = cfg.MinConns
	}
	if dsnHasParam(dsn, "pool_max_conn_lifetime") {
		config.poolSettings.maxConnLifetime = cfg.MaxConnLifetime
	}

	for _, o := range options {
		o(config)
	}

	config.applyDefaults()

	// Apply pool settings to pgx config
	config.pgxConfig.MaxConns = config.poolSettings.maxConns
	config.pgxConfig.MinConns = config.poolSettings.minConns
	config.pgxConfig.MaxConnLifetime = config.poolSettings.maxConnLifetime
	config.pgxConfig.MaxConnLifetimeJitter = config.poolSettings.maxConnLifetimeJitter

	return config, nil
}

func (c *Config) applyDefaults() {
	if c == nil {
		return
	}
	if c.maxAttempts <= 0 {
		c.maxAttempts = defaultMaxAttempts
	}
	if c.maxDelay <= 0 {
		c.maxDelay = defaultMaxRetryDelay
	}
	if c.poolSettings.maxConns <= 0 {
		c.poolSettings.maxConns = defaultMaxConnections
	}
	if c.poolSettings.minConns < 0 {
		c.poolSettings.minConns = defaultMinConnections
	}
	if c.poolSettings.minConns > c.poolSettings.maxConns {
		c.poolSettings.minConns = c.poolSettings.maxConns
	}
	if c.poolSettings.maxConnLifetime <= 0 {
		c.poolSettings.maxConnLifetime = defaultConnLifetime
	}
	if c.poolSettings.maxConnLifetimeJitter < 0 {
		c.poolSettings.maxConnLifetimeJitter = c.poolSettings.maxConnLifetime / 10
	}
	if c.poolName == "" {
		c.poolName = defaultPoolName
	}
	if c.txMaxAttempts <= 0 {
		c.txMaxAttempts = defaultTxMaxAttempts
	}
	if c.health.checker != nil && c.health.name == "" {
		c.health.name = defaultHealthCheckName
	}
	if c.health.intervalCheck <= 0 {
		c.health.intervalCheck = defaultIntervalCheck
	}
	if c.health.failureThreshold <= 0 {
		c.health.failureThreshold = defaultFailureThreshold
	}
}

// Validate checks whether the configuration can be used to create a client.
func (c *Config) Validate() error {
	if c == nil {
		return ErrNilConfig
	}
	if c.dsn == "" {
		return ErrEmptyDSN
	}
	if c.pgxConfig == nil {
		return ErrInvalidConfig
	}
	return nil
}

// isURLDSN reports whether dsn uses the URL form rather than keyword/value.
func isURLDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}

// dsnHasParam reports whether the URL or keyword/value DSN sets the parameter.
func dsnHasParam(dsn, key string) bool {
	if isURLDSN(dsn) {
		u, err := url.Parse(dsn)
		return err == nil && u.Query().Has(key)
	}
	return regexp.MustCompile(`(?:^|\s)` + regexp.QuoteMeta(key) + `\s*=`).MatchString(dsn)
}

var keywordPasswordRe = regexp.MustCompile(`(password\s*=\s*)('(?:[^'\\]|\\.)*'|\S+)`)

// RedactedDSN returns the configured connection string with the password
// masked, safe for logs.
func (c *Config) RedactedDSN() string {
	if c == nil {
		return ""
	}
	if !isURLDSN(c.dsn) {
		return keywordPasswordRe.ReplaceAllString(c.dsn, "${1}xxxxx")
	}
	u, err := url.Parse(c.dsn)
	if err != nil {
		return "<unparseable dsn>"
	}
	if q := u.Query(); q.Has("password") {
		q.Set("password", "xxxxx")
		u.RawQuery = q.Encode()
	}
	return u.Redacted()
}

// HealthCheckInterval returns the configured background health-check interval.
func (c *Config) HealthCheckInterval() time.Duration {
	if c == nil || c.health.intervalCheck <= 0 {
		return defaultIntervalCheck
	}
	return c.health.intervalCheck
}

// Client represents a thread-safe PostgreSQL connection pool.
type Client struct {
	*pgxpool.Pool
	name          string
	healthChecker HealthChecker
	healthName    string
	labels        availabilityLabels
	healthy       atomic.Bool // health as seen by the background monitor
	txMaxAttempts int
	cancelHealth  context.CancelFunc // For graceful shutdown of health checks
	healthDone    chan struct{}      // Closed when the monitor goroutine exits
	closeOnce     sync.Once
	metrics       *metricSet
	maxLag        time.Duration // 0 disables replication lag checks
	lag           atomic.Int64  // last measured replication lag, ns
}

// errPoolSaturated reports a health probe that timed out waiting for a
// connection while every connection was in use: the pool is busy, which says
// nothing about whether the database is up.
var errPoolSaturated = errors.New("pool saturated")

// replicationLagSQL returns the replay delay in seconds: 0 when everything
// received has been replayed, and also on a primary, where both functions
// return NULL.
const replicationLagSQL = `SELECT COALESCE(CASE
	WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
	ELSE EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())
END, 0)`

// probe pings the database through a pooled connection. When lag is non-nil
// it measures replication lag instead of a bare ping. It talks to pgconn
// directly so health checks stay out of query traces and latency metrics.
func probe(ctx context.Context, pool *pgxpool.Pool, lag *time.Duration) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		if ctx.Err() != nil {
			if st := pool.Stat(); st.AcquiredConns() >= st.MaxConns() {
				return errPoolSaturated
			}
		}
		return err
	}
	defer conn.Release()
	if lag == nil {
		return conn.Conn().PgConn().Ping(ctx)
	}
	d, err := replicationLag(ctx, conn.Conn().PgConn())
	if err != nil {
		return err
	}
	*lag = d
	return nil
}

func replicationLag(ctx context.Context, pgConn *pgconn.PgConn) (time.Duration, error) {
	results, err := pgConn.Exec(ctx, replicationLagSQL).ReadAll()
	if err != nil {
		return 0, err
	}
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 1 {
		return 0, errors.New("replication lag query: unexpected result shape")
	}
	secs, err := strconv.ParseFloat(string(results[0].Rows[0][0]), 64)
	if err != nil {
		return 0, fmt.Errorf("replication lag query: %w", err)
	}
	return time.Duration(secs * float64(time.Second)), nil
}

// NewClient creates a new PostgreSQL connection pool with highload optimizations,
// retry logic and health monitoring.
func NewClient(ctx context.Context, cfg *Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
	m := metricsFor(cfg.registerer)

	// Instrument a copy so cfg stays reusable for further clients.
	pgxCfg := cfg.pgxConfig.Copy()
	pgxCfg.ConnConfig.Tracer = buildTracer(cfg, m, pgxCfg.ConnConfig.Tracer)

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("connection pool creation failed: %w", err)
	}

	// Ping with retries; each attempt gets its own deadline.
	attemptTimeout := connectAttemptTimeout
	if ct := cfg.pgxConfig.ConnConfig.ConnectTimeout; ct > 0 {
		attemptTimeout = ct + pingTimeout
	}
	pingErr := DoWithAttempts(ctx, r, func() error {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		defer cancel()
		return probe(attemptCtx, pool, nil)
	}, cfg.maxAttempts, cfg.maxDelay)
	if pingErr != nil {
		pool.Close()
		return nil, fmt.Errorf("connection failed: %w", pingErr)
	}

	// Create context for health check shutdown
	healthCtx, cancel := context.WithCancel(context.Background())

	client := &Client{
		Pool:          pool,
		name:          cfg.poolName,
		txMaxAttempts: cfg.txMaxAttempts,
		healthChecker: cfg.health.checker,
		healthName:    cfg.health.name,
		labels:        labelsFromConfig(cfg),
		cancelHealth:  cancel,
		healthDone:    make(chan struct{}),
		metrics:       m,
		maxLag:        cfg.maxLag,
	}

	client.healthy.Store(true)
	m.acquireAvailability(client.labels)
	m.setAvailability(client.labels, true)
	m.pool.add(client)

	go func() {
		defer close(client.healthDone)
		client.monitor(healthCtx, cfg.HealthCheckInterval(), cfg.health.failureThreshold)
	}()

	return client, nil
}

// monitor periodically pings the database and reports the result to both the
// availability gauge and the optional health checker until ctx is cancelled.
// A single goroutine serves both consumers so each tick costs one ping.
//
// The client turns unhealthy only after failureThreshold consecutive failed
// pings, and one success restores it. A saturated pool leaves the status
// unchanged: under load the database is busy, not down, and reporting it
// unhealthy would shift traffic away or get the service restarted exactly when
// it is busiest.
func (c *Client) monitor(ctx context.Context, interval time.Duration, failureThreshold int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-ctx.Done():
			if c.healthChecker != nil {
				c.healthChecker.SetStatus(c.healthName, false, "health checks stopped")
			}
			return
		case <-ticker.C:
			var lag *time.Duration
			if c.maxLag > 0 {
				lag = new(time.Duration)
			}
			pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := probe(pingCtx, c.Pool, lag)
			cancel()
			if err == nil && lag != nil {
				c.lag.Store(int64(*lag))
			}

			var details string
			switch {
			case err == nil:
				failures = 0
				details = "connection healthy"
			case errors.Is(err, errPoolSaturated):
				details = "pool saturated, status unchanged"
			default:
				failures++
				details = fmt.Sprintf("connection error (%d/%d): %v", failures, failureThreshold, err)
			}

			status := c.healthy.Load()
			if err == nil {
				status = true
			} else if failures >= failureThreshold {
				status = false
			}
			c.healthy.Store(status)
			c.metrics.setAvailability(c.labels, status)
			if c.healthChecker != nil {
				c.healthChecker.SetStatus(c.healthName, status, details)
			}
		}
	}
}

// ReplicationLag returns the replay lag measured by the last successful health
// check. It is always 0 unless WithMaxReplicationLag is set.
func (c *Client) ReplicationLag() time.Duration { return time.Duration(c.lag.Load()) }

// withinLag reports whether the client may serve replica reads in a Cluster.
func (c *Client) withinLag() bool {
	return c.maxLag <= 0 || c.ReplicationLag() <= c.maxLag
}

// Name returns the pool name used in metrics and traces (see WithPoolName).
func (c *Client) Name() string { return c.name }

// Healthy reports the client's health as seen by the background monitor (see
// WithFailureThreshold). It is refreshed every health-check interval and turns
// false once Close starts.
func (c *Client) Healthy() bool { return c.healthy.Load() }

// DoWithAttempts implements advanced retry logic with exponential backoff and jitter.
// Returns the last error wrapped so the root cause is not lost.
func DoWithAttempts(
	ctx context.Context,
	r *rand.Rand, // Isolated random generator
	fn func() error,
	maxAttempts int,
	maxDelay time.Duration,
) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if ctx.Err() != nil {
			return contextError(ctx, lastErr)
		}

		// No need to wait after the final attempt.
		if i == maxAttempts-1 {
			break
		}

		// Exponential backoff with jitter; the shift is bounded to avoid overflow.
		backoff := maxDelay
		if i < maxBackoffShift {
			if d := baseRetryDelay << i; d < maxDelay {
				backoff = d
			}
		}
		backoff = addJitter(backoff, r, jitterFactor)
		if backoff > maxDelay {
			backoff = maxDelay
		}

		select {
		case <-ctx.Done():
			return contextError(ctx, lastErr)
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("operation failed after %d attempts: %w", maxAttempts, lastErr)
}

// contextError reports why retrying stopped without losing the failure that
// caused the retries: both match errors.Is.
func contextError(ctx context.Context, lastErr error) error {
	return fmt.Errorf("%w (last error: %w)", ctx.Err(), lastErr)
}

// addJitter applies random jitter to duration to avoid thundering herd.
func addJitter(d time.Duration, r *rand.Rand, factor float64) time.Duration {
	jitter := factor * (r.Float64() - 0.5) // ±factor/2
	return time.Duration(float64(d) * (1 + jitter))
}

// DSNConfig contains parameters for building PostgreSQL connection string.
type DSNConfig struct {
	Username string
	Password string
	Host     string
	Port     string
	Database string
}

// ToPostgresDSN converts DSNConfig to a properly escaped PostgreSQL connection string.
func (c *DSNConfig) ToPostgresDSN() string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   net.JoinHostPort(c.Host, c.Port),
		Path:   "/" + c.Database,
	}
	return u.String()
}

// Close gracefully terminates the connection pool and health checks. It is
// safe to call more than once, e.g. via both Cluster.Close and the client.
func (c *Client) Close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(c.close)
}

func (c *Client) close() {
	// Stop the monitor goroutine first and wait for it to report the final
	// status, so callers observe "unhealthy" as soon as Close returns and no
	// ping races with the pool shutdown below.
	c.healthy.Store(false)
	if c.cancelHealth != nil {
		c.cancelHealth()
	}
	if c.healthDone != nil {
		<-c.healthDone
		// A tick already in flight when Close started may have stored true.
		c.healthy.Store(false)
		c.metrics.releaseAvailability(c.labels)
		c.metrics.pool.remove(c)
	}

	// Close connection pool
	if c.Pool != nil {
		c.Pool.Close()
	}
}
