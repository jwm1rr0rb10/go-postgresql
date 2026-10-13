package psql

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/jwm1rr0rb10/go-errors"
	"github.com/jwm1rr0rb10/go-metrics"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "kalipso/app/pkg/golang/postgresql"

var queryDuration = metrics.NewHistogramVec(
	metrics.HistogramOpts{
		Name:    "postgres_query_duration_seconds",
		Help:    "PostgreSQL query latency by pool, query name and outcome.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	},
	[]string{"pool", "query", "status"},
)

type queryNameKey struct{}

// WithQueryName labels queries run with ctx in metrics and spans, e.g.
// "kline.save_batch". Without it the label is the SQL verb ("select",
// "insert", ...). Use a fixed set of names: every distinct name is a new
// metric series.
func WithQueryName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, queryNameKey{}, name)
}

func queryName(ctx context.Context, sql string) string {
	if name, ok := ctx.Value(queryNameKey{}).(string); ok && name != "" {
		return name
	}
	return sqlVerb(sql)
}

// knownVerbs bounds the label cardinality when no explicit name is given.
var knownVerbs = map[string]struct{}{
	"select": {}, "insert": {}, "update": {}, "delete": {}, "with": {},
	"merge": {}, "copy": {}, "call": {}, "begin": {}, "commit": {},
	"rollback": {}, "savepoint": {}, "release": {}, "set": {}, "show": {},
	"lock": {}, "truncate": {}, "values": {}, "explain": {}, "analyze": {},
	"vacuum": {}, "create": {}, "alter": {}, "drop": {}, "listen": {},
	"notify": {}, "unlisten": {}, "refresh": {},
}

// sqlVerb returns the lower-cased first keyword of sql, skipping whitespace
// and comments, or "other" when it is not a known statement keyword.
func sqlVerb(sql string) string {
	i := 0
	for i < len(sql) {
		switch {
		case sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r' || sql[i] == '(':
			i++
		case strings.HasPrefix(sql[i:], "--"):
			nl := strings.IndexByte(sql[i:], '\n')
			if nl < 0 {
				return "other"
			}
			i += nl + 1
		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return "other"
			}
			i += end + 4
		default:
			j := i
			for j < len(sql) && isWordByte(sql[j]) {
				j++
			}
			verb := strings.ToLower(sql[i:j])
			if _, ok := knownVerbs[verb]; ok {
				return verb
			}
			return "other"
		}
	}
	return "other"
}

func isWordByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func queryStatus(err error) string {
	switch {
	case err == nil, errors.Is(err, pgx.ErrNoRows):
		return "ok"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "error"
	}
}

// observer is a pgx tracer that records query latency and, optionally, spans.
type observer struct {
	pool     string
	tracer   trace.Tracer
	tracing  bool
	traceSQL bool
	attrs    []attribute.KeyValue
}

type observerStateKey struct{}

type observerState struct {
	name  string
	start time.Time
	span  trace.Span
}

// buildTracer combines the built-in observer with user tracers and any tracer
// already set on the pgx config.
func buildTracer(cfg *Config, existing pgx.QueryTracer) pgx.QueryTracer {
	l := labelsFromConfig(cfg)
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "postgresql"),
		attribute.String("db.namespace", l.database),
		attribute.String("server.address", l.host),
		attribute.String("db.client.connection.pool.name", cfg.poolName),
	}
	if cfg.pgxConfig != nil && cfg.pgxConfig.ConnConfig != nil && cfg.pgxConfig.ConnConfig.Port != 0 {
		attrs = append(attrs, attribute.Int("server.port", int(cfg.pgxConfig.ConnConfig.Port)))
	}

	tracers := []pgx.QueryTracer{&observer{
		pool:     cfg.poolName,
		tracer:   otel.Tracer(instrumentationName),
		tracing:  cfg.tracing,
		traceSQL: cfg.traceSQL,
		attrs:    attrs,
	}}
	if existing != nil {
		tracers = append(tracers, existing)
	}
	tracers = append(tracers, cfg.tracers...)
	if len(tracers) == 1 {
		return tracers[0]
	}
	return multitracer.New(tracers...)
}

func (o *observer) start(ctx context.Context, name, sql string, extra ...attribute.KeyValue) context.Context {
	st := &observerState{name: name, start: time.Now()}
	if o.tracing {
		attrs := make([]attribute.KeyValue, 0, len(o.attrs)+len(extra)+2)
		attrs = append(attrs, o.attrs...)
		attrs = append(attrs, attribute.String("db.operation.name", name))
		if o.traceSQL && sql != "" {
			attrs = append(attrs, attribute.String("db.query.text", sql))
		}
		attrs = append(attrs, extra...)
		ctx, st.span = o.tracer.Start(ctx, "postgres "+name,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attrs...),
		)
	}
	return context.WithValue(ctx, observerStateKey{}, st)
}

func (o *observer) end(ctx context.Context, err error, rows int64) {
	st, ok := ctx.Value(observerStateKey{}).(*observerState)
	if !ok {
		return
	}
	status := queryStatus(err)
	queryDuration.WithLabelValues(o.pool, st.name, status).Observe(time.Since(st.start).Seconds())

	if st.span == nil {
		return
	}
	if rows >= 0 {
		st.span.SetAttributes(attribute.Int64("db.response.returned_rows", rows))
	}
	if status != "ok" {
		st.span.RecordError(err)
		st.span.SetStatus(codes.Error, status)
		var pgErr *PgError
		if errors.As(err, &pgErr) {
			st.span.SetAttributes(attribute.String("db.response.status_code", pgErr.Code))
		}
	}
	st.span.End()
}

func (o *observer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return o.start(ctx, queryName(ctx, data.SQL), data.SQL)
}

func (o *observer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	o.end(ctx, data.Err, data.CommandTag.RowsAffected())
}

func (o *observer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	name := "batch"
	if n, ok := ctx.Value(queryNameKey{}).(string); ok && n != "" {
		name = n
	}
	size := 0
	if data.Batch != nil {
		size = data.Batch.Len()
	}
	return o.start(ctx, name, "", attribute.Int("db.operation.batch.size", size))
}

func (o *observer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	st, ok := ctx.Value(observerStateKey{}).(*observerState)
	if !ok || st.span == nil || data.Err == nil {
		return
	}
	st.span.AddEvent("batch query failed", trace.WithAttributes(
		attribute.String("db.operation.name", sqlVerb(data.SQL)),
		attribute.String("error.message", data.Err.Error()),
	))
}

func (o *observer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	o.end(ctx, data.Err, -1)
}

func (o *observer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	name := "copy"
	if n, ok := ctx.Value(queryNameKey{}).(string); ok && n != "" {
		name = n
	}
	return o.start(ctx, name, "", attribute.String("db.collection.name", data.TableName.Sanitize()))
}

func (o *observer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	o.end(ctx, data.Err, data.CommandTag.RowsAffected())
}

// poolStatsCollector exports pgxpool.Stat for every live client. Clients with
// identical labels are summed, so duplicate names never break a scrape.
type poolStatsCollector struct {
	mu      sync.Mutex
	clients map[*Client]struct{}
}

var (
	poolStats         = &poolStatsCollector{clients: map[*Client]struct{}{}}
	poolStatsRegister sync.Once

	poolLabels = []string{"pool", "host", "database"}

	descConns = prometheus.NewDesc("postgres_pool_connections",
		"Pool connections by state.", append(poolLabels, "state"), nil)
	descMaxConns = prometheus.NewDesc("postgres_pool_max_connections",
		"Configured maximum pool size.", poolLabels, nil)
	descAcquires = prometheus.NewDesc("postgres_pool_acquires_total",
		"Successful connection acquires.", poolLabels, nil)
	descAcquireSeconds = prometheus.NewDesc("postgres_pool_acquire_duration_seconds_total",
		"Total time spent acquiring connections.", poolLabels, nil)
	descEmptyAcquires = prometheus.NewDesc("postgres_pool_empty_acquires_total",
		"Acquires that had to wait because the pool had no idle connection.", poolLabels, nil)
	descEmptyAcquireWait = prometheus.NewDesc("postgres_pool_empty_acquire_wait_seconds_total",
		"Total time acquires waited for a connection on an empty pool.", poolLabels, nil)
	descCanceledAcquires = prometheus.NewDesc("postgres_pool_canceled_acquires_total",
		"Acquires canceled by their context.", poolLabels, nil)
	descNewConns = prometheus.NewDesc("postgres_pool_new_connections_total",
		"Connections opened.", poolLabels, nil)
	descDestroyed = prometheus.NewDesc("postgres_pool_destroyed_connections_total",
		"Connections closed by the pool, by reason.", append(poolLabels, "reason"), nil)
)

func registerPoolStats(c *Client) {
	// Registered lazily so a metrics.SetRegisterer call during startup is honored.
	poolStatsRegister.Do(func() {
		_ = metrics.Registerer.Register(poolStats)
	})
	poolStats.mu.Lock()
	defer poolStats.mu.Unlock()
	poolStats.clients[c] = struct{}{}
}

func unregisterPoolStats(c *Client) {
	poolStats.mu.Lock()
	defer poolStats.mu.Unlock()
	delete(poolStats.clients, c)
}

type poolStatsKey struct{ pool, host, database string }

type poolStatsSum struct {
	acquired, idle, constructing, max     int64
	acquires, empty, canceled, newConns   int64
	lifetimeDestroyed, idleDestroyed      int64
	acquireDuration, emptyAcquireDuration time.Duration
}

func (p *poolStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descConns, descMaxConns, descAcquires, descAcquireSeconds, descEmptyAcquires,
		descEmptyAcquireWait, descCanceledAcquires, descNewConns, descDestroyed,
	} {
		ch <- d
	}
}

func (p *poolStatsCollector) Collect(ch chan<- prometheus.Metric) {
	sums := map[poolStatsKey]*poolStatsSum{}
	p.mu.Lock()
	for c := range p.clients {
		k := poolStatsKey{pool: c.name, host: c.labels.host, database: c.labels.database}
		s := sums[k]
		if s == nil {
			s = &poolStatsSum{}
			sums[k] = s
		}
		st := c.Stat()
		s.acquired += int64(st.AcquiredConns())
		s.idle += int64(st.IdleConns())
		s.constructing += int64(st.ConstructingConns())
		s.max += int64(st.MaxConns())
		s.acquires += st.AcquireCount()
		s.empty += st.EmptyAcquireCount()
		s.canceled += st.CanceledAcquireCount()
		s.newConns += st.NewConnsCount()
		s.lifetimeDestroyed += st.MaxLifetimeDestroyCount()
		s.idleDestroyed += st.MaxIdleDestroyCount()
		s.acquireDuration += st.AcquireDuration()
		s.emptyAcquireDuration += st.EmptyAcquireWaitTime()
	}
	p.mu.Unlock()

	for k, s := range sums {
		lv := []string{k.pool, k.host, k.database}
		gauge := func(d *prometheus.Desc, v float64, extra ...string) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, append(lv, extra...)...)
		}
		counter := func(d *prometheus.Desc, v float64, extra ...string) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, append(lv, extra...)...)
		}
		gauge(descConns, float64(s.acquired), "acquired")
		gauge(descConns, float64(s.idle), "idle")
		gauge(descConns, float64(s.constructing), "constructing")
		gauge(descMaxConns, float64(s.max))
		counter(descAcquires, float64(s.acquires))
		counter(descAcquireSeconds, s.acquireDuration.Seconds())
		counter(descEmptyAcquires, float64(s.empty))
		counter(descEmptyAcquireWait, s.emptyAcquireDuration.Seconds())
		counter(descCanceledAcquires, float64(s.canceled))
		counter(descNewConns, float64(s.newConns))
		counter(descDestroyed, float64(s.lifetimeDestroyed), "max_lifetime")
		counter(descDestroyed, float64(s.idleDestroyed), "max_idle")
	}
}
