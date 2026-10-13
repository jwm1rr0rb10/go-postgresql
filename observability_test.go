package psql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSQLVerb(t *testing.T) {
	tests := map[string]string{
		"SELECT 1":                             "select",
		"  \n\tinsert into t values ($1)":      "insert",
		"-- comment\nUPDATE t SET x = 1":       "update",
		"/* a */ /* b */ delete from t":        "delete",
		"(SELECT 1) UNION (SELECT 2)":          "select",
		"WITH x AS (SELECT 1) SELECT * FROM x": "with",
		"frobnicate everything":                "other",
		"":                                     "other",
		"-- unterminated comment":              "other",
		"/* unterminated":                      "other",
		"select_but_identifier":                "other",
		"Select id from t /* non-ascii ws */":  "select",
	}
	for sql, want := range tests {
		if got := sqlVerb(sql); got != want {
			t.Errorf("sqlVerb(%q) = %q, want %q", sql, got, want)
		}
	}
}

func TestQueryName(t *testing.T) {
	ctx := context.Background()
	if got := queryName(ctx, "SELECT 1"); got != "select" {
		t.Fatalf("queryName = %q", got)
	}
	if got := queryName(WithQueryName(ctx, "kline.load"), "SELECT 1"); got != "kline.load" {
		t.Fatalf("queryName with explicit name = %q", got)
	}
}

func TestQueryStatus(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{nil, "ok"},
		{pgx.ErrNoRows, "ok"},
		{fmt.Errorf("wrap: %w", context.DeadlineExceeded), "timeout"},
		{context.Canceled, "canceled"},
		{errors.New("boom"), "error"},
	}
	for _, tt := range tests {
		if got := queryStatus(tt.err); got != tt.want {
			t.Errorf("queryStatus(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func histogramCount(t *testing.T, pool, query, status string) uint64 {
	t.Helper()
	var m dto.Metric
	h := queryDuration.WithLabelValues(pool, query, status)
	if err := h.(interface{ Write(*dto.Metric) error }).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

func newTestObserver(pool string, tracing, traceSQL bool) (*observer, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	return &observer{
		pool:     pool,
		tracer:   tp.Tracer("test"),
		tracing:  tracing,
		traceSQL: traceSQL,
		attrs:    []attribute.KeyValue{attribute.String("db.system.name", "postgresql")},
	}, rec
}

func spanAttr(s sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestObserverQuerySpanAndMetric(t *testing.T) {
	o, rec := newTestObserver("obs-test", true, true)
	ctx := WithQueryName(context.Background(), "kline.save")

	before := histogramCount(t, "obs-test", "kline.save", "ok")
	qctx := o.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "INSERT INTO k VALUES ($1)"})
	o.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag("INSERT 0 3")})

	if got := histogramCount(t, "obs-test", "kline.save", "ok"); got != before+1 {
		t.Fatalf("histogram count = %d, want %d", got, before+1)
	}

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	s := spans[0]
	if s.Name() != "postgres kline.save" {
		t.Fatalf("span name = %q", s.Name())
	}
	if v, ok := spanAttr(s, "db.query.text"); !ok || v.AsString() != "INSERT INTO k VALUES ($1)" {
		t.Fatalf("db.query.text = %v, %v", v, ok)
	}
	if v, ok := spanAttr(s, "db.response.returned_rows"); !ok || v.AsInt64() != 3 {
		t.Fatalf("rows attr = %v, %v", v, ok)
	}
}

func TestObserverRecordsPgError(t *testing.T) {
	o, rec := newTestObserver("obs-err", true, false)
	ctx := context.Background()

	qctx := o.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	o.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{
		Err: &PgError{Code: pgerrcode.QueryCanceled, Message: "canceling statement"},
	})

	s := rec.Ended()[0]
	if s.Status().Code != codes.Error {
		t.Fatalf("span status = %v", s.Status())
	}
	if v, ok := spanAttr(s, "db.response.status_code"); !ok || v.AsString() != pgerrcode.QueryCanceled {
		t.Fatalf("status code attr = %v, %v", v, ok)
	}
	if _, ok := spanAttr(s, "db.query.text"); ok {
		t.Fatal("db.query.text must be omitted when traceSQL is off")
	}
	if histogramCount(t, "obs-err", "select", "error") == 0 {
		t.Fatal("expected error sample")
	}
}

func TestObserverTracingDisabledStillRecordsMetrics(t *testing.T) {
	o, rec := newTestObserver("obs-off", false, true)
	ctx := context.Background()

	bctx := o.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{Batch: &pgx.Batch{}})
	o.TraceBatchEnd(bctx, nil, pgx.TraceBatchEndData{})
	cctx := o.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{TableName: pgx.Identifier{"t"}})
	o.TraceCopyFromEnd(cctx, nil, pgx.TraceCopyFromEndData{})

	if len(rec.Ended()) != 0 {
		t.Fatal("no spans expected with tracing disabled")
	}
	if histogramCount(t, "obs-off", "batch", "ok") == 0 || histogramCount(t, "obs-off", "copy", "ok") == 0 {
		t.Fatal("batch and copy latency must be recorded")
	}
}

func TestObserverEndWithoutStartIsNoop(t *testing.T) {
	o, _ := newTestObserver("obs-noop", true, true)
	o.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{})
}

type countingTracer struct{ starts, ends int }

func (c *countingTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.starts++
	return ctx
}

func (c *countingTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) { c.ends++ }

func TestBuildTracerChainsUserTracers(t *testing.T) {
	user := &countingTracer{}
	cfg, err := NewConfig("postgres://u:p@localhost:5432/app", WithQueryTracer(user))
	if err != nil {
		t.Fatal(err)
	}
	tr := buildTracer(cfg, nil)
	ctx := tr.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tr.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	if user.starts != 1 || user.ends != 1 {
		t.Fatalf("user tracer calls = %d/%d", user.starts, user.ends)
	}

	if _, ok := buildTracer(&Config{poolName: "x"}, nil).(*observer); !ok {
		t.Fatal("without extra tracers the observer is used directly")
	}
}
