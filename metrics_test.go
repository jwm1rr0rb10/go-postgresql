package psql

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func defaultMetrics() *metricSet { return metricsFor(prometheus.DefaultRegisterer) }

func availabilityValue(t *testing.T, l availabilityLabels) float64 {
	t.Helper()
	var m dto.Metric
	if err := defaultMetrics().availability.WithLabelValues(l.host, l.database).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

func TestLabelsFromConfig(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want availabilityLabels
	}{
		{
			name: "url",
			dsn:  "postgres://user:pass@db.example.com:5432/myapp?sslmode=disable",
			want: availabilityLabels{host: "db.example.com", database: "myapp"},
		},
		{
			name: "keyword/value",
			dsn:  "host=db.example.com port=5432 user=u password=p dbname=myapp sslmode=disable",
			want: availabilityLabels{host: "db.example.com", database: "myapp"},
		},
		{
			name: "multi-host uses primary",
			dsn:  "postgres://user:pass@db1.example.com:5432,db2.example.com:5432/myapp?sslmode=disable",
			want: availabilityLabels{host: "db1.example.com", database: "myapp"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewConfig(tt.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if got := labelsFromConfig(cfg); got != tt.want {
				t.Fatalf("labels = %+v, want %+v", got, tt.want)
			}
		})
	}

	if got := labelsFromConfig(nil); got != (availabilityLabels{}) {
		t.Fatalf("nil config labels = %+v", got)
	}
}

func TestAvailabilitySharedSeries(t *testing.T) {
	l := availabilityLabels{host: "refcount.test", database: "app"}
	gauge := func() float64 { return availabilityValue(t, l) }

	m := defaultMetrics()
	m.acquireAvailability(l)
	m.acquireAvailability(l)
	m.setAvailability(l, true)

	m.releaseAvailability(l)
	if gauge() != 1 {
		t.Fatal("closing one of two clients must not mark the database unavailable")
	}

	m.releaseAvailability(l)
	if gauge() != 0 {
		t.Fatal("closing the last client must mark the database unavailable")
	}
}

func TestMetricsForCustomRegisterer(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := metricsFor(reg)
	if metricsFor(reg) != m {
		t.Fatal("clients sharing a registerer must share the metric set")
	}
	if m == defaultMetrics() {
		t.Fatal("a custom registerer must get its own metric set")
	}

	m.setAvailability(availabilityLabels{host: "h", database: "d"}, true)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "postgres_availability" {
			found = true
		}
	}
	if !found {
		t.Fatal("availability not registered with the custom registerer")
	}

	// A second wrapper of the same registry reuses the registered collectors.
	wrapped := prometheus.WrapRegistererWith(nil, reg)
	if metricsFor(wrapped).availability != m.availability {
		t.Fatal("already registered collectors must be reused")
	}
}

func TestMetricsForNilRegistererDisablesRegistration(t *testing.T) {
	m := metricsFor(nil)
	m.queryDuration.WithLabelValues("p", "q", "ok").Observe(1) // must not panic
}
