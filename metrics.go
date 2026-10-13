package psql

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// metricSet holds the collectors registered with one prometheus.Registerer.
// Clients sharing a registerer share the set, so registration happens once.
type metricSet struct {
	availability  *prometheus.GaugeVec
	queryDuration *prometheus.HistogramVec
	pool          *poolStatsCollector

	// availabilityRefs counts live clients per label set. Clients pointing at
	// the same database share one series, so only the last one to close may
	// mark it unavailable.
	availabilityMu   sync.Mutex
	availabilityRefs map[availabilityLabels]int
}

var (
	metricSetsMu sync.Mutex
	metricSets   = map[prometheus.Registerer]*metricSet{}
)

// metricsFor returns the metric set registered with reg, creating and
// registering it on first use. A nil reg yields collectors that are never
// registered, i.e. metrics are disabled.
func metricsFor(reg prometheus.Registerer) *metricSet {
	metricSetsMu.Lock()
	defer metricSetsMu.Unlock()
	if m, ok := metricSets[reg]; ok {
		return m
	}

	m := &metricSet{
		availability: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "postgres_availability",
			Help: "PostgreSQL connection status (1=healthy, 0=unhealthy)",
		}, []string{"host", "database"}),
		queryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "postgres_query_duration_seconds",
			Help:    "PostgreSQL query latency by pool, query name and outcome.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"pool", "query", "status"}),
		pool:             &poolStatsCollector{clients: map[*Client]struct{}{}},
		availabilityRefs: map[availabilityLabels]int{},
	}
	if reg != nil {
		m.availability = register(reg, m.availability)
		m.queryDuration = register(reg, m.queryDuration)
		m.pool = register(reg, m.pool)
	}
	metricSets[reg] = m
	return m
}

// register registers c with reg. When an identical collector is already
// registered (e.g. through another wrapper of the same registry) it is reused
// so every client reports into the series that is actually scraped.
func register[C prometheus.Collector](reg prometheus.Registerer, c C) C {
	err := reg.Register(c)
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(C); ok {
			return existing
		}
	}
	return c
}

type availabilityLabels struct {
	host     string
	database string
}

// labelsFromConfig takes labels from the parsed pgx config, which handles both
// URL and keyword/value DSNs. For multi-host DSNs it is the primary host.
func labelsFromConfig(cfg *Config) availabilityLabels {
	if cfg == nil || cfg.pgxConfig == nil || cfg.pgxConfig.ConnConfig == nil {
		return availabilityLabels{}
	}
	cc := cfg.pgxConfig.ConnConfig
	return availabilityLabels{host: cc.Host, database: cc.Database}
}

func (m *metricSet) setAvailability(l availabilityLabels, available bool) {
	v := 0.0
	if available {
		v = 1
	}
	m.availability.WithLabelValues(l.host, l.database).Set(v)
}

func (m *metricSet) acquireAvailability(l availabilityLabels) {
	m.availabilityMu.Lock()
	defer m.availabilityMu.Unlock()
	m.availabilityRefs[l]++
}

// releaseAvailability drops a client's reference and marks the series
// unavailable once no client for these labels remains.
func (m *metricSet) releaseAvailability(l availabilityLabels) {
	m.availabilityMu.Lock()
	defer m.availabilityMu.Unlock()
	m.availabilityRefs[l]--
	if m.availabilityRefs[l] > 0 {
		return
	}
	delete(m.availabilityRefs, l)
	m.setAvailability(l, false)
}
