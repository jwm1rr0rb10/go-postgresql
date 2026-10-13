package psql

import (
	"sync"

	"github.com/jwm1rr0rb10/go-metrics"
)

var (
	postgresAvailability = metrics.NewGaugeVec(
		metrics.GaugeOpts{
			Name: "postgres_availability",
			Help: "PostgreSQL connection status (1=healthy, 0=unhealthy)",
		},
		[]string{"host", "database"},
	)

	// availabilityRefs counts live clients per label set. Clients pointing at
	// the same database share one series, so only the last one to close may
	// mark it unavailable.
	availabilityMu   sync.Mutex
	availabilityRefs = map[availabilityLabels]int{}
)

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

func setAvailability(l availabilityLabels, available bool) {
	v := 0.0
	if available {
		v = 1
	}
	postgresAvailability.WithLabelValues(l.host, l.database).Set(v)
}

func acquireAvailability(l availabilityLabels) {
	availabilityMu.Lock()
	defer availabilityMu.Unlock()
	availabilityRefs[l]++
}

// releaseAvailability drops a client's reference and marks the series
// unavailable once no client for these labels remains.
func releaseAvailability(l availabilityLabels) {
	availabilityMu.Lock()
	defer availabilityMu.Unlock()
	availabilityRefs[l]--
	if availabilityRefs[l] > 0 {
		return
	}
	delete(availabilityRefs, l)
	setAvailability(l, false)
}
