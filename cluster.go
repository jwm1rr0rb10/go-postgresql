package psql

import (
	"sync/atomic"
)

// Cluster routes work between a primary and read replicas.
//
// Replica() round-robins over healthy replicas and falls back to the primary
// when none is healthy. A replica leaves rotation after WithFailureThreshold
// consecutive failed pings, so it can stay in rotation for up to
// threshold × WithIntervalCheck after it fails. A replica whose pool is merely
// saturated stays in rotation.
//
// Replicas created with WithMaxReplicationLag also leave rotation while their
// measured lag exceeds the limit. Without it lag is not tracked, and either
// way read-your-writes paths must use Primary().
type Cluster struct {
	primary  *Client
	replicas []*Client
	next     atomic.Uint64
}

// NewCluster groups already connected clients. Give each a distinct
// WithPoolName ("primary", "replica-1", ...) to tell them apart in metrics.
func NewCluster(primary *Client, replicas ...*Client) (*Cluster, error) {
	if primary == nil {
		return nil, ErrNilPrimary
	}
	rs := make([]*Client, 0, len(replicas))
	for _, r := range replicas {
		if r != nil {
			rs = append(rs, r)
		}
	}
	return &Cluster{primary: primary, replicas: rs}, nil
}

// Primary returns the read-write client.
func (c *Cluster) Primary() *Client {
	return c.primary
}

// Replica returns a healthy replica for read-only queries, or the primary
// when no replica is configured or healthy.
func (c *Cluster) Replica() *Client {
	n := uint64(len(c.replicas))
	if n == 0 {
		return c.primary
	}
	start := c.next.Add(1)
	for i := range n {
		if r := c.replicas[(start+i)%n]; r.Healthy() && r.withinLag() {
			return r
		}
	}
	return c.primary
}

// Close closes all replicas, then the primary.
func (c *Cluster) Close() {
	if c == nil {
		return
	}
	for _, r := range c.replicas {
		r.Close()
	}
	c.primary.Close()
}
