package psql

import (
	"errors"
	"testing"
)

func healthyClient(name string, healthy bool) *Client {
	c := &Client{name: name}
	c.healthy.Store(healthy)
	return c
}

func TestNewClusterRequiresPrimary(t *testing.T) {
	if _, err := NewCluster(nil); !errors.Is(err, ErrNilPrimary) {
		t.Fatalf("err = %v", err)
	}
}

func TestClusterReplicaRouting(t *testing.T) {
	primary := healthyClient("primary", true)

	c, err := NewCluster(primary)
	if err != nil {
		t.Fatal(err)
	}
	if c.Replica() != primary {
		t.Fatal("without replicas reads go to the primary")
	}

	r1, r2, r3 := healthyClient("r1", true), healthyClient("r2", false), healthyClient("r3", true)
	c, err = NewCluster(primary, r1, nil, r2, r3)
	if err != nil {
		t.Fatal(err)
	}
	if c.Primary() != primary {
		t.Fatal("Primary() mismatch")
	}

	seen := map[string]int{}
	for range 100 {
		seen[c.Replica().Name()]++
	}
	if seen["r2"] != 0 || seen["primary"] != 0 {
		t.Fatalf("unhealthy replica or primary used: %v", seen)
	}
	if seen["r1"] < 30 || seen["r3"] < 30 {
		t.Fatalf("round-robin is skewed: %v", seen)
	}

	r1.healthy.Store(false)
	r3.healthy.Store(false)
	if c.Replica() != primary {
		t.Fatal("must fall back to the primary when no replica is healthy")
	}
}

func TestClusterCloseNil(t *testing.T) {
	var c *Cluster
	c.Close()
}
