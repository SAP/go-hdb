package driver

import (
	"errors"
	"testing"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

const (
	fallbackHost = "fallback:30015"
	nodeAHost    = "node-a:30013"
	nodeBHost    = "node-b:30013"
	nodeCHost    = "node-c:30013"
	nodeDHost    = "node-d:30013"
	nodeEHost    = "node-e:30013"
	nodeFHost    = "node-f:30013"
)

// testRoutingNodes implements the RoutingNodes interface. SortedNodeList
// returns the prepared node list unchanged: the caller supplies the list
// already sorted by state identity (SiteVolumeID, SiteType), mirroring the
// sorted topology the protocol delivers. err is returned if set.
type testRoutingNodes struct {
	nodes p.RoutingNodeList
	err   error
}

func (rn testRoutingNodes) SortedNodeList(_ *string) (p.RoutingNodeList, error) {
	if rn.err != nil {
		return nil, rn.err
	}
	return rn.nodes, nil
}

func testPriorities(t *testing.T) {
	r := new(routing)

	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1}, // reachable non-standby
		{Host: nodeBHost, Standby: true, SiteVolumeID: 2},  // reachable standby
		{Host: nodeCHost, Standby: false, SiteVolumeID: 3}, // unreachable non-standby
		{Host: nodeDHost, Standby: true, SiteVolumeID: 4},  // unreachable standby
	}})
	r.setReachable(nodeCHost, false)
	r.setReachable(nodeDHost, false)

	for i, want := range []string{nodeAHost, nodeAHost, nodeAHost} {
		if got := r.pick(fallbackHost); got != want {
			t.Fatalf("pick %d: got %s, expected %s", i, got, want)
		}
	}
}

func testRoundRobin(t *testing.T) {
	r := new(routing)

	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1}, // pickable
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2}, // pickable
		{Host: nodeCHost, Standby: false, SiteVolumeID: 3}, // pickable
		{Host: nodeDHost, Standby: true, SiteVolumeID: 4},  // non-pickable
		{Host: nodeEHost, Standby: false, SiteVolumeID: 5}, // non-pickable
		{Host: nodeFHost, Standby: true, SiteVolumeID: 6},  // non-pickable
	}})
	r.setReachable(nodeEHost, false)
	r.setReachable(nodeFHost, false)

	// two full rounds: every pickable node picked exactly twice, non-pickable never.
	counts := map[string]int{nodeAHost: 0, nodeBHost: 0, nodeCHost: 0}
	for i := range 6 {
		got := r.pick(fallbackHost)
		if _, ok := counts[got]; !ok {
			t.Fatalf("pick %d: got %s, expected one of [%s, %s, %s]", i, got, nodeAHost, nodeBHost, nodeCHost)
		}
		counts[got]++
	}
	for host, count := range counts {
		if count != 2 {
			t.Fatalf("host %s picked %d times, expected 2", host, count)
		}
	}
}

func testStandbyOnPrimaryUnreachable(t *testing.T) {
	r := new(routing)

	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1}, // primary
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2}, // primary
		{Host: nodeCHost, Standby: true, SiteVolumeID: 3},  // reachable standby
	}})

	// primaries are picked as long as they are reachable.
	if got := r.pick(fallbackHost); got != nodeAHost && got != nodeBHost {
		t.Fatalf("got %s, expected a primary [%s, %s]", got, nodeAHost, nodeBHost)
	}

	// primaries unreachable: the reachable standby must take over.
	r.setReachable(nodeAHost, false)
	r.setReachable(nodeBHost, false)
	for i, want := range []string{nodeCHost, nodeCHost, nodeCHost} {
		if got := r.pick(fallbackHost); got != want {
			t.Fatalf("pick %d: got %s, expected %s", i, got, want)
		}
	}

	// standby unreachable too: pick falls back to the unreachable primaries.
	r.setReachable(nodeCHost, false)
	for i := range 2 {
		got := r.pick(fallbackHost)
		if got != nodeAHost && got != nodeBHost {
			t.Fatalf("pick %d: got %s, expected one of [%s, %s]", i, got, nodeAHost, nodeBHost)
		}
	}
}

func testReachableAcrossRefresh(t *testing.T) {
	r := new(routing)

	// initial topology
	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
		{Host: nodeCHost, Standby: false, SiteVolumeID: 3},
	}})

	// node A unreachable
	r.setReachable(nodeAHost, false)
	if got := r.pick(fallbackHost); got == nodeAHost {
		t.Fatalf("pick: got %s, expected a reachable node", got)
	}

	// topology refresh with freshly parsed nodes (same hosts, new objects)
	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
		{Host: nodeCHost, Standby: false, SiteVolumeID: 3},
	}})

	// reachability must survive the refresh
	for i := range 3 {
		if got := r.pick(fallbackHost); got == nodeAHost {
			t.Fatalf("pick %d after refresh: got %s, expected a reachable node", i, got)
		}
	}

	// topology refresh removing node B: the remaining nodes' state must survive
	// the merge, and the dropped record must not linger
	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeCHost, Standby: false, SiteVolumeID: 3},
	}})
	for i := range 3 {
		if got := r.pick(fallbackHost); got != nodeCHost {
			t.Fatalf("pick %d after removal: got %s, expected %s", i, got, nodeCHost)
		}
	}
}

func testReachableAcrossFailover(t *testing.T) {
	r := new(routing)

	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
	}})

	// node A unreachable
	r.setReachable(nodeAHost, false)
	if got := r.pick(fallbackHost); got != nodeBHost {
		t.Fatalf("pick: got %s, expected %s", got, nodeBHost)
	}

	// the node with site volume id 1 moved to a new address: its state must
	// follow the site volume identity, not the address.
	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: "node-a-2:30013", Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
	}})

	for i := range 3 {
		if got := r.pick(fallbackHost); got != nodeBHost {
			t.Fatalf("pick %d after refresh: got %s, expected %s", i, got, nodeBHost)
		}
	}
}

func testDrain(t *testing.T) {
	r := new(routing)

	v := r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
	}})

	// matching-version reply applies on the next pick
	r.updateFromReply(v, testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
	}})
	if got := r.pick(fallbackHost); got != nodeAHost {
		t.Fatalf("pick 1: got %s, expected %s", got, nodeAHost)
	}
	if got := r.pick(fallbackHost); got != nodeBHost {
		t.Fatalf("pick 2: got %s, expected %s (reply topology not applied)", got, nodeBHost)
	}

	// stale-version reply is discarded
	r.updateFromReply(v-1, testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeCHost, Standby: false, SiteVolumeID: 1},
	}})
	if got := r.pick(fallbackHost); got == nodeCHost {
		t.Fatalf("pick: got %s, expected a node from the current topology", got)
	}
}

func testFallbackWhenDisabled(t *testing.T) {
	r := new(routing)

	// routing not yet negotiated
	if got := r.pick(fallbackHost); got != fallbackHost {
		t.Fatalf("pick: got %s, expected fallback %s", got, fallbackHost)
	}

	// routing explicitly disabled (no distribution mode on the connect reply)
	if v := r.updateFromConnect(false, "proxy:30013", "DB", "SID", nil); v != -1 {
		t.Fatalf("updateFromConnect returned %d, expected -1", v)
	}
	if r.enabled.Load() {
		t.Fatal("routing enabled after updateFromConnect(false)")
	}
	if got := r.pick(fallbackHost); got != fallbackHost {
		t.Fatalf("pick: got %s, expected fallback %s", got, fallbackHost)
	}
}

func testFallbackWhenEmpty(t *testing.T) {
	r := new(routing)

	v := r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
	}})
	if !r.enabled.Load() {
		t.Fatal("routing not enabled")
	}

	// a reply with an empty topology empties the queue: pick falls back.
	r.updateFromReply(v, testRoutingNodes{nodes: nil})
	if got := r.pick(fallbackHost); got != fallbackHost {
		t.Fatalf("pick: got %s, expected fallback %s", got, fallbackHost)
	}
}

func testVersionBump(t *testing.T) {
	r := new(routing)

	// first connect establishes the cluster identity
	v1 := r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
	}})
	if v1 != 1 {
		t.Fatalf("version %d, expected 1", v1)
	}

	// a reply for the current version applies
	r.updateFromReply(v1, testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
	}})
	if got := r.pick(fallbackHost); got != nodeAHost {
		t.Fatalf("pick: got %s, expected %s", got, nodeAHost)
	}
	if got := r.pick(fallbackHost); got != nodeBHost {
		t.Fatalf("pick: got %s, expected %s", got, nodeBHost)
	}

	// a cluster identity change bumps the version
	v2 := r.updateFromConnect(true, "proxy:30013", "DB2", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeCHost, Standby: false, SiteVolumeID: 1},
	}})
	if v2 != 2 {
		t.Fatalf("version %d, expected 2", v2)
	}

	// a stale-version reply from the old cluster is dropped
	r.updateFromReply(v1, testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeDHost, Standby: false, SiteVolumeID: 1},
	}})
	if got := r.pick(fallbackHost); got != nodeCHost {
		t.Fatalf("pick: got %s, expected %s (stale reply must be dropped)", got, nodeCHost)
	}
}

func testSetReachableUnknown(t *testing.T) {
	r := new(routing)

	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
	}})

	// unknown host is a no-op
	r.setReachable("nope:30013", false)
	for i := range 4 {
		got := r.pick(fallbackHost)
		if got != nodeAHost && got != nodeBHost {
			t.Fatalf("pick %d: got %s, expected one of [%s, %s]", i, got, nodeAHost, nodeBHost)
		}
	}

	// setReachable while disabled is a no-op
	r2 := new(routing)
	r2.setReachable(nodeAHost, false)
	if r2.enabled.Load() {
		t.Fatal("routing enabled after setReachable on a fresh routing")
	}
}

func testSetReachableAllEntries(t *testing.T) {
	r := new(routing)

	// the server listed the same host twice with different routing attributes
	r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeAHost, Standby: true, SiteVolumeID: 2},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 3},
	}})

	// setReachable must update every entry with the host
	r.setReachable(nodeAHost, false)
	for i := range 3 {
		if got := r.pick(fallbackHost); got != nodeBHost {
			t.Fatalf("pick %d: got %s, expected %s", i, got, nodeBHost)
		}
	}
}

func testErrTopology(t *testing.T) {
	r := new(routing)

	// a broken connect topology disables routing
	if v := r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: nil, err: errors.New("bad topology")}); v != -1 {
		t.Fatalf("updateFromConnect returned %d, expected -1", v)
	}
	if r.enabled.Load() {
		t.Fatal("routing enabled despite topology error")
	}

	// a broken reply topology leaves the current topology untouched
	v := r.updateFromConnect(true, "proxy:30013", "DB", "SID", testRoutingNodes{nodes: []*p.RoutingNode{
		{Host: nodeAHost, Standby: false, SiteVolumeID: 1},
		{Host: nodeBHost, Standby: false, SiteVolumeID: 2},
	}})
	r.updateFromReply(v, testRoutingNodes{nodes: nil, err: errors.New("bad topology")})
	for i := range 4 {
		got := r.pick(fallbackHost)
		if got != nodeAHost && got != nodeBHost {
			t.Fatalf("pick %d: got %s, expected a node from the current topology", i, got)
		}
	}
}

func TestRouting(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"priorities", testPriorities},
		{"roundRobin", testRoundRobin},
		{"standbyOnPrimaryUnreachable", testStandbyOnPrimaryUnreachable},
		{"reachableAcrossRefresh", testReachableAcrossRefresh},
		{"reachableAcrossFailover", testReachableAcrossFailover},
		{"drain", testDrain},
		{"fallbackWhenDisabled", testFallbackWhenDisabled},
		{"fallbackWhenEmpty", testFallbackWhenEmpty},
		{"versionBump", testVersionBump},
		{"setReachableUnknown", testSetReachableUnknown},
		{"setReachableAllEntries", testSetReachableAllEntries},
		{"errTopology", testErrTopology},
	}

	for _, test := range tests {
		func(name string, fct func(t *testing.T)) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				fct(t)
			})
		}(test.name, test.fct)
	}
}
