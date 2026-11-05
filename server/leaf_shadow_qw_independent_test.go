// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Independent repro for ticket 0007: the stream-import shadow of a leaf queue
// subscription announces the hard-coded delta 1 to routes/leaves even though it
// copies the leaf's aggregated queue weight (qw). The importing account's
// weight is right, the exporting account's is always 1, and later weight
// updates only touch the primary leaf sub.

func leafShadowQWHubConf() []byte {
	return []byte(`
		port: -1
		cluster {
			name: "C"
			listen: "127.0.0.1:-1"
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				users = [{user: "leaf", password: "pwd", account: "IMP"}]
			}
		}
		accounts {
			EXP {
				users = [{user: "exp", password: "pwd"}]
				exports = [{stream: "foo.>"}]
			}
			IMP {
				users = [{user: "imp", password: "pwd"}]
				imports = [{stream: {account: EXP, subject: "foo.>"}}]
			}
		}
	`)
}

func leafShadowQWSpokeConf(hubOpts *Options) []byte {
	return []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			reconnect: "50ms"
			remotes = [{url: "nats-leaf://leaf:pwd@%s:%d", account: "IMP"}]
		}
		accounts {
			IMP { users = [{user: "imp", password: "pwd"}] }
		}
	`, hubOpts.LeafNode.Host, hubOpts.LeafNode.Port))
}

// subsMatching returns the local and route subs in acc matching subject,
// including queue subs.
func subsMatching(acc *Account, subject string) []*subscription {
	acc.mu.RLock()
	sr := acc.sl.Match(subject)
	var out []*subscription
	out = append(out, sr.psubs...)
	for _, qs := range sr.qsubs {
		out = append(out, qs...)
	}
	acc.mu.RUnlock()
	return out
}

// shadowFor returns the stream-import shadow sub in acc matching subject.
func shadowFor(acc *Account, subject string) *subscription {
	for _, sub := range subsMatching(acc, subject) {
		if sub.im != nil {
			return sub
		}
	}
	return nil
}

func leafShadowWeights(t *testing.T, impAcc, expAcc *Account) (impRM, expRM int64, impLWS, expLWS int32, qw int32) {
	t.Helper()
	key := "N foo.bar q"
	impAcc.mu.RLock()
	impRM, impLWS = impAcc.rm[key], impAcc.lws[key]
	impAcc.mu.RUnlock()
	expAcc.mu.RLock()
	expRM, expLWS = expAcc.rm[key], expAcc.lws[key]
	expAcc.mu.RUnlock()
	if sh := shadowFor(expAcc, "foo.bar"); sh != nil {
		qw = sh.qw
	}
	return
}

func startLeafShadowQWHubAndSpoke(t *testing.T) (*Server, *Server, *Account, *Account) {
	t.Helper()
	hub, hubOpts := RunServerWithConfig(createConfFile(t, leafShadowQWHubConf()))
	t.Cleanup(hub.Shutdown)
	spoke, _ := RunServerWithConfig(createConfFile(t, leafShadowQWSpokeConf(hubOpts)))
	t.Cleanup(spoke.Shutdown)
	checkLeafNodeConnected(t, hub)

	expAcc, err := hub.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := hub.LookupAccount("IMP")
	require_NoError(t, err)
	return hub, spoke, expAcc, impAcc
}

// Path 1: the hub's importing account has weight 3, the exporting account's
// shadow announces weight 1 (while the shadow itself carries qw=3).
func TestLeafShadowQueueWeight(t *testing.T) {
	_, spoke, expAcc, impAcc := startLeafShadowQWHubAndSpoke(t)

	url := spoke.ClientURL()
	var ncs []*nats.Conn
	for i := 0; i < 3; i++ {
		nc := natsConnect(t, url, nats.UserInfo("imp", "pwd"))
		t.Cleanup(nc.Close)
		_, err := nc.QueueSubscribeSync("foo.bar", "q")
		require_NoError(t, err)
		require_NoError(t, nc.Flush())
		ncs = append(ncs, nc)
	}

	// Wait for the leaf sub and its shadow.
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if shadowFor(expAcc, "foo.bar") == nil {
			return fmt.Errorf("no shadow for foo.bar in EXP yet")
		}
		return nil
	})
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		impAcc.mu.RLock()
		n := impAcc.rm["N foo.bar q"]
		impAcc.mu.RUnlock()
		if n != 3 {
			return fmt.Errorf("IMP rm is %d, want 3", n)
		}
		return nil
	})

	impRM, expRM, impLWS, expLWS, qw := leafShadowWeights(t, impAcc, expAcc)
	t.Logf("IMP rm=%d lws=%d; EXP rm=%d lws=%d; shadow qw=%d", impRM, impLWS, expRM, expLWS, qw)

	var problems []string
	if impRM != 3 {
		problems = append(problems, fmt.Sprintf("IMP rm=%d, want 3", impRM))
	}
	if impLWS != 3 {
		problems = append(problems, fmt.Sprintf("IMP lws=%d, want 3", impLWS))
	}
	if expRM != 3 {
		problems = append(problems, fmt.Sprintf("EXP rm=%d, want 3 (shadow qw=%d)", expRM, qw))
	}
	if expLWS != 3 {
		problems = append(problems, fmt.Sprintf("EXP lws=%d, want 3", expLWS))
	}
	if qw != 3 {
		problems = append(problems, fmt.Sprintf("shadow qw=%d, want 3", qw))
	}
	if len(problems) > 0 {
		t.Fatalf("leaf shadow queue weight skew:\n%s", strings.Join(problems, "\n"))
	}
}

// Path 2: add/remove spoke members after the shadow exists. The importing
// account follows the new weight; the exporting account and the shadow's qw do
// not move.
func TestLeafShadowQueueWeightUpdates(t *testing.T) {
	_, spoke, expAcc, impAcc := startLeafShadowQWHubAndSpoke(t)

	url := spoke.ClientURL()
	mk := func() *nats.Conn {
		nc := natsConnect(t, url, nats.UserInfo("imp", "pwd"))
		_, err := nc.QueueSubscribeSync("foo.bar", "q")
		require_NoError(t, err)
		require_NoError(t, nc.Flush())
		return nc
	}
	var ncs []*nats.Conn
	for i := 0; i < 3; i++ {
		ncs = append(ncs, mk())
	}

	waitWeight := func(want int64) {
		t.Helper()
		checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
			impAcc.mu.RLock()
			n := impAcc.rm["N foo.bar q"]
			impAcc.mu.RUnlock()
			if n != want {
				return fmt.Errorf("IMP rm is %d, want %d", n, want)
			}
			return nil
		})
	}
	waitWeight(3)

	// Close one spoke member: the spoke sends LS+ with weight 2.
	ncs[0].Close()
	waitWeight(2)
	impRM, expRM, _, expLWS, qw := leafShadowWeights(t, impAcc, expAcc)
	t.Logf("after removing one spoke member: IMP rm=%d; EXP rm=%d lws=%d; shadow qw=%d", impRM, expRM, expLWS, qw)

	var problems []string
	if impRM != 2 {
		problems = append(problems, fmt.Sprintf("IMP rm=%d, want 2", impRM))
	}
	if expRM != 2 {
		problems = append(problems, fmt.Sprintf("EXP rm=%d, want 2 after the update (shadow qw=%d)", expRM, qw))
	}
	if qw != 2 {
		problems = append(problems, fmt.Sprintf("shadow qw=%d, want 2 after the update", qw))
	}

	// Add a fresh member: weight back to 3.
	ncs = append(ncs, mk())
	waitWeight(3)
	impRM, expRM, _, _, qw = leafShadowWeights(t, impAcc, expAcc)
	t.Logf("after adding a member back: IMP rm=%d; EXP rm=%d; shadow qw=%d", impRM, expRM, qw)
	if expRM != 3 {
		problems = append(problems, fmt.Sprintf("EXP rm=%d, want 3 after re-adding a member (shadow qw=%d)", expRM, qw))
	}
	if qw != 3 {
		problems = append(problems, fmt.Sprintf("shadow qw=%d, want 3 after re-adding a member", qw))
	}

	for _, nc := range ncs {
		nc.Close()
	}
	if len(problems) > 0 {
		t.Fatalf("leaf shadow queue weight does not follow updates:\n%s", strings.Join(problems, "\n"))
	}
}

// dropLeafConns closes the current leafnode connections of s (hub side), which
// forces the soliciting spokes to reconnect and re-send their aggregated LS+
// weights as new subscriptions.
func dropLeafConns(t *testing.T, s *Server) {
	t.Helper()
	s.mu.Lock()
	var leafs []*client
	for _, c := range s.leafs {
		leafs = append(leafs, c)
	}
	s.mu.Unlock()
	require_True(t, len(leafs) > 0)
	for _, c := range leafs {
		c.closeConnection(InternalClient)
	}
}

// Reconnect flavour: when the leaf (re)connects with N queue members already
// present, the spoke announces LS+ weight N in one line. The hub's leaf sub and
// the shadow both get qw=N, but the shadow is still announced to routes/leaves
// with the hard-coded +1, so EXP.rm stays 1 while the shadow says N.
func TestLeafShadowQueueWeightReconnect(t *testing.T) {
	hub, spoke, expAcc, impAcc := startLeafShadowQWHubAndSpoke(t)

	url := spoke.ClientURL()
	var ncs []*nats.Conn
	for i := 0; i < 3; i++ {
		nc := natsConnect(t, url, nats.UserInfo("imp", "pwd"))
		t.Cleanup(nc.Close)
		_, err := nc.QueueSubscribeSync("foo.bar", "q")
		require_NoError(t, err)
		require_NoError(t, nc.Flush())
		ncs = append(ncs, nc)
	}
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		impAcc.mu.RLock()
		n := impAcc.rm["N foo.bar q"]
		impAcc.mu.RUnlock()
		if n != 3 {
			return fmt.Errorf("IMP rm is %d, want 3", n)
		}
		return nil
	})

	// Force a fresh leaf session so the spoke's initial sync announces LS+
	// foo.bar q 3 as a single weighted line.
	dropLeafConns(t, hub)
	checkLeafNodeConnectedCount(t, hub, 0)
	checkLeafNodeConnected(t, hub)

	checkFor(t, 8*time.Second, 15*time.Millisecond, func() error {
		if sh := shadowFor(expAcc, "foo.bar"); sh == nil || sh.qw != 3 {
			return fmt.Errorf("shadow not recreated with qw=3 yet")
		}
		return nil
	})

	impRM, expRM, impLWS, expLWS, qw := leafShadowWeights(t, impAcc, expAcc)
	t.Logf("after leaf reconnect: IMP rm=%d lws=%d; EXP rm=%d lws=%d; shadow qw=%d",
		impRM, impLWS, expRM, expLWS, qw)

	var problems []string
	if impRM != 3 || impLWS != 3 {
		problems = append(problems, fmt.Sprintf("IMP rm/lws=%d/%d, want 3/3", impRM, impLWS))
	}
	if qw != 3 {
		problems = append(problems, fmt.Sprintf("shadow qw=%d, want 3", qw))
	}
	if expRM != 3 || expLWS != 3 {
		problems = append(problems, fmt.Sprintf(
			"EXP rm/lws=%d/%d, want 3/3 although the shadow says qw=%d", expRM, expLWS, qw))
	}
	for _, nc := range ncs {
		nc.Close()
	}
	if len(problems) > 0 {
		t.Fatalf("leaf shadow queue weight on reconnect:\n%s", strings.Join(problems, "\n"))
	}
}

// Path 3: two route peers whose queue interest is both leaf-backed. The hub H
// has three members behind leaf L1; peer P1 has one member behind leaf L2; the
// publisher sits on a third server P0 that routes to both. P0's picking is
// weighted by the advertised route qsub weights, so with the skew H gets the
// same weight (1) as P1's single member and the two sides split 50/50. With a
// correct weight (3) H would get 3/4 and each leaf member an even share.
// Counts only; nothing is lost.
func TestLeafShadowQueueWeightPeerShares(t *testing.T) {
	hub, hubOpts := RunServerWithConfig(createConfFile(t, leafShadowQWHubConf()))
	t.Cleanup(hub.Shutdown)
	l1, _ := RunServerWithConfig(createConfFile(t, leafShadowQWSpokeConf(hubOpts)))
	t.Cleanup(l1.Shutdown)

	p0Conf := []byte(fmt.Sprintf(`
		port: -1
		cluster {
			name: "C"
			listen: "127.0.0.1:-1"
			routes = [ "nats-route://127.0.0.1:%d" ]
		}
		accounts {
			EXP { users = [{user: "exp", password: "pwd"}] }
		}
	`, hubOpts.Cluster.Port))
	p0, p0Opts := RunServerWithConfig(createConfFile(t, p0Conf))
	t.Cleanup(p0.Shutdown)

	p1Conf := []byte(fmt.Sprintf(`
		port: -1
		cluster {
			name: "C"
			listen: "127.0.0.1:-1"
			routes = [ "nats-route://127.0.0.1:%d" ]
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				users = [{user: "leaf2", password: "pwd", account: "EXP"}]
			}
		}
		accounts {
			EXP { users = [{user: "exp", password: "pwd"}] }
		}
	`, p0Opts.Cluster.Port))
	p1, p1Opts := RunServerWithConfig(createConfFile(t, p1Conf))
	t.Cleanup(p1.Shutdown)

	l2Conf := []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes = [{url: "nats-leaf://leaf2:pwd@%s:%d", account: "EXP"}]
		}
		accounts {
			EXP { users = [{user: "exp", password: "pwd"}] }
		}
	`, p1Opts.LeafNode.Host, p1Opts.LeafNode.Port))
	l2, _ := RunServerWithConfig(createConfFile(t, l2Conf))
	t.Cleanup(l2.Shutdown)

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnected(t, p1)
	checkClusterFormed(t, hub, p0, p1)

	// Three members behind L1 (importing side) and one behind L2 (exporting
	// side), all in the same queue group for the imported subject.
	var l1Subs []*nats.Subscription
	var l1Conns []*nats.Conn
	for i := 0; i < 3; i++ {
		nc := natsConnect(t, l1.ClientURL(), nats.UserInfo("imp", "pwd"))
		t.Cleanup(nc.Close)
		sub, err := nc.QueueSubscribeSync("foo.bar", "q")
		require_NoError(t, err)
		require_NoError(t, nc.Flush())
		l1Subs = append(l1Subs, sub)
		l1Conns = append(l1Conns, nc)
	}
	l2NC := natsConnect(t, l2.ClientURL(), nats.UserInfo("exp", "pwd"))
	t.Cleanup(l2NC.Close)
	l2Sub, err := l2NC.QueueSubscribeSync("foo.bar", "q")
	require_NoError(t, err)
	require_NoError(t, l2NC.Flush())

	expAcc, err := hub.LookupAccount("EXP")
	require_NoError(t, err)
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if shadowFor(expAcc, "foo.bar") == nil {
			return fmt.Errorf("no shadow in EXP on the hub")
		}
		return nil
	})

	// P0 should see two leaf-backed route queue subs: one for H (the shadow)
	// and one for P1's leaf.
	p0Exp, err := p0.LookupAccount("EXP")
	require_NoError(t, err)
	var qws []int32
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		qws = qws[:0]
		for _, sub := range subsMatching(p0Exp, "foo.bar") {
			if sub.client != nil && sub.client.kind == ROUTER && len(sub.queue) > 0 {
				qws = append(qws, sub.qw)
			}
		}
		if len(qws) != 2 {
			return fmt.Errorf("P0 has %d route queue subs, want 2", len(qws))
		}
		return nil
	})
	t.Logf("publisher-side route queue sub weights for EXP foo.bar: %v (H shadow and P1 leaf)", qws)

	pub := natsConnect(t, p0.ClientURL(), nats.UserInfo("exp", "pwd"))
	t.Cleanup(pub.Close)
	const total = 400
	for i := 0; i < total; i++ {
		require_NoError(t, pub.Publish("foo.bar", []byte("m")))
	}
	require_NoError(t, pub.Flush())
	time.Sleep(700 * time.Millisecond)

	count := func(sub *nats.Subscription) int {
		n := 0
		for {
			m, err := sub.NextMsg(30 * time.Millisecond)
			if err != nil || m == nil {
				return n
			}
			n++
		}
	}
	var l1N [3]int
	for i, sub := range l1Subs {
		l1N[i] = count(sub)
	}
	l2N := count(l2Sub)
	t.Logf("published=%d: L1 (3 leaf members, behind the hub shadow)=%d (%d,%d,%d); L2 (1 leaf member, peer)=%d; lost=%d",
		total, l1N[0]+l1N[1]+l1N[2], l1N[0], l1N[1], l1N[2], l2N,
		total-l1N[0]-l1N[1]-l1N[2]-l2N)

	var problems []string
	// Under a correct weight the shadow represents 3 members and P1's leaf copy
	// 1 member, so all four members get ~1/4 each. Allow a +/-1/8 window.
	q := total / 4
	lo, hi := q-total/8, q+total/8
	if l2N < lo || l2N > hi {
		problems = append(problems, fmt.Sprintf(
			"peer-side leaf member got %d of %d, want about %d (1/4) if the hub shadow announced its weight of 3", l2N, total, q))
	}
	for i, n := range l1N {
		if n < lo || n > hi {
			problems = append(problems, fmt.Sprintf(
				"hub-side leaf member %d got %d of %d, want about %d (1/4) under a correct weight", i, n, total, q))
		}
	}
	// No loss: the queue group must account for every message.
	if got := l1N[0] + l1N[1] + l1N[2] + l2N; got != total {
		problems = append(problems, fmt.Sprintf("received %d of %d messages", got, total))
	}
	if len(problems) > 0 {
		t.Fatalf("leaf shadow queue weight peer shares:\n%s", strings.Join(problems, "\n"))
	}
}

// gwTally returns the gateway interest tally (entry.n) for account/key in s's
// outbound gateway interest map.
func gwTally(s *Server, accName, key string) (int64, bool) {
	s.gateway.pasi.Lock()
	defer s.gateway.pasi.Unlock()
	st := s.gateway.pasi.m[accName]
	if st == nil {
		return 0, false
	}
	e := st[key]
	if e == nil {
		return 0, false
	}
	return e.n, true
}

// Path 4: gateway consumer. A second server in another gateway region sees EXP's
// queue interest through the gateway. The gateway interest tally is built from
// the same deltas as the route map, so the shadow's hard-coded +1 shows the same
// skew: IMP has 3, EXP has 1 for the same leaf-backed queue interest.
func TestLeafShadowQueueWeightGateway(t *testing.T) {
	bConf := []byte(`
		port: -1
		gateway {
			name: "B"
			host: "127.0.0.1"
			port: -1
		}
		accounts {
			EXP { users = [{user: "exp", password: "pwd"}] }
			IMP { users = [{user: "imp", password: "pwd"}] }
		}
	`)
	b, bOpts := RunServerWithConfig(createConfFile(t, bConf))
	t.Cleanup(b.Shutdown)

	aConf := []byte(fmt.Sprintf(`
		port: -1
		system_account: SYS
		cluster {
			name: "A"
			listen: "127.0.0.1:-1"
		}
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				users = [{user: "leaf", password: "pwd", account: "IMP"}]
			}
		}
		gateway {
			name: "A"
			host: "127.0.0.1"
			port: -1
			gateways = [{ name: "B", url: "nats://127.0.0.1:%d" }]
		}
		accounts {
			SYS { users = [{user: "sys", password: "pwd"}] }
			EXP {
				users = [{user: "exp", password: "pwd"}]
				exports = [{stream: "foo.>"}]
			}
			IMP {
				users = [{user: "imp", password: "pwd"}]
				imports = [{stream: {account: EXP, subject: "foo.>"}}]
			}
		}
	`, bOpts.Gateway.Port))
	a, aOpts := RunServerWithConfig(createConfFile(t, aConf))
	t.Cleanup(a.Shutdown)

	spoke, _ := RunServerWithConfig(createConfFile(t, leafShadowQWSpokeConf(aOpts)))
	t.Cleanup(spoke.Shutdown)

	checkLeafNodeConnected(t, a)
	waitForOutboundGateways(t, a, 1, 5*time.Second)
	waitForOutboundGateways(t, b, 1, 5*time.Second)

	url := spoke.ClientURL()
	var ncs []*nats.Conn
	for i := 0; i < 3; i++ {
		nc := natsConnect(t, url, nats.UserInfo("imp", "pwd"))
		t.Cleanup(nc.Close)
		_, err := nc.QueueSubscribeSync("foo.bar", "q")
		require_NoError(t, err)
		require_NoError(t, nc.Flush())
		ncs = append(ncs, nc)
	}

	expAcc, err := a.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := a.LookupAccount("IMP")
	require_NoError(t, err)

	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if shadowFor(expAcc, "foo.bar") == nil {
			return fmt.Errorf("no shadow in EXP on the hub")
		}
		return nil
	})

	// Wait only for the primary's own tally to settle at 3; the EXP tally is
	// the value under test (1 on a skewed build, 3 once the shadow announces
	// its weight), so it must not gate the wait.
	var impTally, expTally int64
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		impTally, _ = gwTally(a, "IMP", "foo.bar q")
		expTally, _ = gwTally(a, "EXP", "foo.bar q")
		if impTally != 3 {
			return fmt.Errorf("gateway tally IMP=%d EXP=%d, want IMP 3", impTally, expTally)
		}
		return nil
	})

	impRM, expRM, impLWS, expLWS, qw := leafShadowWeights(t, impAcc, expAcc)
	t.Logf("gateway interest tallies on the hub: IMP foo.bar q=%d, EXP foo.bar q=%d", impTally, expTally)
	t.Logf("IMP rm=%d lws=%d; EXP rm=%d lws=%d; shadow qw=%d", impRM, impLWS, expRM, expLWS, qw)

	var problems []string
	if impTally != 3 {
		problems = append(problems, fmt.Sprintf("gateway tally IMP foo.bar q=%d, want 3", impTally))
	}
	if expTally != 3 {
		problems = append(problems, fmt.Sprintf(
			"gateway tally EXP foo.bar q=%d, want 3 (shadow qw=%d, EXP.rm=%d)", expTally, qw, expRM))
	}
	for _, nc := range ncs {
		nc.Close()
	}
	if len(problems) > 0 {
		t.Fatalf("leaf shadow queue weight in gateway interest:\n%s", strings.Join(problems, "\n"))
	}
}
