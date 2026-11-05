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
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// techGLogger captures server trace output so a test can print the LS+/LS-
// sequence that a hub sends to a leaf connection.
type techGLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *techGLogger) logf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.buf, format, v...)
	l.buf.WriteByte('\n')
}

func (l *techGLogger) Noticef(format string, v ...any) { l.logf(format, v...) }
func (l *techGLogger) Warnf(format string, v ...any)   { l.logf(format, v...) }
func (l *techGLogger) Errorf(format string, v ...any)  { l.logf(format, v...) }
func (l *techGLogger) Fatalf(format string, v ...any)  { l.logf(format, v...) }
func (l *techGLogger) Debugf(format string, v ...any)  { l.logf(format, v...) }
func (l *techGLogger) Tracef(format string, v ...any)  { l.logf(format, v...) }

// leafUpdates returns every traced LS+ / LS- op, in order.
func (l *techGLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *techGLogger) leafUpdates() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range strings.Split(l.buf.String(), "\n") {
		if strings.Contains(line, "[LS+") || strings.Contains(line, "[LS-") {
			out = append(out, strings.TrimSpace(line))
		}
	}
	return out
}

// techGRunLeafHub starts a hub with a leafnode listener and returns it with
// the URL a spoke should solicit.
func techGRunLeafHub(t *testing.T, log *techGLogger) (*Server, string) {
	t.Helper()
	opts := DefaultOptions()
	opts.Port = -1
	opts.LeafNode.Port = -1
	h := RunServer(opts)
	t.Cleanup(h.Shutdown)
	if log != nil {
		h.SetLogger(log, false, true)
	}
	return h, fmt.Sprintf("nats-leaf://127.0.0.1:%d", opts.LeafNode.Port)
}

// techGRunLeafSpoke starts a leaf server (which also accepts leaf connections)
// soliciting a leaf connection to hubURL.
func techGRunLeafSpoke(t *testing.T, hubURL string) *Server {
	t.Helper()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		leafnodes {
			port: -1
			remotes = [{ url: %q }]
		}
	`, hubURL)))
	s, _ := RunServerWithConfig(conf)
	t.Cleanup(s.Shutdown)
	return s
}

// techGLeafClients returns the set of hub-side leaf connection ids.
func techGLeafClients(s *Server) map[uint64]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint64]struct{}, len(s.leafs))
	for cid := range s.leafs {
		out[cid] = struct{}{}
	}
	return out
}

// techGWaitNewLeafClient waits for a hub-side leaf connection that was not in
// the before set and returns it.
func techGWaitNewLeafClient(t *testing.T, s *Server, before map[uint64]struct{}) *client {
	t.Helper()
	var found *client
	checkFor(t, 5*time.Second, 5*time.Millisecond, func() error {
		s.mu.Lock()
		for cid, c := range s.leafs {
			if _, ok := before[cid]; !ok {
				found = c
				break
			}
		}
		s.mu.Unlock()
		if found == nil {
			return fmt.Errorf("new hub-side leaf connection not registered yet")
		}
		return nil
	})
	return found
}

// techGWaitInitParkedInSnapshot waits until the leaf connection's
// initLeafNodeSmapAndSendSubs is blocked inside the sublist snapshot while
// holding c.mu (the test holds the account sublist write lock).
func techGWaitInitParkedInSnapshot(t *testing.T, c *client) {
	t.Helper()
	var frames string
	checkFor(t, 10*time.Second, 5*time.Millisecond, func() error {
		// If the leaf lock is free, init has not reached the snapshot yet.
		if c.mu.TryLock() {
			c.mu.Unlock()
			return fmt.Errorf("leaf lock not held: init has not started its snapshot")
		}
		// Stack frames are printed innermost first, so the callees of init
		// are the lines before its own frame.
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		st := string(buf[:n])
		i := strings.Index(st, "initLeafNodeSmapAndSendSubs")
		if i < 0 {
			return fmt.Errorf("no initLeafNodeSmapAndSendSubs goroutine yet")
		}
		start := i - 4000
		if start < 0 {
			start = 0
		}
		frames = st[start:i]
		if strings.Contains(frames, "(*Sublist).All") ||
			strings.Contains(frames, "collectAllSubs") ||
			strings.Contains(frames, "(*RWMutex).RLock") {
			return nil
		}
		return fmt.Errorf("init holds the leaf lock but is not parked in the sublist snapshot yet")
	})
	var parked []string
	for _, line := range strings.Split(frames, "\n") {
		if strings.Contains(line, "Sublist") || strings.Contains(line, "RWMutex") || strings.Contains(line, "collectAllSubs") {
			parked = append(parked, strings.TrimSpace(line))
		}
	}
	t.Logf("init snapshot parked while holding the leaf lock: %s", strings.Join(parked, " | "))
}

// techGLeafQueueWeight reports the weight the server last accepted for a leaf
// queue subscription ("subject queue").
func techGLeafQueueWeight(s *Server, subject, queue string) (int32, bool) {
	acc := s.globalAccount()
	res := acc.sl.Match(subject)
	for _, qsubs := range res.qsubs {
		for _, sub := range qsubs {
			if sub.leaf && string(sub.queue) == queue {
				return atomic.LoadInt32(&sub.qw), true
			}
		}
	}
	return 0, false
}

// techGWaitLeafQSub waits for a queue sub matching filter and its weight on s.
func techGWaitLeafQSub(t *testing.T, s *Server, subject, queue string, want int32, filter func(*subscription) bool) *subscription {
	t.Helper()
	var found *subscription
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		res := s.globalAccount().sl.Match(subject)
		for _, qsubs := range res.qsubs {
			for _, sub := range qsubs {
				if filter(sub) && string(sub.queue) == queue {
					if qw := atomic.LoadInt32(&sub.qw); qw != want {
						return fmt.Errorf("leaf queue sub weight is %d, want %d", qw, want)
					}
					found = sub
					return nil
				}
			}
		}
		return fmt.Errorf("no leaf queue sub for %q %q yet", subject, queue)
	})
	return found
}

// techGSmapEntry reads a hub-side leaf connection's smap entry.
func techGSmapEntry(c *client, key string) (int64, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaf.smap == nil {
		return 0, false, false
	}
	n, ok := c.leaf.smap[key]
	return n, ok, true
}

func techGWaitSmapBuilt(t *testing.T, c *client) {
	t.Helper()
	checkFor(t, 5*time.Second, 5*time.Millisecond, func() error {
		if _, _, built := techGSmapEntry(c, ""); !built {
			return fmt.Errorf("hub leaf smap not built yet")
		}
		return nil
	})
}

// techGQueueMember is one queue subscriber used as a weight source.
type techGQueueMember struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

func techGAddQueueMembers(t *testing.T, nc *nats.Conn, subject, queue string, n int) []*techGQueueMember {
	t.Helper()
	out := make([]*techGQueueMember, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, &techGQueueMember{nc: nc, sub: natsQueueSubSync(t, nc, subject, queue)})
	}
	natsFlush(t, nc)
	return out
}

// techGLeafTailScene holds the state shared by the lock-ordered reproductions:
// the server whose snapshot we freeze (hub for the two-spoke and route shapes,
// mid for the daisy chain), the queue members that produce the weight delta,
// and the fresh leaf connection whose init is parked in its snapshot.
type techGLeafTailScene struct {
	t        *testing.T
	srv      *Server // server whose account sublist is write-locked
	acc      *Account
	slLocked bool
	members  []*techGQueueMember
	osub     *subscription
	initSrv  *Server
	c1       *client
	whence   string
	log      *techGLogger
}

func (sc *techGLeafTailScene) unlock() {
	if sc.slLocked {
		sc.acc.sl.Unlock()
		sc.slLocked = false
	}
}

// techGLeafSceneSpokes builds: hub H with spoke S2 announcing "foo Q" weight
// members, then a second spoke S1 whose hub-side snapshot is frozen. The delta
// is produced by spoke S2's weight update (processLeafSub on the hub).
func techGLeafSceneSpokes(t *testing.T, members int) (*techGLeafTailScene, string) {
	t.Helper()
	log := &techGLogger{}
	h, hubURL := techGRunLeafHub(t, log)
	spoke := techGRunLeafSpoke(t, hubURL)
	checkLeafNodeConnected(t, h)

	nc := natsConnect(t, spoke.ClientURL())
	t.Cleanup(nc.Close)
	mem := techGAddQueueMembers(t, nc, "foo", "Q", members)

	sc := &techGLeafTailScene{t: t, srv: h, acc: h.globalAccount(), members: mem, whence: "leaf", log: log}
	sc.osub = techGWaitLeafQSub(t, h, "foo", "Q", int32(members), func(sub *subscription) bool { return sub.leaf })

	sc.acc.sl.Lock()
	sc.slLocked = true
	t.Cleanup(sc.unlock)
	before := techGLeafClients(h)
	sc.initSrv = techGRunLeafSpoke(t, hubURL)
	sc.c1 = techGWaitNewLeafClient(t, h, before)
	techGWaitInitParkedInSnapshot(t, sc.c1)
	return sc, hubURL
}

// techGLeafSceneDaisy builds: top T with local queue members, mid M soliciting
// T and accepting leaf connections, and S1 soliciting M. M's init for S1 is
// frozen; the delta arrives on M's solicited connection to T.
func techGLeafSceneDaisy(t *testing.T, members int) *techGLeafTailScene {
	t.Helper()
	top, topURL := techGRunLeafHub(t, nil)
	nc := natsConnect(t, top.ClientURL())
	t.Cleanup(nc.Close)
	mem := techGAddQueueMembers(t, nc, "foo", "Q", members)

	mid := techGRunLeafSpoke(t, topURL)
	checkLeafNodeConnected(t, top)
	midURL := fmt.Sprintf("nats-leaf://127.0.0.1:%d", mid.getOpts().LeafNode.Port)

	sc := &techGLeafTailScene{t: t, srv: mid, acc: mid.globalAccount(), members: mem, whence: "remote leaf"}
	sc.osub = techGWaitLeafQSub(t, mid, "foo", "Q", int32(members), func(sub *subscription) bool { return sub.leaf })

	sc.acc.sl.Lock()
	sc.slLocked = true
	t.Cleanup(sc.unlock)
	before := techGLeafClients(mid)
	sc.initSrv = techGRunLeafSpoke(t, midURL)
	sc.c1 = techGWaitNewLeafClient(t, mid, before)
	techGWaitInitParkedInSnapshot(t, sc.c1)
	return sc
}

// techGLeafSceneRoute builds: account server A with local queue members routed
// to H, which also has a leaf listener; S1 solicits H. H's init for S1 is
// frozen; the delta arrives on H through the route (processRSub).
func techGLeafSceneRoute(t *testing.T, members int) *techGLeafTailScene {
	t.Helper()
	aOpts := DefaultOptions()
	aOpts.Port = -1
	aOpts.Cluster.Port = -1
	aOpts.Cluster.Name = "G"
	a := RunServer(aOpts)
	t.Cleanup(a.Shutdown)
	nc := natsConnect(t, a.ClientURL())
	t.Cleanup(nc.Close)
	mem := techGAddQueueMembers(t, nc, "foo", "Q", members)

	hConf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		cluster {
			name: "G"
			listen: 127.0.0.1:-1
			routes = [ nats-route://127.0.0.1:%d ]
		}
		leafnodes { port: -1 }
	`, aOpts.Cluster.Port)))
	h, hOpts := RunServerWithConfig(hConf)
	t.Cleanup(h.Shutdown)
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if an, hn := a.NumRoutes(), h.NumRoutes(); an < 1 || hn < 1 {
			return fmt.Errorf("routes not formed: a=%d h=%d", an, hn)
		}
		return nil
	})

	sc := &techGLeafTailScene{t: t, srv: h, acc: h.globalAccount(), members: mem, whence: "route"}
	sc.osub = techGWaitLeafQSub(t, h, "foo", "Q", int32(members), func(sub *subscription) bool {
		return sub.client != nil && sub.client.kind == ROUTER
	})

	sc.acc.sl.Lock()
	sc.slLocked = true
	t.Cleanup(sc.unlock)
	before := techGLeafClients(h)
	sc.initSrv = techGRunLeafSpoke(t, fmt.Sprintf("nats-leaf://127.0.0.1:%d", hOpts.LeafNode.Port))
	sc.c1 = techGWaitNewLeafClient(t, h, before)
	techGWaitInitParkedInSnapshot(t, sc.c1)
	return sc
}

// dropMembers removes n members from the queue group and waits until the
// server that owns the leaf connection stored the new weight.
func (sc *techGLeafTailScene) dropMembers(n int, want int32) {
	sc.t.Helper()
	m := sc.members[0]
	natsUnsub(sc.t, m.sub)
	natsFlush(sc.t, m.nc)
	sc.members = sc.members[1:]
	checkFor(sc.t, 5*time.Second, 5*time.Millisecond, func() error {
		if qw := atomic.LoadInt32(&sc.osub.qw); qw != want {
			return fmt.Errorf("stored queue weight is %d, want %d", qw, want)
		}
		return nil
	})
}

// settle releases the sublist lock, lets the snapshot complete and the pending
// delta land, and gives the update time to reach the new leaf.
func (sc *techGLeafTailScene) settle() {
	sc.t.Helper()
	sc.unlock()
	techGWaitSmapBuilt(sc.t, sc.c1)
	time.Sleep(100 * time.Millisecond)
	if sc.log != nil {
		sc.t.Logf("LS+/- updates sent to the new leaf: %v", sc.log.leafUpdates())
	}
}

// publishFromNewSpoke publishes on the new spoke and reports whether the
// surviving member received it.
func (sc *techGLeafTailScene) publishFromNewSpoke() error {
	sc.t.Helper()
	nc := natsConnect(sc.t, sc.initSrv.ClientURL())
	defer nc.Close()
	natsPub(sc.t, nc, "foo", []byte("hello"))
	natsFlush(sc.t, nc)
	if len(sc.members) == 0 {
		return fmt.Errorf("no surviving member")
	}
	// The queue group may deliver to any member.
	for _, m := range sc.members {
		msg, err := m.sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		if string(msg.Data) != "hello" {
			return fmt.Errorf("unexpected payload %q", msg.Data)
		}
		return nil
	}
	return fmt.Errorf("no queue member received the publish")
}

func (sc *techGLeafTailScene) smap() (int64, bool) {
	n, ok, _ := techGSmapEntry(sc.c1, "foo Q")
	return n, ok
}

// TestTechGLeafQueueWeightInitDelta reproduces the init/delta interleaving
// deterministically using only real connections and real locks:
//
//   - H has a leaf from S2 announcing queue "foo Q" with weight 2.
//   - The test takes H's account sublist write lock, so:
//     a) the next leaf connection (S1) runs initLeafNodeSmapAndSendSubs and
//     parks in the snapshot while holding its c.mu, and
//     b) a weight update from S2 parks in UpdateRemoteQSub *after* storing
//     the new weight in the subscription that the snapshot will read.
//   - Releasing the lock lets init snapshot weight 1 and then the pending
//     delta (-1, relative to the old weight 2) apply on top of it.
//
// On 9bad2e8d the hub ends with smap["foo Q"] == 0, sends LS- foo Q to S1,
// and a publish from S1 no longer reaches the surviving queue member.
func TestTechGLeafQueueWeightInitDelta(t *testing.T) {
	sc, _ := techGLeafSceneSpokes(t, 2)
	sc.dropMembers(1, 1)
	sc.settle()

	smapN, smapOK := sc.smap()
	s1Weight, s1HasSub := techGLeafQueueWeight(sc.initSrv, "foo", "Q")
	pubErr := sc.publishFromNewSpoke()
	t.Logf("hub leaf smap[%q]=%d (present=%v); new spoke weight=%d (hasSub=%v); publish err=%v",
		"foo Q", smapN, smapOK, s1Weight, s1HasSub, pubErr)

	want := int64(1)
	if !smapOK || smapN != want {
		t.Fatalf("smap[%q] is %d (present=%v), want %d: the snapshot counted weight 1 and the in-flight delta -1 was applied on top",
			"foo Q", smapN, smapOK, want)
	}
	if !s1HasSub || s1Weight != 1 {
		t.Fatalf("new spoke accepted weight %d (hasSub=%v), want 1: hub sent a premature LS- after double-counting the delta",
			s1Weight, s1HasSub)
	}
	if pubErr != nil {
		t.Fatalf("publish from the new spoke did not reach the surviving queue member: %v", pubErr)
	}
}

// TestTechGLeafQueueWeightInitDeltaControl runs the same frozen-snapshot
// sequence with no membership change: every server must converge on weight 2.
func TestTechGLeafQueueWeightInitDeltaControl(t *testing.T) {
	sc, _ := techGLeafSceneSpokes(t, 2)
	sc.settle()

	smapN, smapOK := sc.smap()
	pubErr := sc.publishFromNewSpoke()
	t.Logf("control: hub leaf smap[%q]=%d (present=%v); publish err=%v", "foo Q", smapN, smapOK, pubErr)
	if !smapOK || smapN != 2 {
		t.Fatalf("control smap[%q] is %d (present=%v), want 2", "foo Q", smapN, smapOK)
	}
	if pubErr != nil {
		t.Fatalf("control: publish from the new spoke did not reach the queue member: %v", pubErr)
	}
}

// TestTechGLeafQueueWeightOffsetPersists shows that after the wrong value is
// installed every later weight delta is still relative to the broken base, and
// that only a reconnect of the new leaf rebuilds the value.
func TestTechGLeafQueueWeightOffsetPersists(t *testing.T) {
	sc, hubURL := techGLeafSceneSpokes(t, 2)
	sc.dropMembers(1, 1)
	sc.settle()

	// A new member joins: 1 -> 2. On main the offset makes this 0 + 1 = 1.
	mem := techGAddQueueMembers(t, sc.members[0].nc, "foo", "Q", 1)
	sc.members = append(sc.members, mem...)
	checkFor(t, 5*time.Second, 5*time.Millisecond, func() error {
		if qw := atomic.LoadInt32(&sc.osub.qw); qw != 2 {
			return fmt.Errorf("stored queue weight is %d, want 2", qw)
		}
		return nil
	})
	time.Sleep(100 * time.Millisecond)
	smapN, smapOK := sc.smap()
	s1Weight, s1HasSub := techGLeafQueueWeight(sc.initSrv, "foo", "Q")
	t.Logf("after a later relative delta: hub smap[%q]=%d (present=%v), new spoke weight=%d (hasSub=%v)",
		"foo Q", smapN, smapOK, s1Weight, s1HasSub)
	if !smapOK || smapN != 2 {
		t.Fatalf("offset does not persist across later deltas: smap[%q]=%d (present=%v), want 2", "foo Q", smapN, smapOK)
	}

	// Reconnect the new leaf: its snapshot rebuilds smap from current weights.
	before := techGLeafClients(sc.srv)
	sc.initSrv.Shutdown()
	sc.initSrv = techGRunLeafSpoke(t, hubURL)
	sc.c1 = techGWaitNewLeafClient(t, sc.srv, before)
	techGWaitSmapBuilt(t, sc.c1)
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		if w, ok := techGLeafQueueWeight(sc.initSrv, "foo", "Q"); !ok || w != 2 {
			return fmt.Errorf("after reconnect spoke weight=%d (hasSub=%v), want 2", w, ok)
		}
		return nil
	})
	smapN, smapOK = sc.smap()
	t.Logf("after reconnect: hub smap[%q]=%d (present=%v)", "foo Q", smapN, smapOK)
	if !smapOK || smapN != 2 {
		t.Fatalf("reconnect did not repair the announced weight: smap[%q]=%d (present=%v), want 2", "foo Q", smapN, smapOK)
	}
	if err := sc.publishFromNewSpoke(); err != nil {
		t.Fatalf("after reconnect publish did not reach the queue member: %v", err)
	}
}

// TestTechGLeafQueueWeightInitDeltaRoute is reach path 2: the weight delta
// reaches the initialising server through a route (processRSub).
func TestTechGLeafQueueWeightInitDeltaRoute(t *testing.T) {
	sc := techGLeafSceneRoute(t, 2)
	sc.dropMembers(1, 1)
	sc.settle()

	smapN, smapOK := sc.smap()
	s1Weight, s1HasSub := techGLeafQueueWeight(sc.initSrv, "foo", "Q")
	pubErr := sc.publishFromNewSpoke()
	t.Logf("route path: hub smap[%q]=%d (present=%v); new spoke weight=%d (hasSub=%v); publish err=%v",
		"foo Q", smapN, smapOK, s1Weight, s1HasSub, pubErr)
	if !smapOK || smapN != 1 {
		t.Fatalf("route path: smap[%q] is %d (present=%v), want 1 (delta came from processRSub)", "foo Q", smapN, smapOK)
	}
	if !s1HasSub || s1Weight != 1 {
		t.Fatalf("route path: new spoke accepted weight %d (hasSub=%v), want 1", s1Weight, s1HasSub)
	}
	if pubErr != nil {
		t.Fatalf("route path: publish from the new spoke did not reach the surviving queue member: %v", pubErr)
	}
}

// TestTechGLeafQueueWeightInitDeltaDaisy is reach path 3: the freezing server
// both accepts the new leaf and solicits its own hub, so the weight delta
// arrives on its solicited connection while it initialises the accepted leaf.
func TestTechGLeafQueueWeightInitDeltaDaisy(t *testing.T) {
	sc := techGLeafSceneDaisy(t, 2)
	sc.dropMembers(1, 1)
	sc.settle()

	smapN, smapOK := sc.smap()
	s1Weight, s1HasSub := techGLeafQueueWeight(sc.initSrv, "foo", "Q")
	pubErr := sc.publishFromNewSpoke()
	t.Logf("daisy path: mid smap[%q]=%d (present=%v); new spoke weight=%d (hasSub=%v); publish err=%v",
		"foo Q", smapN, smapOK, s1Weight, s1HasSub, pubErr)
	if !smapOK || smapN != 1 {
		t.Fatalf("daisy path: smap[%q] is %d (present=%v), want 1", "foo Q", smapN, smapOK)
	}
	if !s1HasSub || s1Weight != 1 {
		t.Fatalf("daisy path: new spoke accepted weight %d (hasSub=%v), want 1", s1Weight, s1HasSub)
	}
	if pubErr != nil {
		t.Fatalf("daisy path: publish from the new spoke did not reach the surviving queue member: %v", pubErr)
	}
}

// TestTechGLeafQueueWeightInitChurn is the natural-timing variant: the hub
// holds many local subscriptions so a new leaf's snapshot is long, and queue
// membership churns while the leaf connects. It reports how often the
// announced weight is wrong; it is statistical by nature (the deterministic
// repro is TestTechGLeafQueueWeightInitDelta).
func TestTechGLeafQueueWeightInitChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("churn variant is statistical and slow")
	}
	log := &techGLogger{}
	h, hubURL := techGRunLeafHub(t, log)
	spoke := techGRunLeafSpoke(t, hubURL)
	checkLeafNodeConnected(t, h)

	// Lengthen the snapshot: many local subscriptions in the hub account.
	ncFill := natsConnect(t, h.ClientURL())
	t.Cleanup(ncFill.Close)
	for i := 0; i < 5000; i++ {
		if _, err := ncFill.SubscribeSync(fmt.Sprintf("fill.%d", i)); err != nil {
			t.Fatalf("fill subscribe: %v", err)
		}
	}
	natsFlush(t, ncFill)

	nc := natsConnect(t, spoke.ClientURL())
	t.Cleanup(nc.Close)
	members := techGAddQueueMembers(t, nc, "foo", "Q", 3)
	techGWaitLeafQSub(t, h, "foo", "Q", 3, func(sub *subscription) bool { return sub.leaf })

	attempts := 12
	bad := 0
	for i := 0; i < attempts; i++ {
		below := techGLeafClients(h)
		initSpoke := techGRunLeafSpoke(t, hubURL)
		// Jitter the membership change against the in-flight snapshot.
		time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
		natsUnsub(t, members[0].sub)
		natsFlush(t, nc)
		members = members[1:]

		c1 := techGWaitNewLeafClient(t, h, below)
		techGWaitSmapBuilt(t, c1)
		time.Sleep(50 * time.Millisecond)
		n, ok, _ := techGSmapEntry(c1, "foo Q")
		w, hasSub := techGLeafQueueWeight(initSpoke, "foo", "Q")
		if !ok || n != 2 || !hasSub || w != 2 {
			bad++
			t.Logf("attempt %d: smap[foo Q]=%d (present=%v), new spoke weight=%d (hasSub=%v), want 2", i, n, ok, w, hasSub)
		}
		initSpoke.Shutdown()
		checkLeafNodeConnected(t, h)
		// Restore a third member for the next attempt.
		members = append(members, techGAddQueueMembers(t, nc, "foo", "Q", 1)...)
		techGWaitLeafQSub(t, h, "foo", "Q", 3, func(sub *subscription) bool { return sub.leaf })
	}
	t.Logf("churn: %d/%d attempts announced the wrong weight (LS ops observed: %d)", bad, attempts, len(log.leafUpdates()))
	if bad > 0 {
		t.Fatalf("churn: %d/%d attempts announced a wrong queue weight", bad, attempts)
	}
}
