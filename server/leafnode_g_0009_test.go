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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// g0009TraceLogger records only the hub's outgoing leaf subscription updates
// for the key under test, so a test can report whether `LS- foo qq` was
// actually written to a leaf.
type g0009TraceLogger struct {
	mu   sync.Mutex
	seen []string
}

func (l *g0009TraceLogger) Noticef(string, ...any) {}
func (l *g0009TraceLogger) Warnf(string, ...any)   {}
func (l *g0009TraceLogger) Errorf(string, ...any)  {}
func (l *g0009TraceLogger) Debugf(string, ...any)  {}

func (l *g0009TraceLogger) Fatalf(format string, v ...any) {
	panic(fmt.Sprintf(format, v...))
}

func (l *g0009TraceLogger) Tracef(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, " foo qq") && (strings.Contains(msg, "LS-") || strings.Contains(msg, "LS+")) {
		l.mu.Lock()
		l.seen = append(l.seen, msg)
		l.mu.Unlock()
	}
}

func (l *g0009TraceLogger) mark() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.seen)
}

// since returns the number of LS+/LS- ops for the key after mark m.
func (l *g0009TraceLogger) since(m int) (plus, minus int, sample []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, msg := range l.seen[m:] {
		if strings.Contains(msg, "LS+") {
			plus++
		} else {
			minus++
		}
	}
	n := len(l.seen) - m
	if n > 6 {
		sample = append(sample, l.seen[m:m+3]...)
		sample = append(sample, "..."+l.seen[n+m-1])
	} else {
		sample = append(sample, l.seen[m:]...)
	}
	return plus, minus, sample
}

// Ticket 0009: a closing connection's queued `-delta` can be applied to a leaf
// connection whose init snapshot never counted it.
//
// clearAccountSubs (client.go:6465) removes the closing connection's subs from
// the account sublist in one RemoveBatch (client.go:6491) and only then feeds
// the `-delta` updates to the leaf connections: one per plain sub
// (client.go:6493-6521) and the aggregated queue deltas afterwards
// (client.go:6523-6532). A leaf connection that runs
// initLeafNodeSmapAndSendSubs (leafnode.go:2618) after RemoveBatch but before
// the deltas snapshots a sublist that no longer has the closing subs, so
// updateSmap (leafnode.go:2900) applies a `-delta` for a key the snapshot
// counted without it. If another leaf still has a live queue member for the
// same key, the count lands on that live member's count, is driven to zero and
// the hub sends `LS-`, so the new leaf stops forwarding a subject for which
// live interest still exists.
//
// The tests below assert the desired behaviour (new leaf keeps the live
// member's interest, smap 1, delivery 1/1), so they fail on 9bad2e8d when the
// race is reached.

const (
	g0009ClosingSubs   = 20000
	g0009LeafCloseSubs = 40000
	g0009Fillers       = 3
	g0009Key           = "foo qq"
)

// g0009SpokeConf returns a spoke config that solicits the hub leaf listener.
func g0009SpokeConf(name string, hubLeafPort int) []byte {
	return []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: %s
		leaf {
			remotes: [ { url: "nats-leaf://127.0.0.1:%d" } ]
		}
	`, name, hubLeafPort))
}

// g0009LeafForRemote returns the hub-side leaf connection whose remote server
// name is the given one.
func g0009LeafForRemote(s *Server, remoteName string) *client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, lc := range s.leafs {
		lc.mu.Lock()
		remote := lc.leaf.remoteServer
		lc.mu.Unlock()
		if remote == remoteName {
			return lc
		}
	}
	return nil
}

// g0009SmapValue returns the hub-side smap count for key on that leaf connection.
func g0009SmapValue(lc *client, key string) int64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.leaf.smap == nil {
		return 0
	}
	return lc.leaf.smap[key]
}

// g0009HubTotalQW sums the queue weights this server stores for the remote
// subscriptions announced with the given "subject queue" key.
func g0009HubTotalQW(s *Server, key string) int32 {
	var total int32
	s.mu.RLock()
	for _, lc := range s.leafs {
		lc.mu.Lock()
		if sub := lc.subs[key]; sub != nil {
			total += atomic.LoadInt32(&sub.qw)
		}
		lc.mu.Unlock()
	}
	s.mu.RUnlock()
	return total
}

// g0009WaitInterestGone spins (no sleep, to stay inside the close window) until
// the account has no interest for subject. It returns how long that took. The
// vanish is the observable proof that RemoveBatch has run: only
// clearAccountSubs removes these subs, and it removes all of them in one batch
// before it starts delivering the deltas.
func g0009WaitInterestGone(t *testing.T, acc *Account, subject string, max time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < max {
		if acc.Interest(subject) == 0 {
			return time.Since(start)
		}
	}
	t.Fatalf("interest for %q never dropped to 0 within %v", subject, max)
	return 0
}

// g0009Settle waits until the new leaf connection exists and its hub-side smap
// count for the key and the spoke-side interest for the subject stop changing,
// then returns them. It gives up after max and returns the last reading.
//
// The leaf's init snapshot can legitimately be delayed behind an in-flight
// close (a fix that serialises the two must make the new leaf wait for the
// closing connection's delta loop), so the stability check only counts once
// the leaf has been told about the key at least once. Otherwise a snapshot
// that has not run yet reads as a stable zero and the caller publishes before
// the leaf has any interest at all.
func g0009Settle(t *testing.T, hub, spoke *Server, remoteName, key, subject string, max time.Duration) (*client, int64, int) {
	t.Helper()
	var (
		lc       *client
		smap     int64
		interest int
		lastSmap int64
		lastInt  int
		stable   int
		seen     bool
	)
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if lc == nil {
			lc = g0009LeafForRemote(hub, remoteName)
		}
		if lc != nil {
			smap = g0009SmapValue(lc, key)
		}
		interest = spoke.globalAccount().Interest(subject)
		if smap != 0 {
			seen = true
		}
		if smap == lastSmap && interest == lastInt {
			stable++
			if seen && stable >= 3 {
				return lc, smap, interest
			}
		} else {
			stable = 0
		}
		lastSmap, lastInt = smap, interest
		time.Sleep(20 * time.Millisecond)
	}
	return lc, smap, interest
}

// g0009Delivered drains everything pending on sub for up to a second and
// returns the number of messages.
func g0009Delivered(sub *nats.Subscription) int {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p, _, _ := sub.Pending(); p > 0 {
			n := 0
			for ; p > 0; p-- {
				if _, err := sub.NextMsg(time.Second); err == nil {
					n++
				}
			}
			return n
		}
		time.Sleep(2 * time.Millisecond)
	}
	return 0
}

// g0009PublishFromLeaf publishes one message on each subject from a client
// connected to the given spoke and returns what the subscribed side received.
func g0009PublishFromLeaf(t *testing.T, spoke *Server, msgSub, ctrlSub *nats.Subscription) (int, int) {
	t.Helper()
	nc := natsConnect(t, spoke.ClientURL())
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("hello"))
	natsPub(t, nc, "bar", []byte("hello"))
	natsFlush(t, nc)
	return g0009Delivered(msgSub), g0009Delivered(ctrlSub)
}

// g0009SetupHub starts a hub with a leaf listener and returns it plus the leaf
// listener port.
func g0009SetupHub(t *testing.T, name string) (*Server, int) {
	t.Helper()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: %s
		leaf { listen: 127.0.0.1:-1 }
	`, name)))
	hub, o := RunServerWithConfig(conf)
	t.Cleanup(hub.Shutdown)
	return hub, o.LeafNode.Port
}

// g0009StartSpoke starts a spoke that solicits the hub, waits for its leaf
// connection and registers it for g0009Settle. The caller shuts it down.
func g0009StartSpoke(t *testing.T, hubLeafPort int, name string) *Server {
	t.Helper()
	s, _ := RunServerWithConfig(createConfFile(t, g0009SpokeConf(name, hubLeafPort)))
	checkLeafNodeConnected(t, s)
	return s
}

func g0009StopSpoke(s *Server) {
	s.Shutdown()
	s.WaitForShutdown()
}

// g0009StartLiveMember starts the spoke Z holding the live queue member on
// foo/qq plus a plain control subscription on bar, and the filler spokes that
// make every interest delta walk more leaf connections.
func g0009SetupLiveMember(t *testing.T, hub *Server, hubLeafPort int) (spokeZ *Server, mZ, barSub *nats.Subscription) {
	t.Helper()
	spokeZ = g0009StartSpoke(t, hubLeafPort, "g9-z")
	t.Cleanup(func() { g0009StopSpoke(spokeZ) })

	ncZ := natsConnect(t, spokeZ.ClientURL())
	t.Cleanup(ncZ.Close)
	mZ = natsQueueSubSync(t, ncZ, "foo", "qq")
	barSub = natsSubSync(t, ncZ, "bar")
	natsFlush(t, ncZ)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := g0009HubTotalQW(hub, g0009Key); n != 1 {
			return fmt.Errorf("hub total queue weight for %q = %d, want 1", g0009Key, n)
		}
		return nil
	})
	return spokeZ, mZ, barSub
}

func g0009SetupFillers(t *testing.T, hubLeafPort int) {
	t.Helper()
	for i := 0; i < g0009Fillers; i++ {
		name := fmt.Sprintf("g9-fill-%d", i)
		s := g0009StartSpoke(t, hubLeafPort, name)
		t.Cleanup(func() { g0009StopSpoke(s) })
	}
}

// g0009FillClosingClient subscribes nc to n00k plain subjects x.* and to the
// queue group on foo/qq, then flushes and waits until the hub has parsed all of
// them.
func g0009FillClosingClient(t *testing.T, hub *Server, nc *nats.Conn, nsubs int, queueOnly bool) {
	t.Helper()
	for j := 0; j < nsubs; j++ {
		var err error
		if queueOnly {
			_, err = nc.QueueSubscribeSync(fmt.Sprintf("q.%05d", j), "qq")
		} else {
			_, err = nc.SubscribeSync(fmt.Sprintf("x.%06d", j))
		}
		require_NoError(t, err)
	}
	natsQueueSubSync(t, nc, "foo", "qq")
	natsFlush(t, nc)

	last := fmt.Sprintf("x.%06d", nsubs-1)
	if queueOnly {
		last = fmt.Sprintf("q.%05d", nsubs-1)
	}
	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		if n := hub.globalAccount().Interest(last); n != 1 {
			return fmt.Errorf("hub has not parsed the last closing sub yet (%d)", n)
		}
		if n := hub.globalAccount().Interest("foo"); n != 2 {
			return fmt.Errorf("hub interest for foo = %d, want 2", n)
		}
		return nil
	})
}

// TestG0009Path1ClientCloseDeltaVsNewLeafInit is the primary reach path: a
// local hub client X with 20k plain subs plus the queue sub closes, and a new
// spoke Y connects after RemoveBatch has run but before the deferred deltas.
func TestG0009Path1ClientCloseDeltaVsNewLeafInit(t *testing.T) {
	const attempts = 3

	hub, lnPort := g0009SetupHub(t, "g9-hub")
	_, mZ, barSub := g0009SetupLiveMember(t, hub, lnPort)
	g0009SetupFillers(t, lnPort)
	tl := &g0009TraceLogger{}
	hub.SetLoggerV2(tl, false, true, false)

	for i := 0; i < attempts; i++ {
		t.Run(fmt.Sprintf("attempt%d", i), func(t *testing.T) {
			if p, _, _ := mZ.Pending(); p != 0 {
				t.Fatalf("leftover messages on the live member: %d", p)
			}

			ncX := natsConnect(t, hub.ClientURL())
			g0009FillClosingClient(t, hub, ncX, g0009ClosingSubs, false)
			mark := tl.mark()

			// Close X first, then start Y as soon as its subs are out of the
			// sublist but before the deltas have all been applied.
			closeStart := time.Now()
			ncX.Close()
			goneAfter := g0009WaitInterestGone(t, hub.globalAccount(), fmt.Sprintf("x.%06d", g0009ClosingSubs-1), 2*time.Second)

			yName := fmt.Sprintf("g9-y-%d", i)
			spokeY := g0009StartSpoke(t, lnPort, yName)
			yConnected := time.Since(closeStart)
			lcY, smap, yInterest := g0009Settle(t, hub, spokeY, yName, g0009Key, "foo", 3*time.Second)
			delivered, ctrl := g0009PublishFromLeaf(t, spokeY, mZ, barSub)
			plus, minus, sample := tl.since(mark)
			g0009StopSpoke(spokeY)

			if lcY == nil {
				t.Fatalf("no hub-side leaf connection for %s", yName)
			}
			t.Logf("attempt %d: hub trace LS+ foo qq x%d, LS- foo qq x%d: %q", i, plus, minus, sample)
			t.Logf("attempt %d: RemoveBatch observed %.3fms after close start (hub interest x.*: 1->0), "+
				"Y leaf connected %.3fms after close start; hub smap[%q] on Y=%d, Y interest(%q)=%d, "+
				"foo delivered %d/1, bar control delivered %d/1 (hub total QW=%d)",
				i, float64(goneAfter.Microseconds())/1000, float64(yConnected.Microseconds())/1000,
				g0009Key, smap, "foo", yInterest, delivered, ctrl, g0009HubTotalQW(hub, g0009Key))

			// Desired: the new leaf keeps the live member's interest.
			require_Equal(t, smap, int64(1))
			require_Equal(t, yInterest, 1)
			require_Equal(t, delivered, 1)
			require_Equal(t, ctrl, 1)
		})
	}
}

// TestG0009Path2QueueOnlyCloseDeltaVsNewLeafInit isolates the aggregated queue
// delta: the closing client holds queue subs only (many groups/subjects so the
// aggregated loop is long, with foo/qq somewhere in it).
func TestG0009Path2QueueOnlyCloseDeltaVsNewLeafInit(t *testing.T) {
	const (
		attempts = 10
		closingQ = 5000
	)

	hub, lnPort := g0009SetupHub(t, "g9-hub-q")
	_, mZ, barSub := g0009SetupLiveMember(t, hub, lnPort)
	g0009SetupFillers(t, lnPort)

	failures := 0
	for i := 0; i < attempts; i++ {
		if p, _, _ := mZ.Pending(); p != 0 {
			t.Fatalf("leftover messages on the live member: %d", p)
		}

		ncX := natsConnect(t, hub.ClientURL())
		g0009FillClosingClient(t, hub, ncX, closingQ, true)

		closeStart := time.Now()
		ncX.Close()
		goneAfter := g0009WaitInterestGone(t, hub.globalAccount(), fmt.Sprintf("q.%05d", closingQ-1), 2*time.Second)

		yName := fmt.Sprintf("g9-qy-%d", i)
		spokeY := g0009StartSpoke(t, lnPort, yName)
		yConnected := time.Since(closeStart)
		lcY, smap, yInterest := g0009Settle(t, hub, spokeY, yName, g0009Key, "foo", 3*time.Second)
		delivered, ctrl := g0009PublishFromLeaf(t, spokeY, mZ, barSub)
		g0009StopSpoke(spokeY)

		if lcY == nil {
			t.Fatalf("no hub-side leaf connection for %s", yName)
		}
		bad := smap != 1 || yInterest != 1 || delivered != 1
		if bad {
			failures++
		}
		t.Logf("attempt %d: RemoveBatch %.3fms after close, Y leaf %.3fms; smap=%d interest=%d delivered=%d/1 control=%d/1",
			i, float64(goneAfter.Microseconds())/1000, float64(yConnected.Microseconds())/1000,
			smap, yInterest, delivered, ctrl)
	}
	t.Logf("queue-only failures: %d/%d attempts", failures, attempts)
}

// TestG0009Path3LeafCloseDeltaVsNewLeafInit closes a LEAF connection (a whole
// spoke server with 20k subs plus the queue sub) instead of a client: this is
// the leaf-reconnect-storm shape of the same window.
func TestG0009Path3LeafCloseDeltaVsNewLeafInit(t *testing.T) {
	const attempts = 3

	hub, lnPort := g0009SetupHub(t, "g9-hub-l")
	_, mZ, barSub := g0009SetupLiveMember(t, hub, lnPort)
	g0009SetupFillers(t, lnPort)

	for i := 0; i < attempts; i++ {
		t.Run(fmt.Sprintf("attempt%d", i), func(t *testing.T) {
			if p, _, _ := mZ.Pending(); p != 0 {
				t.Fatalf("leftover messages on the live member: %d", p)
			}

			xName := fmt.Sprintf("g9-x-%d", i)
			spokeX := g0009StartSpoke(t, lnPort, xName)
			ncX := natsConnect(t, spokeX.ClientURL())
			g0009FillClosingClient(t, hub, ncX, g0009LeafCloseSubs, false)

			// Closing the spoke makes the hub run clearAccountSubs for a LEAF
			// connection.
			closeStart := time.Now()
			ncX.Close()
			g0009StopSpoke(spokeX)
			goneAfter := g0009WaitInterestGone(t, hub.globalAccount(), fmt.Sprintf("x.%06d", g0009LeafCloseSubs-1), 3*time.Second)

			yName := fmt.Sprintf("g9-ly-%d", i)
			spokeY := g0009StartSpoke(t, lnPort, yName)
			yConnected := time.Since(closeStart)
			lcY, smap, yInterest := g0009Settle(t, hub, spokeY, yName, g0009Key, "foo", 3*time.Second)
			delivered, ctrl := g0009PublishFromLeaf(t, spokeY, mZ, barSub)
			g0009StopSpoke(spokeY)

			if lcY == nil {
				t.Fatalf("no hub-side leaf connection for %s", yName)
			}
			t.Logf("attempt %d: RemoveBatch of the leaf's 20k subs observed %.3fms after close start, "+
				"Y leaf connected %.3fms after close start; hub smap[%q] on Y=%d, Y interest(%q)=%d, "+
				"foo delivered %d/1, bar control delivered %d/1 (hub total QW=%d)",
				i, float64(goneAfter.Microseconds())/1000, float64(yConnected.Microseconds())/1000,
				g0009Key, smap, "foo", yInterest, delivered, ctrl, g0009HubTotalQW(hub, g0009Key))

			require_Equal(t, smap, int64(1))
			require_Equal(t, yInterest, 1)
			require_Equal(t, delivered, 1)
			require_Equal(t, ctrl, 1)
		})
	}
}

// TestG0009Path5NaturalChurn repeats the same close+new-leaf shape with no
// ordering helper at all: the new spoke is started immediately after the
// closer returns, so it races the hub's processing of the close however the
// scheduler decides. This reports a ratio, not a deterministic repro.
func TestG0009Path5NaturalChurn(t *testing.T) {
	const attempts = 10

	hub, lnPort := g0009SetupHub(t, "g9-hub-n")
	_, mZ, barSub := g0009SetupLiveMember(t, hub, lnPort)
	g0009SetupFillers(t, lnPort)

	failures := 0
	for i := 0; i < attempts; i++ {
		if p, _, _ := mZ.Pending(); p != 0 {
			t.Fatalf("leftover messages on the live member: %d", p)
		}

		ncX := natsConnect(t, hub.ClientURL())
		g0009FillClosingClient(t, hub, ncX, g0009ClosingSubs, false)

		closeStart := time.Now()
		ncX.Close()
		yName := fmt.Sprintf("g9-ny-%d", i)
		spokeY := g0009StartSpoke(t, lnPort, yName)
		lcY, smap, yInterest := g0009Settle(t, hub, spokeY, yName, g0009Key, "foo", 3*time.Second)
		delivered, ctrl := g0009PublishFromLeaf(t, spokeY, mZ, barSub)
		g0009StopSpoke(spokeY)

		if lcY == nil {
			t.Fatalf("no hub-side leaf connection for %s", yName)
		}
		bad := smap != 1 || yInterest != 1 || delivered != 1
		if bad {
			failures++
		}
		t.Logf("attempt %d: Y connected %.3fms after close returned; smap=%d interest=%d delivered=%d/1 control=%d/1",
			i, float64(time.Since(closeStart).Microseconds())/1000, smap, yInterest, delivered, ctrl)
	}
	t.Logf("natural churn failures: %d/%d attempts", failures, attempts)
}

// TestG0009Path7Controls exercises the ordering requirement:
// (a) the close fully settles before the new leaf connects: correct interest;
// (b) the close happens with no new leaf during the window: the live member is
//
//	untouched and a plain hub publish still reaches it;
//
// (c) the new leaf connects before the close starts: the candidate's
//
//	originally passing shape.
func TestG0009Path7Controls(t *testing.T) {
	hub, lnPort := g0009SetupHub(t, "g9-hub-c")
	_, mZ, barSub := g0009SetupLiveMember(t, hub, lnPort)
	g0009SetupFillers(t, lnPort)

	t.Run("settled close then new leaf", func(t *testing.T) {
		ncX := natsConnect(t, hub.ClientURL())
		g0009FillClosingClient(t, hub, ncX, g0009ClosingSubs, false)
		ncX.Close()
		checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
			if n := hub.globalAccount().Interest("foo"); n != 1 {
				return fmt.Errorf("hub interest for foo = %d, want 1", n)
			}
			return nil
		})
		// Let any deferred delta finish before the leaf connects.
		time.Sleep(200 * time.Millisecond)

		yName := "g9-c-a"
		spokeY := g0009StartSpoke(t, lnPort, yName)
		lcY, smap, yInterest := g0009Settle(t, hub, spokeY, yName, g0009Key, "foo", 3*time.Second)
		delivered, ctrl := g0009PublishFromLeaf(t, spokeY, mZ, barSub)
		g0009StopSpoke(spokeY)
		if lcY == nil {
			t.Fatalf("no hub-side leaf connection for %s", yName)
		}
		t.Logf("settled close: smap=%d interest=%d delivered=%d/1 control=%d/1", smap, yInterest, delivered, ctrl)
		require_Equal(t, smap, int64(1))
		require_Equal(t, yInterest, 1)
		require_Equal(t, delivered, 1)
		require_Equal(t, ctrl, 1)
	})

	t.Run("close with no new leaf", func(t *testing.T) {
		ncX := natsConnect(t, hub.ClientURL())
		g0009FillClosingClient(t, hub, ncX, g0009ClosingSubs, false)
		ncX.Close()
		checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
			if n := hub.globalAccount().Interest("foo"); n != 1 {
				return fmt.Errorf("hub interest for foo = %d, want 1", n)
			}
			return nil
		})
		time.Sleep(200 * time.Millisecond)
		qw := g0009HubTotalQW(hub, g0009Key)
		nc := natsConnect(t, hub.ClientURL())
		natsPub(t, nc, "foo", []byte("hello"))
		natsPub(t, nc, "bar", []byte("hello"))
		natsFlush(t, nc)
		nc.Close()
		delivered, ctrl := g0009Delivered(mZ), g0009Delivered(barSub)
		t.Logf("no new leaf: hub total QW=%d delivered=%d/1 control=%d/1", qw, delivered, ctrl)
		require_Equal(t, qw, int32(1))
		require_Equal(t, delivered, 1)
		require_Equal(t, ctrl, 1)
	})

	t.Run("new leaf before close starts", func(t *testing.T) {
		yName := "g9-c-c"
		spokeY := g0009StartSpoke(t, lnPort, yName)
		lcY, smap, yInterest := g0009Settle(t, hub, spokeY, yName, g0009Key, "foo", 3*time.Second)
		if lcY == nil {
			t.Fatalf("no hub-side leaf connection for %s", yName)
		}
		require_Equal(t, smap, int64(1))
		require_Equal(t, yInterest, 1)

		for i := 0; i < 3; i++ {
			ncX := natsConnect(t, hub.ClientURL())
			g0009FillClosingClient(t, hub, ncX, g0009ClosingSubs, false)
			ncX.Close()
			checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
				if n := hub.globalAccount().Interest("foo"); n != 1 {
					return fmt.Errorf("hub interest for foo = %d, want 1", n)
				}
				return nil
			})
		}
		time.Sleep(200 * time.Millisecond)
		_, smap, yInterest = g0009Settle(t, hub, spokeY, yName, g0009Key, "foo", 3*time.Second)
		delivered, ctrl := g0009PublishFromLeaf(t, spokeY, mZ, barSub)
		g0009StopSpoke(spokeY)
		t.Logf("leaf before close: smap=%d interest=%d delivered=%d/1 control=%d/1", smap, yInterest, delivered, ctrl)
		require_Equal(t, smap, int64(1))
		require_Equal(t, yInterest, 1)
		require_Equal(t, delivered, 1)
		require_Equal(t, ctrl, 1)
	})
}
