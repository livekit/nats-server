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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Independent repro for ticket 0006: processSubsOnConfigReload clears
// sub.shadow, then (outside the client lock) rebuilds shadows. addShadowSubscriptions
// takes the importing account's read lock at its top, so holding that account's
// write lock parks the refresh after the clear and before any insert. An UNSUB
// in that window sees a nil shadow handle, cleans up nothing, and the refresh
// then installs shadows (and +1 interest) for a subscription that is gone.

const shadowRaceConf = `
	listen: 127.0.0.1:-1
	cluster {
		name: "C"
		listen: 127.0.0.1:-1
		%s
	}
	accounts {
		EXP {
			users: [ {user: e, password: p} ]
			exports: [ {stream: "foo.>"} ]
		}
		IMP {
			users: [ {user: i, password: p} ]
			imports: [ {stream: {account: EXP, subject: "foo.>"}, prefix: "imp."} ]
		}
	}
`

func shadowRaceConfig(routes string) []byte {
	routeLine := ""
	if routes != "" {
		routeLine = fmt.Sprintf("routes: [ %q ]", routes)
	}
	return []byte(fmt.Sprintf(shadowRaceConf, routeLine))
}

// subsForClient returns every local sub in acc whose client has cid.
func subsForClient(acc *Account, cid uint64) []*subscription {
	var raw [256]*subscription
	subs := raw[:0]
	acc.mu.RLock()
	acc.sl.localSubs(&subs, false)
	var out []*subscription
	for _, sub := range subs {
		if sub.client != nil && sub.client.cid == cid {
			out = append(out, sub)
		}
	}
	acc.mu.RUnlock()
	return out
}

// shadowSubsForClient returns the shadow subs in acc owned by client cid.
func shadowSubsForClient(acc *Account, cid uint64) []*subscription {
	var out []*subscription
	for _, sub := range subsForClient(acc, cid) {
		if sub.im != nil {
			out = append(out, sub)
		}
	}
	return out
}

// expShadows returns the shadow subs matching subj in acc.
func expShadows(acc *Account, subj string) []*subscription {
	acc.mu.RLock()
	sr := acc.sl.Match(subj)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	acc.mu.RUnlock()
	return subs
}

// parkRefreshAndUnsub sets up one client sub in IMP, parks a refresh in the
// shadow-rebuild window and runs unsub() there. It returns the importing
// account and the client once the window has been exercised.
func parkRefreshAndUnsub(t *testing.T, s *Server, nc *nats.Conn, unsub func()) (*Account, *Account, *client) {
	t.Helper()
	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := s.LookupAccount("IMP")
	require_NoError(t, err)

	impAcc.mu.RLock()
	var c *client
	for cl := range impAcc.clients {
		cl.mu.Lock()
		live := cl.kind == CLIENT && !cl.isClosed()
		cl.mu.Unlock()
		if live {
			c = cl
		}
	}
	impAcc.mu.RUnlock()
	require_NotNil(t, c)

	var csub *subscription
	c.mu.Lock()
	for _, sb := range c.subs {
		csub = sb
	}
	c.mu.Unlock()
	require_NotNil(t, csub)

	// Precondition: one shadow in EXP, tracked by the client sub.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(expShadows(expAcc, "foo.bar")); n != 1 {
			return fmt.Errorf("EXP has %d shadow subs, want 1", n)
		}
		c.mu.Lock()
		n := len(csub.shadow)
		c.mu.Unlock()
		if n != 1 {
			return fmt.Errorf("client sub tracks %d shadows, want 1", n)
		}
		return nil
	})

	// Park the refresh between clearing the handle and adding replacements.
	impAcc.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.processSubsOnConfigReload(map[string]struct{}{impAcc.Name: {}})
	}()
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		c.mu.Lock()
		n := len(csub.shadow)
		c.mu.Unlock()
		if n != 0 {
			return fmt.Errorf("refresh has not cleared the shadow handle yet")
		}
		return nil
	})

	unsub()
	// Wait until the server applied the UNSUB (removed the sub from c.subs)
	// while the refresh is still parked: this is the lost-update window.
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		c.mu.Lock()
		n := len(c.subs)
		c.mu.Unlock()
		if n != 0 {
			return fmt.Errorf("client still has %d subs", n)
		}
		return nil
	})

	impAcc.mu.Unlock()
	<-done
	// Flush so the UNSUB is fully applied server-side before the caller closes
	// the connection (otherwise the close would clean the leaked shadow).
	require_NoError(t, nc.Flush())
	c.mu.Lock()
	nsubs, nshadow := len(c.subs), len(csub.shadow)
	c.mu.Unlock()
	t.Logf("after parked refresh: EXP shadows=%d client subs=%d shadow handle=%d",
		len(expShadows(expAcc, "foo.bar")), nsubs, nshadow)
	return expAcc, impAcc, c
}

// Path 1/2 deterministic form: UNSUB lands in the refresh window; the refresh
// leaves the dead sub's shadow (old on main, new with 0005's fix) in EXP with
// route interest.
func TestShadowRefreshLosesRaceWithUnsub(t *testing.T) {
	s, _ := RunServerWithConfig(createConfFile(t, shadowRaceConfig("")))
	t.Cleanup(s.Shutdown)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("i", "p"))
	sub, err := nc.SubscribeSync("imp.foo.bar")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	expAcc, impAcc, c := parkRefreshAndUnsub(t, s, nc, func() {
		require_NoError(t, sub.Unsubscribe())
	})

	c.mu.Lock()
	cline := fmt.Sprintf("subs=%d closed=%v", len(c.subs), c.isClosed())
	c.mu.Unlock()
	t.Logf("before nc.Close: client %s, EXP shadows=%d", cline, len(expShadows(expAcc, "foo.bar")))
	nc.Close()
	t.Logf("right after nc.Close: EXP shadows=%d", len(expShadows(expAcc, "foo.bar")))

	// Wait for the close to be fully processed on IMP.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		impAcc.mu.RLock()
		nclients := len(impAcc.clients)
		impAcc.mu.RUnlock()
		if nclients != 0 {
			return fmt.Errorf("IMP still has %d clients after close", nclients)
		}
		return nil
	})

	shadows := expShadows(expAcc, "foo.bar")
	var leaks []string
	for _, shadow := range shadows {
		key := keyFromSubWithOrigin(shadow)
		expAcc.mu.RLock()
		rm, lws := expAcc.rm[key], expAcc.lws[key]
		expAcc.mu.RUnlock()
		leaks = append(leaks, fmt.Sprintf("sid=%q subject=%q closed=%v rm=%d lws=%d",
			shadow.sid, shadow.subject, shadow.isClosed(), rm, lws))
	}
	t.Logf("after race + close: EXP TotalSubs=%d shadows=%d rm entries=%d",
		expAcc.TotalSubs(), len(shadows), len(expAcc.rm))

	if len(leaks) > 0 {
		t.Fatalf("exporting account leaked %d shadow sub(s) for a dead sid after UNSUB + close:\n%s",
			len(leaks), strings.Join(leaks, "\n"))
	}
}

// Path 5 cluster variant: the peer keeps receiving interest for the leaked
// shadow after the reload and the unsubscribe have both returned.
func TestShadowRefreshRacePeerKeepsInterest(t *testing.T) {
	confA := createConfFile(t, shadowRaceConfig(""))
	sA, optsA := RunServerWithConfig(confA)
	t.Cleanup(sA.Shutdown)

	confB := createConfFile(t, shadowRaceConfig(
		fmt.Sprintf("nats-route://127.0.0.1:%d", optsA.Cluster.Port)))
	sB, _ := RunServerWithConfig(confB)
	t.Cleanup(sB.Shutdown)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if sA.NumRemotes() != 1 || sB.NumRemotes() != 1 {
			return fmt.Errorf("routes not up: A=%d B=%d", sA.NumRemotes(), sB.NumRemotes())
		}
		return nil
	})

	nc := natsConnect(t, sA.ClientURL(), nats.UserInfo("i", "p"))
	sub, err := nc.SubscribeSync("imp.foo.bar")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	expAcc, _, _ := parkRefreshAndUnsub(t, sA, nc, func() {
		require_NoError(t, sub.Unsubscribe())
	})
	nc.Close()

	time.Sleep(200 * time.Millisecond)
	expB, err := sB.LookupAccount("EXP")
	require_NoError(t, err)
	var peer []string
	expB.mu.RLock()
	sr := expB.sl.Match("foo.bar")
	for _, sub := range sr.psubs {
		if sub.client != nil && sub.client.kind == ROUTER {
			peer = append(peer, string(sub.subject))
		}
	}
	expB.mu.RUnlock()

	expAcc.mu.RLock()
	total := expAcc.TotalSubs()
	expAcc.mu.RUnlock()
	left := expShadows(expAcc, "foo.bar")
	t.Logf("after race + close: EXP local subs=%d shadows on foo.bar=%d, peer route subs=%d", total, len(left), len(peer))
	if len(left) != 0 || len(peer) != 0 {
		t.Fatalf("peer keeps interest for a dead shadow: EXP shadows=%d peer route subs=%d", len(left), len(peer))
	}
}

// raceLoopConf is the reload-loop config. The second variant adds an unrelated
// bar.> import so each reload changes IMP's stream imports and therefore
// refreshes every existing shadow of the account.
func raceLoopConf(extra bool) []byte {
	extraLine := ""
	if extra {
		extraLine = `, {stream: {account: EXP, subject: "baz.>"}}`
	}
	return []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		cluster {
			name: "C"
			listen: 127.0.0.1:-1
		}
		accounts {
			EXP {
				users: [ {user: e, password: p} ]
				exports: [ {stream: ">"} ]
			}
			IMP {
				users: [ {user: i, password: p} ]
				imports: [ {stream: {account: EXP, subject: "foo.>"}, prefix: "imp."} %s ]
			}
		}
	`, extraLine))
}

// raceLoop runs iterations of "real reload vs UNSUB or client close" and
// returns how many iterations left a shadow behind for a dead client.
func raceLoop(t *testing.T, iterations int, closeClient bool) (leaks int) {
	t.Helper()
	conf := createConfFile(t, raceLoopConf(false))
	s, _ := RunServerWithConfig(conf)
	t.Cleanup(s.Shutdown)

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := s.LookupAccount("IMP")
	require_NoError(t, err)

	for i := 0; i < iterations; i++ {
		nc := natsConnect(t, s.ClientURL(), nats.UserInfo("i", "p"))
		const nsubs = 64
		subs := make([]*nats.Subscription, 0, nsubs)
		for j := 0; j < nsubs; j++ {
			sub, err := nc.SubscribeSync(fmt.Sprintf("imp.foo.%d", j))
			require_NoError(t, err)
			subs = append(subs, sub)
		}
		require_NoError(t, nc.Flush())

		var c *client
		checkFor(t, 2*time.Second, time.Millisecond, func() error {
			impAcc.mu.RLock()
			var cur *client
			for cl := range impAcc.clients {
				cl.mu.Lock()
				live := cl.kind == CLIENT && !cl.isClosed()
				cl.mu.Unlock()
				if live {
					cur = cl
				}
			}
			n := len(impAcc.clients)
			impAcc.mu.RUnlock()
			c = cur
			if n != 1 || c == nil {
				return fmt.Errorf("IMP has %d clients", n)
			}
			return nil
		})
		c.mu.Lock()
		cid := c.cid
		c.mu.Unlock()
		checkFor(t, 2*time.Second, time.Millisecond, func() error {
			if n := len(shadowSubsForClient(expAcc, cid)); n != nsubs {
				return fmt.Errorf("EXP has %d shadow subs, want %d", n, nsubs)
			}
			return nil
		})

		require_NoError(t, os.WriteFile(conf, raceLoopConf(i%2 == 1), 0666))
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Reload()
		}()
		if closeClient {
			nc.Close()
		} else {
			for _, sub := range subs {
				require_NoError(t, sub.Unsubscribe())
			}
			require_NoError(t, nc.Flush())
		}
		<-done
		nc.Close()

		checkFor(t, 2*time.Second, time.Millisecond, func() error {
			impAcc.mu.RLock()
			n := len(impAcc.clients)
			impAcc.mu.RUnlock()
			if n != 0 {
				return fmt.Errorf("IMP still has %d clients", n)
			}
			return nil
		})

		left := shadowSubsForClient(expAcc, cid)
		if len(left) > 0 {
			leaks++
			if leaks <= 3 {
				sh := left[0]
				key := keyFromSubWithOrigin(sh)
				expAcc.mu.RLock()
				rm := expAcc.rm[key]
				expAcc.mu.RUnlock()
				t.Logf("iteration %d: %d leaked shadow(s) for cid=%d, e.g. subject=%q closed=%v rm=%d",
					i, len(left), cid, sh.subject, sh.isClosed(), rm)
			}
		}
	}
	return leaks
}

// Path 1: concurrent real reload and subscriber UNSUB, 200 iterations.
func TestShadowRefreshReloadVsUnsubLoop(t *testing.T) {
	const iterations = 200
	leaks := raceLoop(t, iterations, false)
	t.Logf("reload-vs-UNSUB: %d/%d iterations leaked a shadow", leaks, iterations)
	if leaks != 0 {
		t.Fatalf("reload-vs-UNSUB race leaked in %d/%d iterations", leaks, iterations)
	}
}

// Path 3: client close concurrent with the reload (clearAccountSubs).
func TestShadowRefreshReloadVsCloseLoop(t *testing.T) {
	const iterations = 200
	leaks := raceLoop(t, iterations, true)
	t.Logf("reload-vs-close: %d/%d iterations leaked a shadow", leaks, iterations)
	if leaks != 0 {
		t.Fatalf("reload-vs-close race leaked in %d/%d iterations", leaks, iterations)
	}
}

// Diagnostic: a real reload with changed imports does rebuild the shadow
// (rm inflates to 2 on main), so the loops below exercise the refresh path.
func TestDiagReloadRefreshHappens(t *testing.T) {
	conf := createConfFile(t, raceLoopConf(false))
	s, _ := RunServerWithConfig(conf)
	t.Cleanup(s.Shutdown)
	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("i", "p"))
	defer nc.Close()
	_, err = nc.SubscribeSync("imp.foo.bar")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	key := "R foo.bar"
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		expAcc.mu.RLock()
		rm := expAcc.rm[key]
		expAcc.mu.RUnlock()
		if rm != 1 {
			return fmt.Errorf("rm=%d, want 1", rm)
		}
		return nil
	})
	require_NoError(t, os.WriteFile(conf, raceLoopConf(true), 0666))
	require_NoError(t, s.Reload())
	expAcc.mu.RLock()
	rm := expAcc.rm[key]
	expAcc.mu.RUnlock()
	t.Logf("after a bare reload with changed imports: rm[%q]=%d shadows=%d", key, rm, len(expShadows(expAcc, "foo.bar")))
}
