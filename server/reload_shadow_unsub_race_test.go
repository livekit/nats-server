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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const reloadShadowUnsubRaceConf = `
	port: -1
	cluster {
		name: "A"
		listen: "127.0.0.1:-1"
	}
	accounts {
		EXP {
			users: [{user: exp, password: pwd}]
			exports: [{stream: "foo.>"}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [{stream: {account: EXP, subject: "foo.>"}, prefix: "imp"}]
		}
	}
`

// shadowSnapshot returns the subscriptions currently matching subj in acc.
func shadowSnapshot(acc *Account, subj string) []*subscription {
	acc.mu.RLock()
	sr := acc.sl.Match(subj)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	acc.mu.RUnlock()
	return subs
}

// A stream-import refresh (processSubsOnConfigReload) clears sub.shadow, then
// re-adds the shadows and only assigns the new handle at the end. An UNSUB that
// lands in that window sees a nil handle, removes nothing and closes the sub;
// the refresh then inserts fresh shadows for a sub that no longer exists.
//
// The interleaving is forced deterministically: the refresh blocks in
// addShadowSubscriptions on the importing account's read lock, which is taken
// right after the handle is cleared and before any shadow is inserted. Holding
// the account write lock lets the UNSUB run exactly in the window.
//
// On pinned main the exporting account is left with the old *and* the new
// shadow and their route interest; with researcher-r2's fix (which withdraws
// oldShadows) the old one is gone but the new one still leaks, because it is
// not part of oldShadows.
func TestStreamImportRefreshLosesRaceWithUnsubscribe(t *testing.T) {
	s, _ := RunServerWithConfig(createConfFile(t, []byte(reloadShadowUnsubRaceConf)))
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer nc.Close()

	sub, err := nc.SubscribeSync("imp.foo.bar")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := s.LookupAccount("IMP")
	require_NoError(t, err)

	// Locate the importing client and its only subscription.
	var c *client
	impAcc.mu.RLock()
	for cl := range impAcc.clients {
		if cl.kind == CLIENT && !cl.isClosed() {
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

	// Precondition: the subscribe announced one shadow in the exporting
	// account and the handle is recorded on the client's sub.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(shadowSnapshot(expAcc, "foo.bar")); n != 1 {
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

	// Park the refresh in addShadowSubscriptions: it takes acc.mu.RLock after
	// clearing sub.shadow and before inserting the replacements.
	impAcc.mu.Lock()
	refreshed := make(chan struct{})
	go func() {
		defer close(refreshed)
		c.processSubsOnConfigReload(map[string]struct{}{impAcc.Name: {}})
	}()

	// Wait for the handle to be cleared; the refresh is now blocked.
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		c.mu.Lock()
		n := len(csub.shadow)
		c.mu.Unlock()
		if n != 0 {
			return fmt.Errorf("refresh has not cleared the shadow handle yet")
		}
		return nil
	})

	// The UNSUB lands in the window and sees no shadows to withdraw.
	require_NoError(t, sub.Unsubscribe())
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		c.mu.Lock()
		n := len(c.subs)
		c.mu.Unlock()
		if n != 0 {
			return fmt.Errorf("client still tracks %d subs after the unsub", n)
		}
		return nil
	})

	// Let the refresh finish: it inserts the new shadows for the dead sub.
	impAcc.mu.Unlock()
	<-refreshed
	require_NoError(t, nc.Flush())

	expSubs := shadowSnapshot(expAcc, "foo.bar")
	if len(expSubs) == 0 {
		t.Fatalf("no shadow sub left in EXP: the refresh did not leak one")
	}
	for _, shadow := range expSubs {
		key := keyFromSubWithOrigin(shadow)
		expAcc.mu.RLock()
		rm, lws := expAcc.rm[key], expAcc.lws[key]
		expAcc.mu.RUnlock()
		t.Logf("leaked shadow: sid=%q subject=%q closed=%v rm=%d lws=%d",
			shadow.sid, shadow.subject, shadow.isClosed(), rm, lws)
		if rm == 0 {
			t.Fatalf("leaked shadow %q has no route interest", key)
		}
	}
	t.Fatalf("exporting account still has %d shadow sub(s) for a dead sid", len(expSubs))

	// Unreachable while the leak exists; keep the permanence oracle for the
	// fixed build.
	nc.Close()
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := expAcc.TotalSubs(); n != 0 {
			return fmt.Errorf("EXP still has %d subs after the subscriber closed", n)
		}
		return nil
	})
}
