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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const streamImportAutoUnsubConf = `
	listen: 127.0.0.1:-1
	accounts: {
		EXP: {
			users: [ {user: pub, password: pwd} ]
			exports: [ {stream: "foo.>"}, {stream: "bar.>"} ]
		}
		IMP: {
			users: [ {user: sub, password: pwd} ]
			imports: [ {stream: {account: EXP, subject: "foo.>"}, prefix: "import"} ]
		}
	}
`

// Only difference: IMP gained a second stream import, so the reload marks IMP
// as "stream imports changed" and refreshes the import shadows.
const streamImportAutoUnsubConfChanged = `
	listen: 127.0.0.1:-1
	accounts: {
		EXP: {
			users: [ {user: pub, password: pwd} ]
			exports: [ {stream: "foo.>"}, {stream: "bar.>"} ]
		}
		IMP: {
			users: [ {user: sub, password: pwd} ]
			imports: [
				{stream: {account: EXP, subject: "foo.>"}, prefix: "import"},
				{stream: {account: EXP, subject: "bar.>"}, prefix: "import2"}
			]
		}
	}
`

// matchSubs returns a snapshot of the subscriptions matching subj. Some are
// shadow subscriptions living in the exporting account.
func matchSubs(a *Account, subj string) []*subscription {
	a.mu.RLock()
	sr := a.sl.Match(subj)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	a.mu.RUnlock()
	return subs
}

// importClient returns the only CLIENT connection in the account.
func importClient(a *Account) *client {
	var found *client
	a.mu.RLock()
	for c := range a.clients {
		c.mu.RLock()
		closed := c.isClosed()
		kind := c.kind
		c.mu.RUnlock()
		if kind == CLIENT && !closed {
			found = c
		}
	}
	a.mu.RUnlock()
	return found
}

func clientSubMapLen(c *client) int {
	if c == nil {
		return -1
	}
	c.mu.RLock()
	n := len(c.subs)
	c.mu.RUnlock()
	return n
}

// A stream-import subscriber that sets AutoUnsubscribe(1) must see the
// subscription removed after the first delivery. The shadow subscription
// copies max/nm at creation time, so the importing client's later
// UNSUB <sid> <max> never reaches it: the shadow keeps max==0, the delivery
// goes through the shadow and the server never auto-unsubscribes at all.
//
// nats.go enforces the max locally, so only the server state shows the bug.
func TestStreamImportAutoUnsubscribeAppliesToShadow(t *testing.T) {
	s, _ := RunServerWithConfig(createConfFile(t, []byte(streamImportAutoUnsubConf)))
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("sub", "pwd"))
	defer nc.Close()

	sub, err := nc.SubscribeSync("import.foo.bar")
	require_NoError(t, err)
	require_NoError(t, sub.AutoUnsubscribe(1))
	require_NoError(t, nc.Flush())

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := s.LookupAccount("IMP")
	require_NoError(t, err)

	// Precondition: IMP has the original and EXP has the shadow.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(matchSubs(impAcc, "import.foo.bar")); n != 1 {
			return fmt.Errorf("IMP has %d matching subs, want 1", n)
		}
		if n := len(matchSubs(expAcc, "foo.bar")); n != 1 {
			return fmt.Errorf("EXP has %d matching subs, want 1", n)
		}
		return nil
	})

	pub := natsConnect(t, s.ClientURL(), nats.UserInfo("pub", "pwd"))
	defer pub.Close()
	require_NoError(t, pub.Publish("foo.bar", []byte("one")))
	require_NoError(t, pub.Flush())

	// The single allowed delivery must auto-unsubscribe the original and the
	// shadow. On main both stay in their sublists and c.subs keeps the sid.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(matchSubs(expAcc, "foo.bar")); n != 0 {
			return fmt.Errorf("shadow still in exporting account after auto-unsub: %d", n)
		}
		if n := len(matchSubs(impAcc, "import.foo.bar")); n != 0 {
			return fmt.Errorf("original still in importing account after auto-unsub: %d", n)
		}
		if n := clientSubMapLen(importClient(impAcc)); n != 0 {
			return fmt.Errorf("client still tracks %d subs after auto-unsub", n)
		}
		return nil
	})
}

// The corrupting form: the auto-unsub must run on a shadow that inherited a
// non-zero max, which only happens when a stream-import refresh re-creates the
// shadows after the client set its limit. The auto-unsub then unsubscribes
// with the importing account instead of the exporting one, so the original is
// dropped from c.subs but stays in the importing sublist, and the shadow stays
// in the exporting sublist with its route interest.
func TestStreamImportAutoUnsubscribeShadowReloadCorruption(t *testing.T) {
	s, _, confFile := runReloadServerWithContent(t, []byte(streamImportAutoUnsubConf))
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("sub", "pwd"))
	defer nc.Close()

	sub, err := nc.SubscribeSync("import.foo.bar")
	require_NoError(t, err)
	require_NoError(t, sub.AutoUnsubscribe(1))
	require_NoError(t, nc.Flush())

	// Change IMP's stream imports so the reload refreshes its shadows; the
	// fresh shadow copy now carries max=1.
	require_NoError(t, os.WriteFile(confFile, []byte(streamImportAutoUnsubConfChanged), 0666))
	require_NoError(t, s.Reload())

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := s.LookupAccount("IMP")
	require_NoError(t, err)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		subs := matchSubs(expAcc, "foo.bar")
		if len(subs) != 1 {
			return fmt.Errorf("EXP has %d matching subs, want 1", len(subs))
		}
		sub := subs[0]
		sub.client.mu.RLock()
		max := sub.max
		sub.client.mu.RUnlock()
		if max != 1 {
			return fmt.Errorf("shadow not refreshed with max=1 (max=%d)", max)
		}
		return nil
	})

	pub := natsConnect(t, s.ClientURL(), nats.UserInfo("pub", "pwd"))
	defer pub.Close()
	require_NoError(t, pub.Publish("foo.bar", []byte("one")))
	require_NoError(t, pub.Flush())

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(matchSubs(expAcc, "foo.bar")); n != 0 {
			return fmt.Errorf("shadow still in exporting account after auto-unsub: %d", n)
		}
		if n := len(matchSubs(impAcc, "import.foo.bar")); n != 0 {
			return fmt.Errorf("original still in importing account after auto-unsub: %d", n)
		}
		if n := clientSubMapLen(importClient(impAcc)); n != 0 {
			return fmt.Errorf("client still tracks %d subs after auto-unsub", n)
		}
		return nil
	})
}
