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

// autoUnsubShadowConf is a two-server cluster. IMP gets no stream import at
// boot; the test adds it with a config reload after the subscriber has set its
// auto-unsubscribe limit, so the refreshed shadow copies max=1.
const autoUnsubShadowConf = `
	listen: 127.0.0.1:-1
	cluster {
		name: %q
		listen: 127.0.0.1:-1
		%s
	}
	accounts: {
		EXP: {
			users: [ {user: publisher, password: pwd} ]
			exports: [ {stream: "foo.>"} ]
		}
		IMP: {
			users: [ {user: subscriber, password: pwd} ]
			%s
		}
	}
`

func autoUnsubShadowConfig(name, routes, imp string) []byte {
	if routes != "" {
		routes = "routes: [ " + routes + " ]"
	}
	return []byte(fmt.Sprintf(autoUnsubShadowConf, name, routes, imp))
}

func autoUnsubMatchAll(a *Account, subject string) []*subscription {
	a.mu.RLock()
	sr := a.sl.Match(subject)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	a.mu.RUnlock()
	return subs
}

// A stream-import shadow that reaches its auto-unsubscribe limit must take the
// original subscription with it: remove it from the importing account's
// sublist, delete its sid from the client map, remove the shadow from the
// exporting account's sublist and withdraw the route interest of both.
func TestStreamImportShadowAutoUnsubCleanup(t *testing.T) {
	confA := createConfFile(t, autoUnsubShadowConfig("C", "", ""))
	sA, optsA := RunServerWithConfig(confA)
	defer sA.Shutdown()

	confB := createConfFile(t, autoUnsubShadowConfig("C",
		fmt.Sprintf(`"nats-route://127.0.0.1:%d"`, optsA.Cluster.Port), ""))
	sB, optsB := RunServerWithConfig(confB)
	defer sB.Shutdown()

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if sA.NumRemotes() != 1 || sB.NumRemotes() != 1 {
			return fmt.Errorf("routes not up: A=%d B=%d", sA.NumRemotes(), sB.NumRemotes())
		}
		return nil
	})

	expAcc, err := sA.lookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := sA.lookupAccount("IMP")
	require_NoError(t, err)
	baseA := sA.NumSubscriptions()
	baseExp := expAcc.TotalSubs()
	baseImp := impAcc.TotalSubs()

	// AutoUnsubscribe(1) sends SUB then UNSUB <sid> 1, which sets max on the
	// original subscription only.
	nc := natsConnect(t, sA.ClientURL(), nats.UserInfo("subscriber", "pwd"))
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo.bar")
	require_NoError(t, err)
	require_NoError(t, sub.AutoUnsubscribe(1))
	require_NoError(t, nc.Flush())

	// Add the stream import: the refresh rebuilds the shadow from the original
	// that already carries max=1.
	require_NoError(t, os.WriteFile(confA, autoUnsubShadowConfig("C", "",
		`imports: [ {stream: {account: EXP, subject: "foo.>"}} ]`), 0666))
	require_NoError(t, sA.Reload())

	var shadow *subscription
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		subs := autoUnsubMatchAll(expAcc, "foo.bar")
		if len(subs) != 1 {
			return fmt.Errorf("EXP has %d shadow subs for foo.bar, want 1", len(subs))
		}
		if subs[0].max != 1 {
			return fmt.Errorf("shadow was rebuilt with max=%d, want 1", subs[0].max)
		}
		shadow = subs[0]
		return nil
	})
	key := keyFromSubWithOrigin(shadow)
	expAcc.mu.RLock()
	rmBefore := expAcc.rm[key]
	expAcc.mu.RUnlock()
	require_Equal(t, rmBefore, int64(1))

	// One publish from the peer server reaches the shadow and its auto-unsub.
	pub := natsConnect(t, fmt.Sprintf("nats://publisher:pwd@127.0.0.1:%d", optsB.Port))
	defer pub.Close()
	require_NoError(t, pub.Publish("foo.bar", []byte("hello")))
	require_NoError(t, pub.Flush())
	natsNexMsg(t, sub, 2*time.Second)

	// The only part that works on main is the removal of the sid from the
	// client's sub map; wait for it so the rest is sampled after the auto-unsub.
	var subc *client
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		subc = nil
		impAcc.mu.RLock()
		for c := range impAcc.clients {
			if c.kind == CLIENT && !c.isClosed() {
				subc = c
			}
		}
		impAcc.mu.RUnlock()
		if subc == nil {
			return fmt.Errorf("no client in the importing account")
		}
		subc.mu.RLock()
		n := len(subc.subs)
		subc.mu.RUnlock()
		if n != 0 {
			return fmt.Errorf("client sub map still has %d entries", n)
		}
		return nil
	})

	impSubs := autoUnsubMatchAll(impAcc, "foo.bar")
	expSubs := autoUnsubMatchAll(expAcc, "foo.bar")
	expAcc.mu.RLock()
	rmAfter, lwsAfter := expAcc.rm[key], expAcc.lws[key]
	expAcc.mu.RUnlock()

	var problems []string
	if len(impSubs) != 0 {
		problems = append(problems, fmt.Sprintf(
			"importing account still matches %d sub(s) for foo.bar", len(impSubs)))
	}
	if len(expSubs) != 0 {
		problems = append(problems, fmt.Sprintf(
			"exporting account still matches %d shadow sub(s) for foo.bar", len(expSubs)))
	}
	if rmAfter != 0 || lwsAfter != 0 {
		problems = append(problems, fmt.Sprintf(
			"exporting account route entry %q not withdrawn: rm=%d lws=%d", key, rmAfter, lwsAfter))
	}

	if len(problems) > 0 {
		t.Fatalf("stream-import shadow auto-unsub cleanup:\n%s", strings.Join(problems, "\n"))
	}

	// And closing the connection must return the server to its baseline.
	nc.Close()
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := sA.NumSubscriptions(); n != baseA {
			return fmt.Errorf("server A has %d subscriptions after close, baseline %d", n, baseA)
		}
		if n := expAcc.TotalSubs(); n != baseExp {
			return fmt.Errorf("EXP has %d subs after close, baseline %d", n, baseExp)
		}
		if n := impAcc.TotalSubs(); n != baseImp {
			return fmt.Errorf("IMP has %d subs after close, baseline %d", n, baseImp)
		}
		return nil
	})

}
