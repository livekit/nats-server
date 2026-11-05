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

const permReloadInterestConf = `
	listen: 127.0.0.1:-1
	cluster {
		name: "A"
		listen: 127.0.0.1:-1
	}
	accounts: {
		A: {
			users: [
				{user: u, password: pwd, permissions: {publish: ">", subscribe: "foo"}}
			]
		}
	}
`

// Same config, but the user is no longer allowed to subscribe to "foo", so a
// reload removes the existing subscription.
const permReloadInterestConfDenied = `
	listen: 127.0.0.1:-1
	cluster {
		name: "A"
		listen: 127.0.0.1:-1
	}
	accounts: {
		A: {
			users: [
				{user: u, password: pwd, permissions: {publish: ">", subscribe: "bar"}}
			]
		}
	}
`

// A subscription removed by a permissions reload is unsubscribed (c.subs +
// account sublist) but the route/gateway/leaf interest it registered at SUB
// time is never withdrawn, so the account's remote-interest map keeps a
// phantom entry with no local subscriber.
// permSubs returns the subscriptions matching subj in the account.
func permSubs(a *Account, subj string) []*subscription {
	a.mu.RLock()
	sr := a.sl.Match(subj)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	a.mu.RUnlock()
	return subs
}

func TestConfigReloadPermRemovalWithdrawsRemoteInterest(t *testing.T) {
	s, _, confFile := runReloadServerWithContent(t, []byte(permReloadInterestConf))
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("u", "pwd"))
	defer nc.Close()
	// nats.go reports the -ERR as an async error; keep the connection usable.
	nc.SetErrorHandler(func(*nats.Conn, *nats.Subscription, error) {})

	_, err := nc.SubscribeSync("foo")
	require_NoError(t, err)
	require_NoError(t, nc.Flush())

	acc, err := s.LookupAccount("A")
	require_NoError(t, err)
	// Route-interest key for a plain local subscription.
	key := keyFromSubWithOrigin(&subscription{subject: []byte("foo")})

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		acc.mu.RLock()
		n := acc.rm[key]
		acc.mu.RUnlock()
		if n != 1 {
			return fmt.Errorf("account remote interest for %q is %d, want 1", key, n)
		}
		return nil
	})

	require_NoError(t, os.WriteFile(confFile, []byte(permReloadInterestConfDenied), 0666))
	require_NoError(t, s.Reload())

	// The sub must be gone from the account sublist.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(permSubs(acc, "foo")); n != 0 {
			return fmt.Errorf("still %d subscription(s) for %q after reload", n, key)
		}
		return nil
	})

	// ... and so must the interest it registered.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		acc.mu.RLock()
		n, w := acc.rm[key], acc.lws[key]
		acc.mu.RUnlock()
		if n != 0 || w != 0 {
			return fmt.Errorf("phantom remote interest for %q after reload: rm=%d lws=%d", key, n, w)
		}
		return nil
	})
}
