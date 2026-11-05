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

const acctShadowInterestConf = `
	listen: 127.0.0.1:-1
	cluster {
		name: "A"
		listen: 127.0.0.1:-1
	}
	accounts: {
		EXP: {
			exports = [{stream: "foo.>"}]
		}
		IMP: {
			imports = [{stream: {account: EXP, subject: "foo.>"}}]
		}
	}
`

// An internal account-scoped (ACCOUNT kind) subscription that matches a stream
// import creates a shadow in the exporting account and announces its interest
// there. On unsubscribe, ACCOUNT clients are not in the kind set that
// withdraws the shadow's route/gateway/leaf interest, so the exporting
// account keeps the announcement with no shadow.
func TestAccountInternalSubShadowUnsubLeavesInterest(t *testing.T) {
	s, _ := RunServerWithConfig(createConfFile(t, []byte(acctShadowInterestConf)))
	defer s.Shutdown()

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := s.LookupAccount("IMP")
	require_NoError(t, err)

	impAcc.mu.Lock()
	ic := impAcc.internalClient()
	impAcc.mu.Unlock()
	require_True(t, ic != nil)
	require_Equal(t, ic.kind, ACCOUNT)

	cb := func(_ *subscription, _ *client, _ *Account, _, _ string, _ []byte) {}
	sub, err := ic.processSubEx([]byte("foo.bar"), nil, []byte("r1acct"), cb, false, false, false)
	require_NoError(t, err)
	require_True(t, sub != nil)

	key := keyFromSubWithOrigin(&subscription{subject: []byte("foo.bar")})
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		expAcc.mu.RLock()
		n := expAcc.rm[key]
		expAcc.mu.RUnlock()
		if n != 1 {
			return fmt.Errorf("EXP remote interest for %q is %d, want 1", key, n)
		}
		if len(acctShadowSubs(expAcc, "foo.bar")) != 1 {
			return fmt.Errorf("EXP has no shadow for foo.bar")
		}
		return nil
	})

	require_NoError(t, ic.processUnsub([]byte("r1acct")))

	// The shadow and the primary are gone ...
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(acctShadowSubs(expAcc, "foo.bar")); n != 0 {
			return fmt.Errorf("EXP still has %d shadow sub(s)", n)
		}
		return nil
	})

	// ... but the interest the shadow announced in EXP never came back down.
	expAcc.mu.RLock()
	n := expAcc.rm[key]
	lws := expAcc.lws[key]
	expAcc.mu.RUnlock()
	t.Logf("after internal UNSUB: EXP rm[%q]=%d lws=%d", key, n, lws)
	if n != 0 || lws != 0 {
		t.Fatalf("phantom shadow interest in EXP for %q: rm=%d lws=%d", key, n, lws)
	}
}

func acctShadowSubs(a *Account, subject string) []*subscription {
	a.mu.RLock()
	sr := a.sl.Match(subject)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	a.mu.RUnlock()
	return subs
}
