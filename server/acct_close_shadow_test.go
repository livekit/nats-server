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

const acctCloseShadowConf = `
	listen: 127.0.0.1:-1
	cluster {
		name: "A"
		listen: 127.0.0.1:-1
	}
	accounts: {
		C: {
			users = [{user: "c", password: "pwd"}]
			exports = [{stream: "_R_.>"}]
		}
		SRV: {
			users = [{user: "srv", password: "pwd"}]
			exports = [{service: "svc"}]
			imports = [{stream: {account: "C", subject: "_R_.>"}}]
		}
		IMP: {
			users = [{user: "imp", password: "pwd"}]
			imports = [{service: {account: "SRV", subject: "svc"}}]
		}
	}
`

// When an account's internal client is closed (account removed from an
// operator JWT directory, or deleted on a config reload), the server only
// unsubscribes the service-import sids and then closes the client. ACCOUNT
// clients are excluded from clearAccountSubs in closeConnection, so any other
// internal subscription is never unsubscribed: here the lazily created
// service-import reply wildcard ("_R_.<rand>.>") of the service exporter. Its
// stream-import shadow stays in the exporting account's sublist and its
// announcement stays in the route/leaf interest maps forever.
func TestAccountClientCloseLeavesShadowInExportingAccount(t *testing.T) {
	s, _ := RunServerWithConfig(createConfFile(t, []byte(acctCloseShadowConf)))
	defer s.Shutdown()

	cAcc, err := s.LookupAccount("C")
	require_NoError(t, err)
	srvAcc, err := s.LookupAccount("SRV")
	require_NoError(t, err)

	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()

	// A request through the service import forces the *service exporter* to
	// create its per-account reply wildcard on its internal account client
	// (newServiceReply -> subscribeServiceImportResponse, on osi.acc).
	ncImp.Request("svc", []byte("hi"), 100*time.Millisecond)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		srvAcc.mu.RLock()
		siReply := srvAcc.siReply
		srvAcc.mu.RUnlock()
		if siReply == nil {
			return fmt.Errorf("account reply wildcard not created")
		}
		if n := len(acctCloseSubs(cAcc, string(siReply)+">")); n == 0 {
			return fmt.Errorf("C has no shadow for %q", string(siReply)+">")
		}
		return nil
	})

	srvAcc.mu.RLock()
	siReply := string(srvAcc.siReply)
	ic := srvAcc.ic
	srvAcc.mu.RUnlock()
	t.Logf("reply wildcard %q, internal client kind=%v", siReply, ic.kind)

	// Exactly what removeCb() (account removed from the JWT resolver) and the
	// config-reload deleted-account path do.
	srvAcc.removeAllServiceImportSubs()

	cAcc.mu.RLock()
	nshadow := len(acctCloseSubsLocked(cAcc, siReply+">"))
	var keys []string
	for k := range cAcc.rm {
		if strings.Contains(k, "_R_.") {
			keys = append(keys, fmt.Sprintf("%s=%d", k, cAcc.rm[k]))
		}
	}
	cAcc.mu.RUnlock()

	t.Logf("after internal client close: C shadow(s)=%d, rm=%v", nshadow, keys)
	if nshadow != 0 {
		t.Fatalf("C still has %d shadow sub(s) for the closed account's internal sub", nshadow)
	}
	if len(keys) != 0 {
		t.Fatalf("C still announces interest for the closed account's internal sub: %v", keys)
	}
}

func acctCloseSubs(a *Account, subject string) []*subscription {
	a.mu.RLock()
	subs := acctCloseSubsLocked(a, subject)
	a.mu.RUnlock()
	return subs
}

func acctCloseSubsLocked(a *Account, subject string) []*subscription {
	sr := a.sl.Match(subject)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	return subs
}
