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

// During a config reload, configureAccounts copies each account's imports from
// the Options account (shallowCopy) and only rebinds their exporting account to
// the registered account in a later pass. Between those two points the
// published imports point at Options accounts, which have no sublist. A SUB
// processed in that window reaches addShadowSub and panics on the nil sublist.
// Filler accounts widen the window while the reload walks its account map.
func TestSubscribeDuringReloadNilSublist(t *testing.T) {
	const fillers = 120
	var sb strings.Builder
	sb.WriteString(`
		listen: 127.0.0.1:-1
		cluster {
			name: "A"
			listen: 127.0.0.1:-1
		}
		accounts: {
			EXP: {
				users = [{user: "exp", password: "pwd"}]
				exports = [{stream: "foo.>"}]
			}
			IMP: {
				users = [{user: "imp", password: "pwd"}]
				imports = [{stream: {account: "EXP", subject: "foo.>"}, prefix: "imp."}]
			}
	`)
	for i := 0; i < fillers; i++ {
		fmt.Fprintf(&sb, "A%03d { users = [{user: \"u%03d\", password: \"pwd\"}] }\n", i, i)
	}
	sb.WriteString("}\n")

	s, _ := RunServerWithConfig(createConfFile(t, []byte(sb.String())))
	defer s.Shutdown()

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer nc.Close()

	// Churn subscriptions in IMP while the account map is reloaded.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			sub, err := nc.SubscribeSync("imp.foo.bar")
			if err != nil {
				return
			}
			nc.Flush()
			sub.Unsubscribe()
			nc.Flush()
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for i := 0; i < 500 && time.Now().Before(deadline); i++ {
		require_NoError(t, s.Reload())
	}
	close(stop)
	<-done
	require_NoError(t, nc.Flush())
	nc.Close()

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		expAcc.mu.RLock()
		var subs []*subscription
		sr := expAcc.sl.Match("foo.bar")
		subs = append(subs, sr.psubs...)
		for _, qs := range sr.qsubs {
			subs = append(subs, qs...)
		}
		expAcc.mu.RUnlock()
		if len(subs) != 0 {
			return fmt.Errorf("EXP has %d leftover shadow(s) for foo.bar", len(subs))
		}
		return nil
	})
}
