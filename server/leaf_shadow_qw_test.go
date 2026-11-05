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
)

// A leafnode advertises a queue subscription with its aggregated weight in a
// single LS+ line (e.g. weight 3 for three members). processLeafSub records
// that weight in the importing account's route map (rm) and leaf smap, but
// addShadowSub only announces +1 for the stream-import shadow in the exporting
// account. Route/leaf peers therefore see weight 1 for the imported subject.
func TestStreamImportShadowQueueWeight(t *testing.T) {
	hubConf := `
		port: -1
		cluster { listen: "127.0.0.1:-1" }
		leafnodes {
			listen: "127.0.0.1:-1"
			authorization {
				users = [{user: "leaf", password: "pwd", account: "IMP"}]
			}
		}
		accounts {
			EXP {
				users = [{user: "exp", password: "pwd"}]
				exports = [{stream: "foo.>"}]
			}
			IMP {
				users = [{user: "imp", password: "pwd"}]
				imports = [{stream: {account: EXP, subject: "foo.>"}}]
			}
		}
	`
	hub, hubOpts := RunServerWithConfig(createConfFile(t, []byte(hubConf)))
	defer hub.Shutdown()

	spokeConf := fmt.Sprintf(`
		port: -1
		leafnodes {
			remotes = [{url: "nats-leaf://leaf:pwd@%s:%d", account: "IMP"}]
		}
		accounts {
			IMP { users = [{user: "imp", password: "pwd"}] }
		}
	`, hubOpts.LeafNode.Host, hubOpts.LeafNode.Port)
	spoke, spokeOpts := RunServerWithConfig(createConfFile(t, []byte(spokeConf)))
	defer spoke.Shutdown()

	checkLeafNodeConnected(t, hub)

	// Three queue members on the spoke aggregate into one LS+ with weight 3.
	url := fmt.Sprintf("nats://imp:pwd@%s:%d", spokeOpts.Host, spokeOpts.Port)
	for i := 0; i < 3; i++ {
		nc := natsConnect(t, url)
		defer nc.Close()
		if _, err := nc.QueueSubscribeSync("foo.bar", "q"); err != nil {
			t.Fatalf("Error subscribing: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("Error flushing: %v", err)
		}
	}

	expAcc, err := hub.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := hub.LookupAccount("IMP")
	require_NoError(t, err)

	// Wait until the hub has the leaf queue sub and the shadow in EXP.
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		if n := len(matchAllSubsTest(expAcc, "foo.bar")); n != 1 {
			return fmt.Errorf("EXP has %d subs for foo.bar, want 1 shadow", n)
		}
		return nil
	})

	impAcc.mu.RLock()
	impRM := impAcc.rm["N foo.bar q"]
	impAcc.mu.RUnlock()
	expAcc.mu.RLock()
	expRM := expAcc.rm["N foo.bar q"]
	expAcc.mu.RUnlock()

	res := expAcc.sl.Match("foo.bar")
	var shadowQw int32
	for _, qsubs := range res.qsubs {
		for _, sub := range qsubs {
			shadowQw = sub.qw
		}
	}

	t.Logf("IMP rm[N foo.bar q]=%d; EXP rm[N foo.bar q]=%d; shadow qw=%d", impRM, expRM, shadowQw)

	if impRM != 3 {
		t.Fatalf("importing account has queue weight %d, want 3", impRM)
	}
	if expRM != 3 {
		t.Fatalf("exporting account shadow queue weight %d, want 3 (shadow qw=%d)", expRM, shadowQw)
	}
	if shadowQw != 3 {
		t.Fatalf("shadow sub qw=%d, want 3", shadowQw)
	}
}

func matchAllSubsTest(a *Account, subject string) []*subscription {
	a.mu.RLock()
	sr := a.sl.Match(subject)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	a.mu.RUnlock()
	return subs
}
