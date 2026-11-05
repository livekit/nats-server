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
)

// A service import contributes to a new leaf's initial smap twice: once through
// the internal account subscription that addServiceImportSub creates for the
// import's local subject (it is in the sublist snapshot) and once through the
// `ims` loop in initLeafNodeSmapAndSendSubs. A service import added while the
// leaf is already connected contributes once (the delta from
// addServiceImportSub). Removing the import then only withdraws one of the two
// units, so a leaf that connected while the import existed never gets an LS-
// for that subject: it keeps advertising interest in a subject that has none.
func svcImportHub(t *testing.T, withImport bool) (*Server, *client, *Account, *Server, string) {
	t.Helper()
	conf := createConfFile(t, svcImportHubConf(withImport))
	hub, o := RunServerWithConfig(conf)
	t.Cleanup(hub.Shutdown)

	spokeConf := []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: si-spoke
		leaf {
			remotes: [ { url: "nats-leaf://acc:pw@127.0.0.1:%d" } ]
		}
	`, o.LeafNode.Port))
	spoke, _ := RunServerWithConfig(createConfFile(t, spokeConf))
	t.Cleanup(spoke.Shutdown)
	checkLeafNodeConnected(t, spoke)

	lc := leafForRemoteServer(t, hub, "si-spoke")
	if lc == nil {
		t.Fatalf("no hub-side leaf connection")
	}
	acc, err := hub.LookupAccount("ACC")
	if err != nil {
		t.Fatalf("lookup ACC: %v", err)
	}
	return hub, lc, acc, spoke, conf
}

func svcImportHubConf(withImport bool) []byte {
	imp := ""
	if withImport {
		imp = `imports: [ { service: { account: SVC, subject: "svc.dest" } } ]`
	}
	return []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: si-hub
		accounts: {
			ACC: {
				users: [{user: "acc", password: "pw"}]
				%s
			}
			SVC: {
				users: [{user: "svc", password: "pw"}]
				exports: [ { service: "svc.dest" } ]
			}
		}
		leaf {
			listen: 127.0.0.1:-1
			authorization {
				users: [{user: "acc", password: "pw", account: "ACC"}]
			}
		}
	`, imp))
}

// The import's local subject is counted twice: the internal subscription from
// addServiceImportSub is in the sublist snapshot and the `ims` loop adds it
// again.
func TestLeafNodeServiceImportSmapDoubleCount(t *testing.T) {
	_, lc, acc, spoke, _ := svcImportHub(t, true)

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := acc.Interest("svc.dest"); n != 1 {
			return fmt.Errorf("hub interest for svc.dest = %d, want 1", n)
		}
		return nil
	})
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := spoke.globalAccount().Interest("svc.dest"); n != 1 {
			return fmt.Errorf("remote interest for svc.dest = %d, want 1", n)
		}
		return nil
	})

	n, ok := leafSmapValue(lc, "svc.dest")
	if !ok || n != 1 {
		t.Fatalf("hub leaf smap[svc.dest] = %d (present=%v), want 1: the import's sub and the ims entry are both counted", n, ok)
	}
}

// The user-visible consequence: a leaf that connected while the import existed
// never sees an LS- when the import is removed, so it keeps advertising
// interest in the import's local subject.
func TestLeafNodeServiceImportRemovalLeavesInterest(t *testing.T) {
	hub, lc, acc, spoke, conf := svcImportHub(t, true)

	require_NoError(t, os.WriteFile(conf, svcImportHubConf(false), 0600))
	require_NoError(t, hub.Reload())
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := acc.Interest("svc.dest"); n != 0 {
			return fmt.Errorf("hub interest for svc.dest after reload = %d, want 0", n)
		}
		return nil
	})
	// Give any queued LS- time to reach the remote side.
	time.Sleep(100 * time.Millisecond)

	n, _ := leafSmapValue(lc, "svc.dest")
	remoteInterest := spoke.globalAccount().Interest("svc.dest")
	if n != 0 || remoteInterest != 0 {
		t.Fatalf("after removing the import: hub leaf smap[svc.dest] = %d, remote interest = %d, want 0/0 (no LS- was sent: one of the two init units is left over)", n, remoteInterest)
	}
}

// Control: a service import added and removed while the leaf is already
// connected is counted once by each delta and leaves no residue.
func TestLeafNodeServiceImportSmapDeltaControl(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: si-hub
		accounts: {
			ACC: { users: [{user: "acc", password: "pw"}] }
			SVC: {
				users: [{user: "svc", password: "pw"}]
				exports: [ { service: "svc.dest" } ]
			}
		}
		leaf {
			listen: 127.0.0.1:-1
			authorization {
				users: [{user: "acc", password: "pw", account: "ACC"}]
			}
		}
	`))
	hub, o := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	spoke, _ := RunServerWithConfig(createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: si-spoke
		leaf {
			remotes: [ { url: "nats-leaf://acc:pw@127.0.0.1:%d" } ]
		}
	`, o.LeafNode.Port))))
	defer spoke.Shutdown()
	checkLeafNodeConnected(t, spoke)

	lc := leafForRemoteServer(t, hub, "si-spoke")
	if lc == nil {
		t.Fatalf("no hub-side leaf connection")
	}
	acc, err := hub.LookupAccount("ACC")
	if err != nil {
		t.Fatalf("lookup ACC: %v", err)
	}
	svc, err := hub.LookupAccount("SVC")
	if err != nil {
		t.Fatalf("lookup SVC: %v", err)
	}
	if err := acc.AddServiceImport(svc, "svc.dest", "svc.dest"); err != nil {
		t.Fatalf("add service import: %v", err)
	}
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n, ok := leafSmapValue(lc, "svc.dest"); !ok || n != 1 {
			return fmt.Errorf("hub leaf smap[svc.dest] = %d (present=%v), want 1", n, ok)
		}
		return nil
	})

	acc.removeAllServiceImportSubs()
	time.Sleep(100 * time.Millisecond)
	if n, _ := leafSmapValue(lc, "svc.dest"); n != 0 {
		t.Fatalf("after removing a delta-added import: hub leaf smap[svc.dest] = %d, want 0", n)
	}
	if n := spoke.globalAccount().Interest("svc.dest"); n != 0 {
		t.Fatalf("after removing a delta-added import: remote interest = %d, want 0", n)
	}
}
