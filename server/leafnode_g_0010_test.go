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
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
)

// Ticket 0010: a service import's local subject is counted twice in a new
// leaf's initial smap, so removing the import never sends `LS-`.
//
// addServiceImportSub (accounts.go:2283) creates an internal ACCOUNT-client
// subscription on the import's `from` subject. initLeafNodeSmapAndSendSubs
// counts it once through the account sublist walk (leafnode.go:2733-2745) and
// then a second time through the `ims` loop, which appends every service
// import's local subject (leafnode.go:2662-2669) and increments the same key
// again (leafnode.go:2748-2750).
//
// The delta path is different: an import added while the leaf is already
// connected produces one unit (`addServiceImportSub` calls
// `a.updateLeafNodes(sub, 1)`, accounts.go:2312), and removal unsubscribes the
// internal sid, whose `-1` (client.go:3632-3643) takes it back to zero. So a
// leaf that initialised with the import present is left holding a phantom
// unit: 2 -> 1, no `LS-` (updateSmap only sends when the count crosses zero,
// leafnode.go:2928-2940), and the remote keeps advertising interest forever.
//
// The tests assert the desired counts, so they fail on 9bad2e8d for the
// init-then-remove shapes and pass for the delta control and the controls.

const g0010Key = "svc.dest"

const g0010ImportBody = `imports: [ { service: { account: SVC, subject: "svc.dest" } } ]`

// g0010Trace records the hub's outgoing leaf subscription updates so a test can
// report whether `LS-` was actually written for a key.
type g0010Trace struct {
	mu    sync.Mutex
	lines []string
}

func (l *g0010Trace) Noticef(string, ...any) {}
func (l *g0010Trace) Warnf(string, ...any)   {}
func (l *g0010Trace) Errorf(string, ...any)  {}
func (l *g0010Trace) Debugf(string, ...any)  {}

func (l *g0010Trace) Fatalf(format string, v ...any) {
	panic(fmt.Sprintf(format, v...))
}

func (l *g0010Trace) Tracef(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	if !strings.Contains(msg, "LS+") && !strings.Contains(msg, "LS-") {
		return
	}
	l.mu.Lock()
	l.lines = append(l.lines, msg)
	l.mu.Unlock()
}

func (l *g0010Trace) mark() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

func (l *g0010Trace) since(m int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines[m:]...)
}

// g0010HasOp reports whether one of the captured trace lines carries the leaf
// subscription update op ("LS+" or "LS-") for exactly key.
func g0010HasOp(lines []string, op, key string) bool {
	for _, line := range lines {
		if strings.Contains(line, "["+op+" "+key+"]") ||
			strings.Contains(line, "["+op+" "+key+" ") {
			return true
		}
	}
	return false
}

// g0010HubConf builds a hub config. accExtras is spliced into account ACC (an
// import, a mapping, or nothing); withSystem adds a system account, which makes
// nats-server register the default `$SYS.*` service imports on every account.
func g0010HubConf(accExtras string, withSystem bool) []byte {
	var sysAcc, sysLine string
	if withSystem {
		sysAcc = `SYS: { users: [{user: "sys", password: "pw"}] }`
		sysLine = "system_account: SYS"
	}
	return []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: g10-hub
		accounts: {
			ACC: {
				users: [{user: "acc", password: "pw"}]
				%s
			}
			SVC: {
				users: [{user: "svc", password: "pw"}]
				exports: [ { service: "svc.dest" } ]
			}
			%s
		}
		%s
		leaf {
			listen: 127.0.0.1:-1
			authorization {
				users: [{user: "acc", password: "pw", account: "ACC"}]
			}
		}
	`, accExtras, sysAcc, sysLine))
}

func g0010SpokeConf(port int, name string) []byte {
	return []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: %s
		leaf {
			remotes: [ { url: "nats-leaf://acc:pw@127.0.0.1:%d" } ]
		}
	`, name, port))
}

func g0010StartHub(t *testing.T, accExtras string, withSystem bool) (*Server, *Options, string) {
	t.Helper()
	conf := createConfFile(t, g0010HubConf(accExtras, withSystem))
	hub, o := RunServerWithConfig(conf)
	t.Cleanup(hub.Shutdown)
	return hub, o, conf
}

// g0010StartSpoke starts a spoke that solicits the hub and binds (on the hub
// side) to ACC through the leaf listener's user entry.
func g0010StartSpoke(t *testing.T, port int, name string) *Server {
	t.Helper()
	spoke, _ := RunServerWithConfig(createConfFile(t, g0010SpokeConf(port, name)))
	checkLeafNodeConnected(t, spoke)
	return spoke
}

// g0010Leaf returns the hub-side leaf connection for the given remote server
// name, waiting for it to appear.
func g0010Leaf(t *testing.T, hub *Server, name string) *client {
	t.Helper()
	var lc *client
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		for _, cand := range hub.leafs {
			cand.mu.Lock()
			remote := cand.leaf.remoteServer
			cand.mu.Unlock()
			if remote == name {
				lc = cand
				return nil
			}
		}
		return fmt.Errorf("hub has no leaf connection for %q yet", name)
	})
	return lc
}

func g0010Smap(lc *client, key string) (int64, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.leaf.smap == nil {
		return 0, false
	}
	n, ok := lc.leaf.smap[key]
	return n, ok
}

func g0010SmapDump(lc *client) map[string]int64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	out := make(map[string]int64, len(lc.leaf.smap))
	for k, v := range lc.leaf.smap {
		out[k] = v
	}
	return out
}

// g0010BindingAccount returns the account name the hub bound this leaf
// connection to, so a test can prove it did not land in $G.
func g0010BindingAccount(lc *client) string {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.acc == nil {
		return ""
	}
	return lc.acc.Name
}

// g0010MatchDetail lists the subscriptions of acc that match subject, so a
// test can explain Interest() when a wildcard sub is also present.
func g0010MatchDetail(acc *Account, subject string) []string {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	if acc.sl == nil {
		return nil
	}
	res := acc.sl.Match(subject)
	var out []string
	for _, sub := range res.psubs {
		out = append(out, fmt.Sprintf("plain %q kind=%d", sub.subject, sub.client.kind))
	}
	for _, group := range res.qsubs {
		for _, sub := range group {
			out = append(out, fmt.Sprintf("queue %q group=%q", sub.subject, sub.queue))
		}
	}
	return out
}

func g0010Wait(t *testing.T, what string, f func() error) {
	t.Helper()
	checkFor(t, 5*time.Second, 10*time.Millisecond, f)
}

func g0010WaitInterest(t *testing.T, acc *Account, subject string, want int) {
	t.Helper()
	g0010Wait(t, fmt.Sprintf("%s interest for %q to be %d", acc.Name, subject, want), func() error {
		if n := acc.Interest(subject); n != want {
			return fmt.Errorf("%s.Interest(%q) = %d, want %d", acc.Name, subject, n, want)
		}
		return nil
	})
}

func g0010WaitSmapPresent(t *testing.T, lc *client, key string) {
	t.Helper()
	g0010Wait(t, fmt.Sprintf("leaf smap to contain %q", key), func() error {
		if _, ok := g0010Smap(lc, key); !ok {
			return fmt.Errorf("leaf smap has no entry for %q yet", key)
		}
		return nil
	})
}

// g0010Note logs the observed state of every observable the ticket asks for.
func g0010Note(t *testing.T, path string, lc *client, acc *Account, spoke *Server, key string, lines []string) {
	t.Helper()
	n, ok := g0010Smap(lc, key)
	remote := 0
	if spoke != nil {
		remote = spoke.globalAccount().Interest(key)
	}
	t.Logf("%s: leaf bound to %q, hub smap[%q] = %d (present=%v), %s.Interest = %d, spoke interest = %d, LS+ sent = %v, LS- sent = %v",
		path, g0010BindingAccount(lc), key, n, ok, acc.Name, acc.Interest(key), remote,
		g0010HasOp(lines, "LS+", key), g0010HasOp(lines, "LS-", key))
}

// TestG0010Path1InitCountsImportTwice is the primary reach path: the leaf
// initialises while the service import exists, so both the internal sub and the
// `ims` entry are counted. Desired smap is 1 (one real subscription of
// interest), observed on main is 2.
func TestG0010Path1InitCountsImportTwice(t *testing.T) {
	hub, o, _ := g0010StartHub(t, g0010ImportBody, false)
	acc, err := hub.LookupAccount("ACC")
	require_NoError(t, err)

	// The import's internal subscription is the account's only interest in
	// the local subject before any leaf connects.
	g0010WaitInterest(t, acc, g0010Key, 1)

	tl := &g0010Trace{}
	hub.SetLoggerV2(tl, false, true, false)
	mark := tl.mark()

	spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p1")
	defer spoke.Shutdown()
	lc := g0010Leaf(t, hub, "g10-p1")
	g0010WaitInterest(t, spoke.globalAccount(), g0010Key, 1)
	g0010WaitSmapPresent(t, lc, g0010Key)
	time.Sleep(50 * time.Millisecond)

	lines := tl.since(mark)
	g0010Note(t, "path1", lc, acc, spoke, g0010Key, lines)

	if got := g0010BindingAccount(lc); got != "ACC" {
		t.Fatalf("leaf bound to %q, want ACC (the config uses the leaf user's account entry)", got)
	}
	n, _ := g0010Smap(lc, g0010Key)
	if n != 1 {
		t.Fatalf("hub leaf smap[%q] = %d, want 1: the import's internal sub and the ims entry are both counted", g0010Key, n)
	}
}

// TestG0010Path2ReloadRemovalLeavesPhantomInterest is the user-visible damage:
// after the import is removed by a reload, the hub has no interest left but the
// leaf still advertises one and no `LS-` was sent. Desired 0/0.
func TestG0010Path2ReloadRemovalLeavesPhantomInterest(t *testing.T) {
	hub, o, conf := g0010StartHub(t, g0010ImportBody, false)
	acc, err := hub.LookupAccount("ACC")
	require_NoError(t, err)
	g0010WaitInterest(t, acc, g0010Key, 1)

	tl := &g0010Trace{}
	hub.SetLoggerV2(tl, false, true, false)

	spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p2")
	defer spoke.Shutdown()
	lc := g0010Leaf(t, hub, "g10-p2")
	g0010WaitInterest(t, spoke.globalAccount(), g0010Key, 1)
	g0010WaitSmapPresent(t, lc, g0010Key)
	time.Sleep(50 * time.Millisecond)
	pre, _ := g0010Smap(lc, g0010Key)
	mark := tl.mark()

	require_NoError(t, os.WriteFile(conf, g0010HubConf("", false), 0600))
	require_NoError(t, hub.Reload())
	g0010WaitInterest(t, acc, g0010Key, 0)
	time.Sleep(150 * time.Millisecond) // let any LS- reach the remote side

	lines := tl.since(mark)
	g0010Note(t, "path2", lc, acc, spoke, g0010Key, lines)
	t.Logf("path2: smap before removal = %d", pre)

	n, _ := g0010Smap(lc, g0010Key)
	remote := spoke.globalAccount().Interest(g0010Key)
	if n != 0 || remote != 0 {
		t.Fatalf("after reloading the import away: hub leaf smap[%q] = %d, remote interest = %d, want 0/0 (the ims unit is left over, so no LS- is sent)", g0010Key, n, remote)
	}
	if !g0010HasOp(lines, "LS-", g0010Key) {
		t.Fatalf("no LS- for %q after removing the import", g0010Key)
	}
}

// TestG0010Path3DeltaControl must PASS: an import added and removed while the
// leaf is already connected is counted once by each delta and leaves no
// residue. It proves the failure is the init-vs-delta asymmetry.
func TestG0010Path3DeltaControl(t *testing.T) {
	hub, o, _ := g0010StartHub(t, "", false)
	acc, err := hub.LookupAccount("ACC")
	require_NoError(t, err)
	svc, err := hub.LookupAccount("SVC")
	require_NoError(t, err)
	if n := acc.Interest(g0010Key); n != 0 {
		t.Fatalf("ACC already has interest for %q before the import: %d", g0010Key, n)
	}

	tl := &g0010Trace{}
	hub.SetLoggerV2(tl, false, true, false)

	spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p3")
	defer spoke.Shutdown()
	lc := g0010Leaf(t, hub, "g10-p3")
	if n, ok := g0010Smap(lc, g0010Key); ok {
		t.Fatalf("leaf smap already has %q = %d before the import exists", g0010Key, n)
	}
	mark := tl.mark()

	require_NoError(t, acc.AddServiceImport(svc, g0010Key, g0010Key))
	g0010WaitInterest(t, acc, g0010Key, 1)
	g0010Wait(t, "leaf smap to reach 1 after the delta add", func() error {
		if n, ok := g0010Smap(lc, g0010Key); !ok || n != 1 {
			return fmt.Errorf("leaf smap[%q] = %d (present=%v), want 1", g0010Key, n, ok)
		}
		return nil
	})
	g0010WaitInterest(t, spoke.globalAccount(), g0010Key, 1)
	afterAdd, _ := g0010Smap(lc, g0010Key)

	acc.removeAllServiceImportSubs()
	g0010WaitInterest(t, acc, g0010Key, 0)
	time.Sleep(150 * time.Millisecond)
	g0010Wait(t, "leaf smap to drop the key after the delta remove", func() error {
		if n, _ := g0010Smap(lc, g0010Key); n != 0 {
			return fmt.Errorf("leaf smap[%q] = %d, want 0", g0010Key, n)
		}
		return nil
	})
	g0010WaitInterest(t, spoke.globalAccount(), g0010Key, 0)

	lines := tl.since(mark)
	afterRemoval, _ := g0010Smap(lc, g0010Key)
	t.Logf("path3: delta add smap = %d, after remove smap = %d, remote = %d; LS+ sent = %v, LS- sent = %v",
		afterAdd, afterRemoval, spoke.globalAccount().Interest(g0010Key),
		g0010HasOp(lines, "LS+", g0010Key), g0010HasOp(lines, "LS-", g0010Key))
	if afterAdd != 1 || afterRemoval != 0 {
		t.Fatalf("delta control counts went 0 -> %d -> %d, want 0 -> 1 -> 0", afterAdd, afterRemoval)
	}
	if !g0010HasOp(lines, "LS-", g0010Key) {
		t.Fatalf("delta control did not send LS- for %q", g0010Key)
	}
}

// TestG0010Path4DefaultSystemImports shows the bug needs no custom config: a
// server with a system account gives every account four `$SYS.*` service
// imports, and a new leaf counts each of them twice.
func TestG0010Path4DefaultSystemImports(t *testing.T) {
	hub, o, _ := g0010StartHub(t, "", true)
	acc, err := hub.LookupAccount("ACC")
	require_NoError(t, err)

	sysKeys := []string{
		"$SYS.REQ.ACCOUNT.PING.CONNZ",
		"$SYS.REQ.SERVER.PING.CONNZ",
		"$SYS.REQ.ACCOUNT.PING.STATZ",
		"$SYS.REQ.USER.INFO",
	}
	// Wait until the account has all four internal import subscriptions, so
	// the leaf's init snapshot is guaranteed to include them.
	for _, key := range sysKeys {
		g0010WaitInterest(t, acc, key, 1)
	}

	spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p4")
	defer spoke.Shutdown()
	lc := g0010Leaf(t, hub, "g10-p4")
	for _, key := range sysKeys {
		g0010Wait(t, "leaf smap to contain "+key, func() error {
			if _, ok := g0010Smap(lc, key); !ok {
				return fmt.Errorf("leaf smap has no entry for %q yet", key)
			}
			return nil
		})
	}
	time.Sleep(50 * time.Millisecond)

	dump := g0010SmapDump(lc)
	t.Logf("path4: leaf bound to %q, hub leaf smap dump (%d entries): %v", g0010BindingAccount(lc), len(dump), dump)

	if got := g0010BindingAccount(lc); got != "ACC" {
		t.Fatalf("leaf bound to %q, want ACC", got)
	}
	for _, key := range sysKeys {
		t.Logf("path4: matching subs of ACC for %q: %v", key, g0010MatchDetail(acc, key))
		t.Logf("path4: matching subs of the spoke $G for %q: %v", key, g0010MatchDetail(spoke.globalAccount(), key))
		if n := dump[key]; n != 1 {
			t.Errorf("hub leaf smap[%q] = %d, want 1 (default system service import counted once; ACC matching interest = %d)", key, n, acc.Interest(key))
		}
		// The spoke also auto-creates a system account and registers the same
		// default imports in its $G, so its raw interest is (import sub + the
		// leaf sub the hub advertised) and is not the oracle here; the hub's
		// smap is. Log it for the record.
		if remote := spoke.globalAccount().Interest(key); remote < 1 {
			t.Errorf("spoke interest for %q = %d, want at least 1 (the hub advertises the import)", key, remote)
		}
	}
}

// TestG0010Path5ReAddAndReconnect proves the phantom unit persists across a
// re-add and a leaf reconnect, and that removing the import again still does
// not send `LS-`. Desired counts are 1 after each init and 0 after removal.
func TestG0010Path5ReAddAndReconnect(t *testing.T) {
	hub, o, conf := g0010StartHub(t, g0010ImportBody, false)
	acc, err := hub.LookupAccount("ACC")
	require_NoError(t, err)
	g0010WaitInterest(t, acc, g0010Key, 1)

	tl := &g0010Trace{}
	hub.SetLoggerV2(tl, false, true, false)

	spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p5a")
	lc := g0010Leaf(t, hub, "g10-p5a")
	g0010WaitInterest(t, spoke.globalAccount(), g0010Key, 1)
	g0010WaitSmapPresent(t, lc, g0010Key)
	time.Sleep(50 * time.Millisecond)
	first, _ := g0010Smap(lc, g0010Key)
	t.Logf("path5: initial smap = %d (want 1, main: 2)", first)

	// Remove the import by reload; the hub drops to no interest and the leaf
	// keeps the phantom unit.
	require_NoError(t, os.WriteFile(conf, g0010HubConf("", false), 0600))
	require_NoError(t, hub.Reload())
	g0010WaitInterest(t, acc, g0010Key, 0)
	time.Sleep(100 * time.Millisecond)
	afterRemove, _ := g0010Smap(lc, g0010Key)
	remoteAfterRemove := spoke.globalAccount().Interest(g0010Key)
	t.Logf("path5: after reload removal smap = %d, remote = %d (want 0/0)", afterRemove, remoteAfterRemove)

	// Re-add it. The delta add is correct, but it lands on the phantom unit.
	require_NoError(t, os.WriteFile(conf, g0010HubConf(g0010ImportBody, false), 0600))
	require_NoError(t, hub.Reload())
	g0010WaitInterest(t, acc, g0010Key, 1)
	g0010Wait(t, "smap after re-add", func() error {
		if n, ok := g0010Smap(lc, g0010Key); !ok || n == 0 {
			return fmt.Errorf("leaf smap[%q] = %d (present=%v), want > 0", g0010Key, n, ok)
		}
		return nil
	})
	afterReAdd, _ := g0010Smap(lc, g0010Key)
	t.Logf("path5: after re-add smap = %d (want 1)", afterReAdd)

	// Reconnect the leaf: init with the import present counts it twice again.
	spoke.Shutdown()
	spoke.WaitForShutdown()
	spoke2 := g0010StartSpoke(t, o.LeafNode.Port, "g10-p5b")
	defer spoke2.Shutdown()
	lc2 := g0010Leaf(t, hub, "g10-p5b")
	g0010WaitInterest(t, spoke2.globalAccount(), g0010Key, 1)
	g0010WaitSmapPresent(t, lc2, g0010Key)
	time.Sleep(50 * time.Millisecond)
	mark := tl.mark()

	n2, _ := g0010Smap(lc2, g0010Key)
	g0010Note(t, "path5/reconnect", lc2, acc, spoke2, g0010Key, tl.since(mark))
	if n2 != 1 {
		t.Errorf("after reconnect with the import re-added: hub leaf smap[%q] = %d, want 1", g0010Key, n2)
	}

	// And the final removal still leaves the phantom interest behind.
	acc.removeAllServiceImportSubs()
	g0010WaitInterest(t, acc, g0010Key, 0)
	time.Sleep(150 * time.Millisecond)
	lines := tl.since(mark)
	g0010Note(t, "path5/final-removal", lc2, acc, spoke2, g0010Key, lines)
	nFinal, _ := g0010Smap(lc2, g0010Key)
	remoteFinal := spoke2.globalAccount().Interest(g0010Key)
	if nFinal != 0 || remoteFinal != 0 {
		t.Errorf("after removing the re-added import: hub leaf smap[%q] = %d, remote = %d, want 0/0", g0010Key, nFinal, remoteFinal)
	}
	if !g0010HasOp(lines, "LS-", g0010Key) {
		t.Errorf("no LS- for %q after removing the re-added import", g0010Key)
	}
}

// TestG0010Path6ClaimsRebuild drives the account-claims rebuild path directly
// (updateAccountClaimsWithRefresh, accounts.go:3500; the import sub replacement
// is at accounts.go:3851-3867). A claims refresh that keeps the same import
// should leave the leaf count at 1; a refresh without imports should drop it to
// 0 and send `LS-`.
func TestG0010Path6ClaimsRebuild(t *testing.T) {
	hub, o, _ := g0010StartHub(t, g0010ImportBody, false)
	acc, err := hub.LookupAccount("ACC")
	require_NoError(t, err)
	g0010WaitInterest(t, acc, g0010Key, 1)

	tl := &g0010Trace{}
	hub.SetLoggerV2(tl, false, true, false)

	spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p6")
	defer spoke.Shutdown()
	lc := g0010Leaf(t, hub, "g10-p6")
	g0010WaitInterest(t, spoke.globalAccount(), g0010Key, 1)
	g0010WaitSmapPresent(t, lc, g0010Key)
	time.Sleep(50 * time.Millisecond)
	base, _ := g0010Smap(lc, g0010Key)

	// Refresh with the same import.
	withImport := jwt.NewAccountClaims("ACC")
	withImport.Imports.Add(&jwt.Import{Account: "SVC", Subject: g0010Key, Type: jwt.Service})
	hub.updateAccountClaimsWithRefresh(acc, withImport, false)
	g0010WaitInterest(t, acc, g0010Key, 1)
	g0010Wait(t, "smap after claims refresh with the import", func() error {
		if n, ok := g0010Smap(lc, g0010Key); !ok || n == 0 {
			return fmt.Errorf("leaf smap[%q] = %d (present=%v), want > 0", g0010Key, n, ok)
		}
		return nil
	})
	afterRefresh, _ := g0010Smap(lc, g0010Key)
	t.Logf("path6: smap before = %d, after claims refresh with the same import = %d (want 1); %s.Interest = %d, remote = %d",
		base, afterRefresh, acc.Name, acc.Interest(g0010Key), spoke.globalAccount().Interest(g0010Key))
	if afterRefresh != 1 {
		t.Errorf("after a claims refresh that keeps the import: hub leaf smap[%q] = %d, want 1", g0010Key, afterRefresh)
	}

	// Refresh without imports: this is the claims-side removal.
	mark := tl.mark()
	noImport := jwt.NewAccountClaims("ACC")
	hub.updateAccountClaimsWithRefresh(acc, noImport, false)
	g0010WaitInterest(t, acc, g0010Key, 0)
	time.Sleep(150 * time.Millisecond)
	lines := tl.since(mark)
	g0010Note(t, "path6/removal", lc, acc, spoke, g0010Key, lines)
	n, _ := g0010Smap(lc, g0010Key)
	remote := spoke.globalAccount().Interest(g0010Key)
	if n != 0 || remote != 0 {
		t.Fatalf("after a claims refresh without the import: hub leaf smap[%q] = %d, remote = %d, want 0/0", g0010Key, n, remote)
	}
	if !g0010HasOp(lines, "LS-", g0010Key) {
		t.Fatalf("no LS- for %q after the claims-side removal", g0010Key)
	}
}

// TestG0010Path7DirectRemoveAll is path 2 without the reload machinery: the
// same accounting error on a direct removeAllServiceImportSubs call.
func TestG0010Path7DirectRemoveAll(t *testing.T) {
	hub, o, _ := g0010StartHub(t, g0010ImportBody, false)
	acc, err := hub.LookupAccount("ACC")
	require_NoError(t, err)
	g0010WaitInterest(t, acc, g0010Key, 1)

	tl := &g0010Trace{}
	hub.SetLoggerV2(tl, false, true, false)

	spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p7")
	defer spoke.Shutdown()
	lc := g0010Leaf(t, hub, "g10-p7")
	g0010WaitInterest(t, spoke.globalAccount(), g0010Key, 1)
	g0010WaitSmapPresent(t, lc, g0010Key)
	time.Sleep(50 * time.Millisecond)
	before, _ := g0010Smap(lc, g0010Key)
	mark := tl.mark()

	acc.removeAllServiceImportSubs()
	g0010WaitInterest(t, acc, g0010Key, 0)
	time.Sleep(150 * time.Millisecond)

	lines := tl.since(mark)
	g0010Note(t, "path7", lc, acc, spoke, g0010Key, lines)
	t.Logf("path7: smap before direct removal = %d", before)

	n, _ := g0010Smap(lc, g0010Key)
	remote := spoke.globalAccount().Interest(g0010Key)
	if n != 0 || remote != 0 {
		t.Fatalf("after removeAllServiceImportSubs: hub leaf smap[%q] = %d, remote = %d, want 0/0", g0010Key, n, remote)
	}
	if !g0010HasOp(lines, "LS-", g0010Key) {
		t.Fatalf("no LS- for %q after removeAllServiceImportSubs", g0010Key)
	}
}

// TestG0010Path9Controls checks the two accounting controls:
//
//	(a) an account with no imports counts a real local subscription once;
//	(b) a mapping-only account is not conflated with the service-import double
//	    count: the mapping's source is contributed once by the ims loop, not
//	    twice.
//
// Neither control asserts the mapping removal path, which belongs to ticket
// 0002 (the mapping-source asymmetry in the same ims loop).
func TestG0010Path9Controls(t *testing.T) {
	t.Run("no imports counts a real sub once", func(t *testing.T) {
		hub, o, _ := g0010StartHub(t, "", false)
		acc, err := hub.LookupAccount("ACC")
		require_NoError(t, err)

		nc := natsConnect(t, hub.ClientURL(), nats.UserInfo("acc", "pw"))
		defer nc.Close()
		natsSubSync(t, nc, "plain.subj")
		natsFlush(t, nc)
		g0010WaitInterest(t, acc, "plain.subj", 1)

		spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p9a")
		defer spoke.Shutdown()
		lc := g0010Leaf(t, hub, "g10-p9a")
		g0010WaitInterest(t, spoke.globalAccount(), "plain.subj", 1)
		g0010WaitSmapPresent(t, lc, "plain.subj")
		time.Sleep(50 * time.Millisecond)

		n, _ := g0010Smap(lc, "plain.subj")
		t.Logf("control a: leaf bound to %q, smap[plain.subj] = %d, hub interest = %d, remote = %d (want 1)",
			g0010BindingAccount(lc), n, acc.Interest("plain.subj"), spoke.globalAccount().Interest("plain.subj"))
		if n != 1 {
			t.Fatalf("smap[plain.subj] = %d, want 1 (a real subscription is counted once)", n)
		}
	})

	t.Run("mapping source is contributed once", func(t *testing.T) {
		hub, o, _ := g0010StartHub(t, `mappings: { "m.src": "m.dst" }`, false)
		acc, err := hub.LookupAccount("ACC")
		require_NoError(t, err)
		g0010WaitInterest(t, acc, "m.src", 0) // mappings have no account sub

		spoke := g0010StartSpoke(t, o.LeafNode.Port, "g10-p9b")
		defer spoke.Shutdown()
		lc := g0010Leaf(t, hub, "g10-p9b")
		g0010WaitInterest(t, spoke.globalAccount(), "m.src", 1)
		g0010WaitSmapPresent(t, lc, "m.src")
		time.Sleep(50 * time.Millisecond)

		n, _ := g0010Smap(lc, "m.src")
		t.Logf("control b: leaf bound to %q, smap[m.src] = %d, hub interest = %d, remote = %d (want 1, not 2)",
			g0010BindingAccount(lc), n, acc.Interest("m.src"), spoke.globalAccount().Interest("m.src"))
		if n != 1 {
			t.Fatalf("smap[m.src] = %d, want 1: a mapping contributes one ims unit and has no sublist sub", n)
		}
	})
}
