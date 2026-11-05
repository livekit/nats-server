package server

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const acctCloseShadowTmpl = `
	port: -1
	no_sys_acc: true
	cluster {
		name: "A"
		listen: 127.0.0.1:-1
%s	}
	accounts {
		C {
			users: [{user: c, password: pwd}]
			exports: [{stream: "_R_.>"}]
		}
		SRV {
			users: [{user: srv, password: pwd}]
			exports: [{service: "svc"}]
			imports: [{stream: {account: C, subject: "_R_.>"}}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [{service: {account: SRV, subject: "svc"}}]
		}
	}
`

// acctCloseShadowState is what the exporting account C holds for one internal
// sub: the shadows in its sublist and its announced remote interest.
type acctCloseShadowState struct {
	Shadows int
	Sids    []string
	RM      map[string]int64
	LWS     map[string]bool
}

func (st acctCloseShadowState) String() string {
	return fmt.Sprintf("shadows=%d sids=%v rm=%v lws=%v", st.Shadows, st.Sids, st.RM, st.LWS)
}

func acctCloseState(acc *Account, subject string) acctCloseShadowState {
	res := acc.sl.Match(subject)
	subs := append([]*subscription(nil), res.psubs...)
	for _, qs := range res.qsubs {
		subs = append(subs, qs...)
	}
	st := acctCloseShadowState{Shadows: len(subs), RM: map[string]int64{}, LWS: map[string]bool{}}
	for _, sh := range subs {
		st.Sids = append(st.Sids, string(sh.sid))
	}
	acc.mu.Lock()
	for k, n := range acc.rm {
		if strings.Contains(k, subject) {
			st.RM[k] = n
			_, st.LWS[k] = acc.lws[k]
		}
	}
	acc.mu.Unlock()
	return st
}

func startAcctCloseShadowCluster(t *testing.T) (*Server, *Server) {
	t.Helper()
	confA := createConfFile(t, []byte(fmt.Sprintf(acctCloseShadowTmpl, "")))
	srvA, _ := RunServerWithConfig(confA)
	t.Cleanup(srvA.Shutdown)
	routes := fmt.Sprintf("\t\troutes: [\"nats://127.0.0.1:%d\"]\n", srvA.ClusterAddr().Port)
	confB := createConfFile(t, []byte(fmt.Sprintf(acctCloseShadowTmpl, routes)))
	srvB, _ := RunServerWithConfig(confB)
	t.Cleanup(srvB.Shutdown)
	checkClusterFormed(t, srvA, srvB)
	return srvA, srvB
}

// makeServiceReplyWildcard drives a request through IMP's service import of
// SRV's "svc": the service exporter SRV creates its per-account reply wildcard
// "_R_.<rand>.>" on its internal ACCOUNT client, which matches SRV's stream
// import of "_R_.>" from C and leaves a shadow there. Returns the wildcard.
func makeServiceReplyWildcard(t *testing.T, s *Server, srvAcc *Account) string {
	t.Helper()
	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	ncImp.Request("svc", []byte("hi"), 200*time.Millisecond)

	var wc string
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		srvAcc.mu.RLock()
		siReply := srvAcc.siReply
		ic := srvAcc.ic
		srvAcc.mu.RUnlock()
		if siReply == nil || ic == nil {
			return fmt.Errorf("service reply wildcard/internal client not created")
		}
		wc = string(siReply) + ">"
		cAcc, err := s.LookupAccount("C")
		if err != nil {
			return err
		}
		if n := acctCloseState(cAcc, wc).Shadows; n == 0 {
			return fmt.Errorf("C has no shadow for %q yet", wc)
		}
		return nil
	})
	return wc
}

// TestAccountClientCloseLeavesNoImportShadow is the primary form: the
// service-import reply wildcard lives on the account's internal ACCOUNT client;
// removeAllServiceImportSubs (what account removal does) unsubscribes only the
// service-import sids and then closes the client. With ACCOUNT excluded from
// clearAccountSubs the wildcard is never unsubscribed, so its shadow and the
// announced interest stay in the exporting account and on the route peer.
func TestAccountClientCloseLeavesNoImportShadow(t *testing.T) {
	srvA, srvB := startAcctCloseShadowCluster(t)
	cAccA, err := srvA.LookupAccount("C")
	if err != nil {
		t.Fatalf("Error looking up C on A: %v", err)
	}
	cAccB, err := srvB.LookupAccount("C")
	if err != nil {
		t.Fatalf("Error looking up C on B: %v", err)
	}
	srvAccA, err := srvA.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV on A: %v", err)
	}

	// A client in C on the peer registers the account on the route so the
	// shadow's announcement is actually propagated.
	ncC := natsConnect(t, srvB.ClientURL(), nats.UserInfo("c", "pwd"))
	defer ncC.Close()
	if _, err := ncC.SubscribeSync("keep"); err != nil {
		t.Fatalf("Error subscribing on the peer: %v", err)
	}
	natsFlush(t, ncC)

	wc := makeServiceReplyWildcard(t, srvA, srvAccA)

	// Before the close: one shadow in C on A, one announced remote interest,
	// and one remote sub in C on the peer.
	beforeA := acctCloseState(cAccA, wc)
	var peerBefore acctCloseShadowState
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		peerBefore = acctCloseState(cAccB, wc)
		if peerBefore.Shadows != 1 {
			return fmt.Errorf("peer C remote sub(s)=%d, want 1", peerBefore.Shadows)
		}
		return nil
	})
	if beforeA.Shadows != 1 || beforeA.RM["R "+wc] != 1 || !beforeA.LWS["R "+wc] {
		t.Fatalf("before close, C on A: %v (want 1 shadow, R %s rm=1 lws=true)", beforeA, wc)
	}

	// Exactly what removeCb (account removed from the JWT resolver) and the
	// config-reload deleted-account path do.
	srvAccA.removeAllServiceImportSubs()

	var afterA, peerAfter acctCloseShadowState
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		afterA = acctCloseState(cAccA, wc)
		peerAfter = acctCloseState(cAccB, wc)
		if afterA.Shadows != 0 || len(afterA.RM) != 0 || len(afterA.LWS) != 0 || peerAfter.Shadows != 0 {
			return fmt.Errorf("after close C on A: %v, peer: %v", afterA, peerAfter)
		}
		return nil
	})
	t.Logf("reply wildcard %q: before A %v / peer %v; after close A %v / peer %v",
		wc, beforeA, peerBefore, afterA, peerAfter)

	if afterA.Shadows != 0 || len(afterA.RM) != 0 || len(afterA.LWS) != 0 || peerAfter.Shadows != 0 {
		t.Fatalf("internal client close left the import shadow behind: C on A %v, peer %v (want 0 shadows and no _R_ rm/lws)", afterA, peerAfter)
	}
}

// TestAccountClientCloseLeavesNoPlainInternalSubShadow is path 4: an internal
// ACCOUNT subscription that is not a service-import sid at all. Nothing in
// removeAllServiceImportSubs knows about it; only clearing the closed client's
// account subscriptions withdraws its shadow.
func TestAccountClientCloseLeavesNoPlainInternalSubShadow(t *testing.T) {
	conf := `
		port: -1
		no_sys_acc: true
		cluster {
			name: "A"
			listen: 127.0.0.1:-1
		}
		accounts {
			C {
				users: [{user: c, password: pwd}]
				exports: [{stream: ">"}]
			}
			SRV {
				users: [{user: srv, password: pwd}]
				imports: [{stream: {account: C, subject: "foo.>"}}]
			}
		}
	`
	s, _ := RunServerWithConfig(createConfFile(t, []byte(conf)))
	t.Cleanup(s.Shutdown)
	cAcc, err := s.LookupAccount("C")
	if err != nil {
		t.Fatalf("Error looking up C: %v", err)
	}
	srvAcc, err := s.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV: %v", err)
	}

	if _, err := srvAcc.subscribeInternal("foo.bar", nil); err != nil {
		t.Fatalf("Error creating the internal sub: %v", err)
	}
	before := acctCloseState(cAcc, "foo.bar")
	if before.Shadows != 1 || before.RM["R foo.bar"] != 1 || !before.LWS["R foo.bar"] {
		t.Fatalf("before close, C: %v (want 1 shadow, R foo.bar rm=1 lws=true)", before)
	}

	srvAcc.removeAllServiceImportSubs()

	after := acctCloseState(cAcc, "foo.bar")
	if after.Shadows != 0 || len(after.RM) != 0 || len(after.LWS) != 0 {
		t.Fatalf("internal client close left the plain stream-import shadow behind: C %v (want 0 shadows, no rm/lws)", after)
	}
}

// TestAccountRemoveCbLeavesNoImportShadow is path 3: the operator/JWT resolver
// removal callback. removeCb is the single function both resolvers' deleted
// callbacks invoke (accounts.go:4578-4590), so calling it directly exercises
// exactly what an account deletion in the resolver does without requiring JWT
// and operator setup. It has the same body as the primary test but the
// lifecycle step is removeCb instead of removeAllServiceImportSubs.
func TestAccountRemoveCbLeavesNoImportShadow(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(acctCloseShadowTmpl, "")))
	s, _ := RunServerWithConfig(conf)
	t.Cleanup(s.Shutdown)
	cAcc, err := s.LookupAccount("C")
	if err != nil {
		t.Fatalf("Error looking up C: %v", err)
	}
	srvAcc, err := s.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV: %v", err)
	}

	wc := makeServiceReplyWildcard(t, s, srvAcc)
	before := acctCloseState(cAcc, wc)
	if before.Shadows != 1 || before.RM["R "+wc] != 1 || !before.LWS["R "+wc] {
		t.Fatalf("before removeCb, C: %v (want 1 shadow, R %s rm=1 lws=true)", before, wc)
	}

	// What the resolver's deleted callback runs for the removed account key.
	removeCb(s, "SRV")

	var after acctCloseShadowState
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		after = acctCloseState(cAcc, wc)
		if after.Shadows != 0 || len(after.RM) != 0 || len(after.LWS) != 0 {
			return fmt.Errorf("after removeCb C: %v", after)
		}
		return nil
	})
	if after.Shadows != 0 || len(after.RM) != 0 || len(after.LWS) != 0 {
		t.Fatalf("removeCb left the import shadow behind: C %v (want 0 shadows, no _R_ rm/lws)", after)
	}
}

// TestAccountRemovedOnReloadLeavesNoImportShadow is path 2: the public trigger.
// Deleting the exporting-service account from the config file and reloading
// runs the same account teardown for SRV, and C must not keep the shadow.
func TestAccountRemovedOnReloadLeavesNoImportShadow(t *testing.T) {
	confAfter := `
		port: -1
		no_sys_acc: true
		cluster {
			name: "A"
			listen: 127.0.0.1:-1
		}
		accounts {
			C {
				users: [{user: c, password: pwd}]
				exports: [{stream: "_R_.>"}]
			}
			IMP {
				users: [{user: imp, password: pwd}]
			}
		}
	`
	cf := createConfFile(t, []byte(fmt.Sprintf(acctCloseShadowTmpl, "")))
	s, _ := RunServerWithConfig(cf)
	t.Cleanup(s.Shutdown)
	cAcc, err := s.LookupAccount("C")
	if err != nil {
		t.Fatalf("Error looking up C: %v", err)
	}
	srvAcc, err := s.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV: %v", err)
	}

	wc := makeServiceReplyWildcard(t, s, srvAcc)
	before := acctCloseState(cAcc, wc)
	if before.Shadows != 1 || before.RM["R "+wc] != 1 {
		t.Fatalf("before reload, C: %v (want 1 shadow, rm=1)", before)
	}

	if err := os.WriteFile(cf, []byte(confAfter), 0644); err != nil {
		t.Fatalf("Error writing the new config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	var after acctCloseShadowState
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		after = acctCloseState(cAcc, wc)
		if after.Shadows != 0 || len(after.RM) != 0 || len(after.LWS) != 0 {
			return fmt.Errorf("after reload C: %v", after)
		}
		return nil
	})
	if after.Shadows != 0 || len(after.RM) != 0 || len(after.LWS) != 0 {
		t.Fatalf("account removal by reload left the import shadow behind: C %v (want 0 shadows, no _R_ rm/lws)", after)
	}
}
