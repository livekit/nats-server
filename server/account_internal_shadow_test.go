package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const acctInternalShadowConf = `
	port: -1
	no_sys_acc: true
	cluster {
		name: "C"
		listen: 127.0.0.1:-1
%s	}
	accounts {
		EXP {
			users: [{user: exp, password: pwd}]
			exports: [{stream: "foo.>"}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [{stream: {account: EXP, subject: "foo.>"}}]
		}
	}
`

func startAcctInternalShadowCluster(t *testing.T) (*Server, *Server) {
	t.Helper()
	confA := createConfFile(t, []byte(fmt.Sprintf(acctInternalShadowConf, "")))
	srvA, _ := RunServerWithConfig(confA)
	t.Cleanup(srvA.Shutdown)
	routes := fmt.Sprintf("\t\troutes: [\"nats://127.0.0.1:%d\"]\n", srvA.ClusterAddr().Port)
	confB := createConfFile(t, []byte(fmt.Sprintf(acctInternalShadowConf, routes)))
	srvB, _ := RunServerWithConfig(confB)
	t.Cleanup(srvB.Shutdown)
	checkClusterFormed(t, srvA, srvB)
	return srvA, srvB
}

// TestAccountInternalSubShadowInterestWithdrawal is ticket 0008's minimal form:
// an ACCOUNT-kind internal subscription that matches a stream import gets its
// shadow announced with +1, but unsubscribe's shadow loop does not include the
// ACCOUNT kind, so the exporting account keeps rm/lws for a shadow that is no
// longer in the sublist and the route peer keeps the interest.
func TestAccountInternalSubShadowInterestWithdrawal(t *testing.T) {
	srvA, srvB := startAcctInternalShadowCluster(t)

	expAccA, err := srvA.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP on A: %v", err)
	}
	impAccA, err := srvA.LookupAccount("IMP")
	if err != nil {
		t.Fatalf("Error looking up IMP on A: %v", err)
	}
	expAccB, err := srvB.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP on B: %v", err)
	}

	// A client in EXP on the peer registers the account on the route so the
	// announced interest is actually propagated across it.
	ncExpB := natsConnect(t, srvB.ClientURL(), nats.UserInfo("exp", "pwd"))
	defer ncExpB.Close()
	if _, err := ncExpB.SubscribeSync("keep"); err != nil {
		t.Fatalf("Error subscribing on the peer: %v", err)
	}
	natsFlush(t, ncExpB)

	// The request/reply plumbing creates this sub in the importing account on
	// an ACCOUNT-kind internal client (auth callout replies, service-import
	// response wildcards, JetStream).
	sub, err := impAccA.subscribeInternal("foo.bar", nil)
	if err != nil {
		t.Fatalf("Error on internal subscribe: %v", err)
	}
	if sub == nil || sub.client == nil || sub.client.kind != ACCOUNT {
		t.Fatalf("Expected an ACCOUNT-kind internal subscription, got %+v", sub)
	}

	if rm, lws := acctInterest(expAccA, "R foo.bar"); rm != 1 || !lws {
		t.Fatalf("Before the unsub EXP should hold the shadow interest: rm=%d lws=%v", rm, lws)
	}
	if n := len(expAccA.sl.Match("foo.bar").psubs); n != 1 {
		t.Fatalf("Before the unsub EXP should hold the shadow: %d", n)
	}

	impAccA.unsubscribeInternal(sub)

	// The shadow leaves the sublist, but nothing withdraws its route interest.
	if n := len(expAccA.sl.Match("foo.bar").psubs); n != 0 {
		t.Fatalf("EXP still has %d shadow sub(s) after the internal unsub", n)
	}
	rm, lws := acctInterest(expAccA, "R foo.bar")
	if rm != 0 || lws {
		t.Fatalf("EXP keeps remote interest for a removed ACCOUNT-kind shadow: rm=%d lws=%v, expected 0/false", rm, lws)
	}
	// The peer route keeps the interest, so publishes there are still routed.
	if rm, lws := acctInterest(expAccB, "R foo.bar"); rm != 0 || lws {
		t.Fatalf("peer EXP keeps remote interest for the removed ACCOUNT-kind shadow: rm=%d lws=%v, expected 0/false", rm, lws)
	}
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if rm, lws := acctInterest(expAccB, "R foo.bar"); rm != 0 || lws {
			return fmt.Errorf("peer EXP keeps remote interest: rm=%d lws=%v", rm, lws)
		}
		return nil
	})
}

func acctInterest(acc *Account, key string) (int64, bool) {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	_, lws := acc.lws[key]
	return acc.rm[key], lws
}

const acctInternalSvcConf = `
	port: -1
	no_sys_acc: true
	cluster {
		name: "C"
		listen: 127.0.0.1:-1
%s	}
	accounts {
		SRV {
			users: [{user: srv, password: pwd}]
			exports: [{stream: "_R_.>"}]
		}
		EXP {
			users: [{user: exp, password: pwd}]
			exports: [{service: "svc"}]
			imports: [{stream: {account: SRV, subject: "_R_.>"}}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [{service: {account: EXP, subject: "svc"}}]
		}
	}
`

// TestAccountInternalSubShadowInterestWithdrawalPublicServiceReply is ticket
// 0008's public-API variant B: a client request crossing a service import makes
// the server create the account's `_R_.<id>.>` response wildcard internal sub;
// because the account also stream-imports `_R_.>`, that internal sub gets a
// shadow announced in EXP. Removing the internal sub (the accounting the server
// does on account/JetStream cleanup) must withdraw the shadow interest.
func TestAccountInternalSubShadowInterestWithdrawalPublicServiceReply(t *testing.T) {
	confA := createConfFile(t, []byte(fmt.Sprintf(acctInternalSvcConf, "")))
	srvA, _ := RunServerWithConfig(confA)
	t.Cleanup(srvA.Shutdown)
	routes := fmt.Sprintf("\t\troutes: [\"nats://127.0.0.1:%d\"]\n", srvA.ClusterAddr().Port)
	confB := createConfFile(t, []byte(fmt.Sprintf(acctInternalSvcConf, routes)))
	srvB, _ := RunServerWithConfig(confB)
	t.Cleanup(srvB.Shutdown)
	checkClusterFormed(t, srvA, srvB)

	srvAccA, err := srvA.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV on A: %v", err)
	}
	expAccA, err := srvA.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP on A: %v", err)
	}
	srvAccB, err := srvB.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV on B: %v", err)
	}

	// A client in SRV on the peer registers the account on the route.
	ncSrvB := natsConnect(t, srvB.ClientURL(), nats.UserInfo("srv", "pwd"))
	defer ncSrvB.Close()
	if _, err := ncSrvB.SubscribeSync("keep"); err != nil {
		t.Fatalf("Error subscribing on the peer: %v", err)
	}
	natsFlush(t, ncSrvB)

	// Responder in EXP (on A) and requester in IMP (on A); the server creates
	// the response wildcard in EXP, whose stream import of `_R_.>` (from SRV)
	// gives it a shadow.
	ncExp := natsConnect(t, srvA.ClientURL(), nats.UserInfo("exp", "pwd"))
	defer ncExp.Close()
	if _, err := ncExp.Subscribe("svc", func(m *nats.Msg) { m.Respond([]byte("ok")) }); err != nil {
		t.Fatalf("Error subscribing responder: %v", err)
	}
	natsFlush(t, ncExp)

	ncImp := natsConnect(t, srvA.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	if _, err := ncImp.Request("svc", []byte("hi"), 2*time.Second); err != nil {
		t.Fatalf("Error on request across the service import: %v", err)
	}

	// The server created the account's response wildcard sub for this request.
	expAccA.mu.Lock()
	siReply := append([]byte(nil), expAccA.siReply...)
	ic := expAccA.ic
	expAccA.mu.Unlock()
	if len(siReply) == 0 || ic == nil {
		t.Fatalf("No service-import response wildcard was created: siReply=%q ic=%v", siReply, ic)
	}
	wcSubject := string(siReply) + ">"

	ic.mu.Lock()
	var wcSub *subscription
	for _, s := range ic.subs {
		if string(s.subject) == wcSubject {
			wcSub = s
			break
		}
	}
	ic.mu.Unlock()
	if wcSub == nil {
		t.Fatalf("No internal ACCOUNT-kind subscription for %q", wcSubject)
	}
	if wcSub.client.kind != ACCOUNT {
		t.Fatalf("Expected the wildcard sub on an ACCOUNT client, got kind %d", wcSub.client.kind)
	}

	// The stream import of `_R_.>` gave the internal sub a shadow in EXP.
	if n := len(srvAccA.sl.Match(wcSubject).psubs); n != 1 {
		t.Fatalf("SRV should hold 1 shadow for %q, has %d (siReply=%q)", wcSubject, n, siReply)
	}
	if rm, lws := acctInterest(srvAccA, "R "+wcSubject); rm != 1 || !lws {
		t.Fatalf("Before the unsub SRV should hold the shadow interest for %q: rm=%d lws=%v", wcSubject, rm, lws)
	}

	// The server removes the sub when the reply accounting is torn down.
	expAccA.unsubscribeInternal(wcSub)

	if n := len(srvAccA.sl.Match(wcSubject).psubs); n != 0 {
		t.Fatalf("SRV still has %d shadow sub(s) for %q after the internal unsub", n, wcSubject)
	}
	rm, lws := acctInterest(srvAccA, "R "+wcSubject)
	if rm != 0 || lws {
		t.Fatalf("SRV keeps remote interest for the removed service-reply wildcard %q: rm=%d lws=%v, expected 0/false", wcSubject, rm, lws)
	}
	if rm, lws := acctInterest(srvAccB, "R "+wcSubject); rm != 0 || lws {
		t.Fatalf("peer SRV keeps remote interest for the removed service-reply wildcard %q: rm=%d lws=%v, expected 0/false", wcSubject, rm, lws)
	}
}

const acctInternalCalloutConf = `
	port: -1
	cluster {
		name: "C"
		listen: 127.0.0.1:-1
%s	}
	system_account: SYS
	authorization {
		auth_callout {
			issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
			auth_users: [auth, srv]
			account: AUTH
		}
	}
	accounts {
		SYS { users: [{user: sys, password: pwd}] }
		AUTH {
			users: [{user: auth, password: pwd}]
			imports: [{stream: {account: SRV, subject: "$SYS._INBOX.>"}}]
		}
		SRV {
			users: [{user: srv, password: pwd}]
			exports: [{stream: "$SYS._INBOX.>"}]
		}
	}
`

// TestAccountInternalSubShadowInterestWithdrawalAuthCallout is ticket 0008's
// public-API variant A in config mode: every auth callout creates the reply
// subscription with acc.subscribeInternal(reply, ...) in the callout account
// (AUTH here) and removes it with defer acc.unsubscribeInternal(sub). The reply
// inbox is $SYS._INBOX.<server>.<rand>, so AUTH stream-imports $SYS._INBOX.>
// from SRV and each completed callout leaves one shadow and its remote
// interest announced in SRV, never withdrawn.
func TestAccountInternalSubShadowInterestWithdrawalAuthCallout(t *testing.T) {
	confA := createConfFile(t, []byte(fmt.Sprintf(acctInternalCalloutConf, "")))
	srvA, _ := RunServerWithConfig(confA)
	t.Cleanup(srvA.Shutdown)
	routes := fmt.Sprintf("\t\troutes: [\"nats://127.0.0.1:%d\"]\n", srvA.ClusterAddr().Port)
	confB := createConfFile(t, []byte(fmt.Sprintf(acctInternalCalloutConf, routes)))
	srvB, _ := RunServerWithConfig(confB)
	t.Cleanup(srvB.Shutdown)
	checkClusterFormed(t, srvA, srvB)

	srvAccA, err := srvA.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV on A: %v", err)
	}
	srvAccB, err := srvB.LookupAccount("SRV")
	if err != nil {
		t.Fatalf("Error looking up SRV on B: %v", err)
	}

	// A client in SRV on the peer registers the account on the route.
	ncSrvB := natsConnect(t, srvB.ClientURL(), nats.UserInfo("srv", "pwd"))
	defer ncSrvB.Close()
	if _, err := ncSrvB.SubscribeSync("keep"); err != nil {
		t.Fatalf("Error subscribing on the peer: %v", err)
	}
	natsFlush(t, ncSrvB)

	// Auth callout responder: allow every request and bind it to the global
	// account, which is what config-mode callout tests use.
	ncCallout := natsConnect(t, srvA.ClientURL(), nats.UserInfo("auth", "pwd"))
	defer ncCallout.Close()
	if _, err := ncCallout.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}); err != nil {
		t.Fatalf("Error subscribing the callout handler: %v", err)
	}
	natsFlush(t, ncCallout)

	const callouts = 3
	for i := 0; i < callouts; i++ {
		nc := natsConnect(t, srvA.ClientURL(), nats.UserInfo("dlc", "zzz"))
		nc.Close()
	}

	// Each callout created and removed an internal literal
	// $SYS._INBOX.<server>.<rand> sub in AUTH; SRV's shadows are gone, but the
	// interest they announced is not withdrawn. The peer received the same
	// announcements through the route.
	countPhantom := func(acc *Account) []string {
		acc.mu.Lock()
		defer acc.mu.Unlock()
		var phantom []string
		for k := range acc.rm {
			if strings.HasPrefix(k, "R $SYS._INBOX.") {
				phantom = append(phantom, k)
			}
		}
		return phantom
	}
	phantomA, phantomB := countPhantom(srvAccA), countPhantom(srvAccB)
	if len(phantomA) != 0 || len(phantomB) != 0 {
		t.Fatalf("after %d callouts SRV keeps remote interest for removed ACCOUNT-kind shadows: A=%d (%v) B=%d (%v)",
			callouts, len(phantomA), phantomA, len(phantomB), phantomB)
	}
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(countPhantom(srvAccB)); n != 0 {
			return fmt.Errorf("peer SRV keeps %d remote interest entries", n)
		}
		return nil
	})
}
