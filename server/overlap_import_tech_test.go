package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const overlapImportTmpl = `
	port: -1
	no_sys_acc: true
	cluster {
		name: "C"
		port: -1
%s	}
	accounts {
		EXP {
			users: [{user: exp, password: pwd}]
			exports: [{stream: ">"}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [%s]
		}
	}
`

// overlapImportPair is one pair of stream imports from EXP used to check which
// overlaps produce duplicate shadows.
type overlapImportPair struct {
	name string
	a, b string
	sub  string
}

var overlapImportPairs = []overlapImportPair{
	{"wildcard+specific", `{stream: {account: EXP, subject: "foo.>"}}`, `{stream: {account: EXP, subject: "foo.bar"}}`, "foo.bar"},
	{"two wildcards", `{stream: {account: EXP, subject: "foo.>"}}`, `{stream: {account: EXP, subject: "foo.*"}}`, "foo.baz"},
	{"star+specific", `{stream: {account: EXP, subject: "foo.*"}}`, `{stream: {account: EXP, subject: "foo.bar"}}`, "foo.bar"},
	{"allow_trace differs", `{stream: {account: EXP, subject: "foo.>"}, allow_trace: true}`, `{stream: {account: EXP, subject: "foo.bar"}}`, "foo.bar"},
	{"non-overlapping (control)", `{stream: {account: EXP, subject: "foo.>"}}`, `{stream: {account: EXP, subject: "bar.>"}}`, "foo.bar"},
}

func overlapAccounts(t *testing.T, s *Server) (exp, imp *Account) {
	t.Helper()
	exp, err := s.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP: %v", err)
	}
	imp, err = s.LookupAccount("IMP")
	if err != nil {
		t.Fatalf("Error looking up IMP: %v", err)
	}
	return exp, imp
}

func startOverlapServer(t *testing.T, imports string) *Server {
	t.Helper()
	conf := createConfFile(t, []byte(fmt.Sprintf(overlapImportTmpl, "", imports)))
	s, _ := RunServerWithConfig(conf)
	t.Cleanup(s.Shutdown)
	return s
}

// overlapShadows returns the subscriptions matching subject in the account.
func overlapShadows(acc *Account, subject string) []*subscription {
	res := acc.sl.Match(subject)
	subs := append([]*subscription(nil), res.psubs...)
	for _, qs := range res.qsubs {
		subs = append(subs, qs...)
	}
	return subs
}

func overlapInterest(acc *Account, key string) (int64, bool) {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	_, lws := acc.lws[key]
	return acc.rm[key], lws
}

// overlapDiag records everything the overlap bug touches in the exporting
// account: how many shadows one client subscription produced, their shape, the
// announced remote interest that follows from them, and how many stream
// imports the importing account accepted.
type overlapDiag struct {
	Shadows  int
	Subjects []string
	Sids     []string
	Queues   []string
	Weights  []int
	RM       map[string]int64
	LWS      map[string]bool
	Imports  int
	FromTo   []string
}

func overlapDiagnose(exp, imp *Account, subject string) overlapDiag {
	shadows := overlapShadows(exp, subject)
	d := overlapDiag{Shadows: len(shadows), RM: map[string]int64{}, LWS: map[string]bool{}}
	for _, sh := range shadows {
		d.Subjects = append(d.Subjects, string(sh.subject))
		d.Sids = append(d.Sids, string(sh.sid))
		d.Queues = append(d.Queues, string(sh.queue))
		d.Weights = append(d.Weights, int(sh.qw))
	}
	exp.mu.Lock()
	for k, n := range exp.rm {
		if strings.Contains(k, subject) {
			d.RM[k] = n
			_, d.LWS[k] = exp.lws[k]
		}
	}
	exp.mu.Unlock()
	imp.mu.Lock()
	d.Imports = len(imp.imports.streams)
	for _, si := range imp.imports.streams {
		d.FromTo = append(d.FromTo, fmt.Sprintf("%s->%s(usePub=%v,invalid=%v)", si.from, si.to, si.usePub, si.invalid))
	}
	imp.mu.Unlock()
	return d
}

func (d overlapDiag) String() string {
	return fmt.Sprintf("shadows=%d subjects=%v sids=%v queues=%v weights=%v rm=%v lws=%v imports=%d %v",
		d.Shadows, d.Subjects, d.Sids, d.Queues, d.Weights, d.RM, d.LWS, d.Imports, d.FromTo)
}

// overlapDeliveries publishes payload on subject from a client in the exporting
// account and returns how many copies reached the subscription.
func overlapDeliveries(t *testing.T, srv *Server, sub *nats.Subscription, subject string) int {
	t.Helper()
	ncExp := natsConnect(t, srv.ClientURL(), nats.UserInfo("exp", "pwd"))
	defer ncExp.Close()
	if err := ncExp.Publish(subject, []byte("hi")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	natsFlush(t, ncExp)
	got := 0
	for {
		if _, err := sub.NextMsg(200 * time.Millisecond); err != nil {
			break
		}
		got++
		if got > 10 {
			break
		}
	}
	return got
}

// TestOverlappingStreamImportDuplicateShadow is the primary form: two
// overlapping stream imports from the same account make addShadowSubscriptions
// create one shadow per import, so a single client subscription appears twice
// in the exporting account, announces doubled interest and receives each
// publish twice.
func TestOverlappingStreamImportDuplicateShadow(t *testing.T) {
	const imports = `{stream: {account: EXP, subject: "foo.>"}}, {stream: {account: EXP, subject: "foo.bar"}}`
	s := startOverlapServer(t, imports)

	expAcc, impAcc := overlapAccounts(t, s)

	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	sub := natsSubSync(t, ncImp, "foo.bar")
	natsFlush(t, ncImp)

	before := overlapDiagnose(expAcc, impAcc, "foo.bar")
	got := overlapDeliveries(t, s, sub, "foo.bar")

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Error unsubscribing: %v", err)
	}
	natsFlush(t, ncImp)
	var after overlapDiag
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		after = overlapDiagnose(expAcc, impAcc, "foo.bar")
		if after.Shadows != 0 || len(after.RM) != 0 || len(after.LWS) != 0 {
			return fmt.Errorf("not withdrawn yet: %v", after)
		}
		return nil
	})

	if before.Shadows != 1 || before.RM["R foo.bar"] != 1 || !before.LWS["R foo.bar"] || got != 1 {
		t.Fatalf("one subscriber over overlapping imports: before %v, one publish delivered %d time(s), after %v (want shadows=1 R foo.bar rm=1 lws=true, 1 delivery, then shadows=0 and no rm/lws)",
			before, got, after)
	}
}

// TestOverlappingStreamImportQueueWeight is the queue flavour.
func TestOverlappingStreamImportQueueWeight(t *testing.T) {
	const imports = `{stream: {account: EXP, subject: "foo.>"}}, {stream: {account: EXP, subject: "foo.bar"}}`
	s := startOverlapServer(t, imports)

	expAcc, impAcc := overlapAccounts(t, s)

	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	sub := natsQueueSubSync(t, ncImp, "foo.bar", "q")
	natsFlush(t, ncImp)

	before := overlapDiagnose(expAcc, impAcc, "foo.bar")
	got := overlapDeliveries(t, s, sub, "foo.bar")

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Error unsubscribing: %v", err)
	}
	natsFlush(t, ncImp)
	var after overlapDiag
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		after = overlapDiagnose(expAcc, impAcc, "foo.bar")
		if after.Shadows != 0 || len(after.RM) != 0 || len(after.LWS) != 0 {
			return fmt.Errorf("not withdrawn yet: %v", after)
		}
		return nil
	})

	if before.Shadows != 1 || before.RM["R foo.bar q"] != 1 || !before.LWS["R foo.bar q"] || got != 1 {
		t.Fatalf("one queue member over overlapping imports: before %v, one publish delivered %d time(s), after %v (want shadows=1 R foo.bar q rm=1 lws=true, 1 delivery, then shadows=0 and no rm/lws)",
			before, got, after)
	}
}

// TestOverlappingStreamImportReload refreshes the same sub through
// processSubsOnConfigReload and must stay deduped.
func TestOverlappingStreamImportReload(t *testing.T) {
	const imports = `{stream: {account: EXP, subject: "foo.>"}}, {stream: {account: EXP, subject: "foo.bar"}}`
	s := startOverlapServer(t, imports)

	expAcc, impAcc := overlapAccounts(t, s)
	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	sub := natsSubSync(t, ncImp, "foo.bar")
	natsFlush(t, ncImp)

	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}
	var d overlapDiag
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		d = overlapDiagnose(expAcc, impAcc, "foo.bar")
		if d.Shadows != 1 || d.RM["R foo.bar"] != 1 || !d.LWS["R foo.bar"] {
			return fmt.Errorf("after reload: %v", d)
		}
		return nil
	})
	got := overlapDeliveries(t, s, sub, "foo.bar")
	if got != 1 {
		t.Fatalf("after reload one publish was delivered %d time(s) (subscription %s)", got, sub.Subject)
	}
}

// TestOverlappingStreamImportIdenticalRejected records the boundary of the
// overlap: two imports of exactly the same subject are rejected when the
// config is loaded (isStreamImportDuplicate), so only distinct overlapping
// subjects reach the shadow loop twice.
func TestOverlappingStreamImportIdenticalRejected(t *testing.T) {
	const imports = `{stream: {account: EXP, subject: "foo.bar"}}, {stream: {account: EXP, subject: "foo.bar"}}`
	conf := createConfFile(t, []byte(fmt.Sprintf(overlapImportTmpl, "", imports)))
	_, err := ProcessConfigFile(conf)
	require_Error(t, err, errors.New(`Error adding stream import "foo.bar": stream import already exists`))
}

// TestOverlappingStreamImportShapes records which overlapping import pairs
// duplicate, plus a non-overlapping control, and fails once with the whole
// table.
func TestOverlappingStreamImportShapes(t *testing.T) {
	var report []string
	for _, tc := range overlapImportPairs {
		s := startOverlapServer(t, tc.a+", "+tc.b)
		expAcc, impAcc := overlapAccounts(t, s)
		ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
		sub := natsSubSync(t, ncImp, tc.sub)
		natsFlush(t, ncImp)

		d := overlapDiagnose(expAcc, impAcc, tc.sub)
		got := overlapDeliveries(t, s, sub, tc.sub)
		line := fmt.Sprintf("%s: %v, one publish delivered %d time(s)", tc.name, d, got)
		report = append(report, line)
		t.Logf("%s", line)
		if d.Shadows != 1 || d.RM["R "+tc.sub] != 1 || !d.LWS["R "+tc.sub] || got != 1 {
			t.Errorf("%s: %v, one publish delivered %d time(s) (want shadows=1 R %s rm=1 lws=true, 1 delivery)", tc.name, d, got, tc.sub)
		}
		ncImp.Close()
		s.Shutdown()
	}
	t.Logf("overlap shapes:\n%s", strings.Join(report, "\n"))
}

// overlapRemoteSubs returns the number of plain remote subs and the queue
// weights the account holds for subject, i.e. what a peer has been told.
func overlapRemoteSubs(acc *Account, subject string) (int, []int) {
	res := acc.sl.Match(subject)
	var weights []int
	for _, qs := range res.qsubs {
		for _, qs := range qs {
			weights = append(weights, int(qs.qw))
		}
	}
	return len(res.psubs), weights
}

// TestOverlappingStreamImportRoutePeer checks what a peer is told and how many
// copies a routed publish produces. Overlapping imports on the subscribing
// server double the local shadows and the delivery count; for a normal sub the
// peer still sees one route subscription (announcements only fire on 0->1),
// while a queue sub's weight on the peer is doubled.
func TestOverlappingStreamImportRoutePeer(t *testing.T) {
	const imports = `{stream: {account: EXP, subject: "foo.>"}}, {stream: {account: EXP, subject: "foo.bar"}}`
	confA := createConfFile(t, []byte(fmt.Sprintf(overlapImportTmpl, "", imports)))
	srvA, _ := RunServerWithConfig(confA)
	t.Cleanup(srvA.Shutdown)
	routes := fmt.Sprintf("\t\troutes: [\"nats://127.0.0.1:%d\"]\n", srvA.ClusterAddr().Port)
	confB := createConfFile(t, []byte(fmt.Sprintf(overlapImportTmpl, routes, imports)))
	srvB, _ := RunServerWithConfig(confB)
	t.Cleanup(srvB.Shutdown)
	checkClusterFormed(t, srvA, srvB)

	expAccA, impAccA := overlapAccounts(t, srvA)
	expAccB, impAccB := overlapAccounts(t, srvB)

	// Plain sub on B, publisher on A.
	ncImp := natsConnect(t, srvB.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	sub := natsSubSync(t, ncImp, "foo.bar")
	natsFlush(t, ncImp)

	var localB, remoteA overlapDiag
	var peerSubs int
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		localB = overlapDiagnose(expAccB, impAccB, "foo.bar")
		remoteA = overlapDiagnose(expAccA, impAccA, "foo.bar")
		peerSubs, _ = overlapRemoteSubs(expAccA, "foo.bar")
		if peerSubs != 1 {
			return fmt.Errorf("peer EXP has %d remote sub(s)", peerSubs)
		}
		return nil
	})
	plainDelivered := overlapDeliveries(t, srvA, sub, "foo.bar")
	plainOK := localB.Shadows == 1 && localB.RM["R foo.bar"] == 1 && peerSubs == 1 && plainDelivered == 1
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Error unsubscribing: %v", err)
	}
	natsFlush(t, ncImp)

	// Queue member on B, publisher on A: the peer's queue weight is the
	// peer-visible doubling.
	qsub := natsQueueSubSync(t, ncImp, "foo.bar", "q")
	natsFlush(t, ncImp)
	var localQB overlapDiag
	var peerWeights []int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		localQB = overlapDiagnose(expAccB, impAccB, "foo.bar")
		_, peerWeights = overlapRemoteSubs(expAccA, "foo.bar")
		if localQB.Shadows >= 1 && len(peerWeights) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	queueDelivered := overlapDeliveries(t, srvA, qsub, "foo.bar")
	queueOK := localQB.Shadows == 1 && localQB.RM["R foo.bar q"] == 1 && len(peerWeights) == 1 && peerWeights[0] == 1 && queueDelivered == 1
	if !plainOK || !queueOK {
		t.Fatalf("routed publishes over overlapping imports:\nplain: B local %v, A remote %v, peer subs=%d, one publish delivered %d time(s)\nqueue: B local %v, peer queue weights=%v, one publish delivered %d time(s)\n(want B shadows=1 and rm=1, peer subs=1 / peer weight [1], 1 delivery each)",
			localB, remoteA, peerSubs, plainDelivered, localQB, peerWeights, queueDelivered)
	}
}
