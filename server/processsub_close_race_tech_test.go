package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const processSubRaceConf = `
	port: -1
	no_sys_acc: true
	cluster {
		name: "C"
		listen: 127.0.0.1:-1
%s	}
	accounts {
		EXP {
			users: [{user: exp, password: pwd}]
			exports: [{stream: "foo.bar"}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [{stream: {account: EXP, subject: "foo.bar"}}]
		}
	}
`

// Several stream imports of one wildcard export: a single wildcard SUB creates
// one shadow per matching import, so the window between the sublist insert and
// the sub.shadow assignment is several inserts/announcements wide.
const processSubManyImportsConf = `
	port: -1
	no_sys_acc: true
	cluster {
		name: "C"
		listen: 127.0.0.1:-1
%s	}
	accounts {
		EXP {
			users: [{user: exp, password: pwd}]
			exports: [{stream: "s1"}, {stream: "s2"}, {stream: "s3"}, {stream: "s4"}, {stream: "s5"}, {stream: "s6"}, {stream: "s7"}, {stream: "s8"}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [
				{stream: {account: EXP, subject: "s1"}}
				{stream: {account: EXP, subject: "s2"}}
				{stream: {account: EXP, subject: "s3"}}
				{stream: {account: EXP, subject: "s4"}}
				{stream: {account: EXP, subject: "s5"}}
				{stream: {account: EXP, subject: "s6"}}
				{stream: {account: EXP, subject: "s7"}}
				{stream: {account: EXP, subject: "s8"}}
			]
		}
	}
`

var manyImportSubjects = []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}

func startProcessSubRaceCluster(t *testing.T, conf string) (*Server, *Server) {
	t.Helper()
	confA := createConfFile(t, []byte(fmt.Sprintf(conf, "")))
	srvA, _ := RunServerWithConfig(confA)
	t.Cleanup(srvA.Shutdown)
	routes := fmt.Sprintf("\t\troutes: [\"nats://127.0.0.1:%d\"]\n", srvA.ClusterAddr().Port)
	confB := createConfFile(t, []byte(fmt.Sprintf(conf, routes)))
	srvB, _ := RunServerWithConfig(confB)
	t.Cleanup(srvB.Shutdown)
	checkClusterFormed(t, srvA, srvB)
	return srvA, srvB
}

func findClientByName(s *Server, name string) *client {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clients {
		c.mu.Lock()
		n := c.opts.Name
		c.mu.Unlock()
		if n == name {
			return c
		}
	}
	return nil
}

// rawImpConn opens a client connection for the IMP user with the given name.
// The test only writes protocol bytes on it.
func rawImpConn(t *testing.T, url, name string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url,
		nats.UserInfo("imp", "pwd"),
		nats.Name(name),
		nats.NoReconnect(),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// TestProcessSubShadowLeakOnConcurrentClose parks a SUB after its sublist
// insert but before addShadowSubscriptions creates and announces the shadow,
// closes the client in that window, then lets the SUB continue. The shadow it
// creates belongs to a closed client, is not tracked by c.subs, and keeps the
// exporting account's remote interest (rm/lws) alive forever. The +1 for the
// closed subscription itself is announced after the close already sent its -1,
// so the importing account keeps phantom interest too.
func TestProcessSubShadowLeakOnConcurrentClose(t *testing.T) {
	srvA, srvB := startProcessSubRaceCluster(t, processSubRaceConf)

	expAccA, err := srvA.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP: %v", err)
	}
	impAccA, err := srvA.LookupAccount("IMP")
	if err != nil {
		t.Fatalf("Error looking up IMP: %v", err)
	}

	nc := rawImpConn(t, srvA.ClientURL(), "imp-raw")
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if findClientByName(srvA, "imp-raw") == nil {
			return fmt.Errorf("client not registered")
		}
		return nil
	})
	c := findClientByName(srvA, "imp-raw")

	// Park processSubEx inside addShadowSubscriptions (it takes IMP's read
	// lock at its top) after the SUB was inserted into IMP's sublist.
	impAccA.mu.Lock()
	locked := true
	defer func() {
		if locked {
			impAccA.mu.Unlock()
		}
	}()

	// The SUB is written asynchronously; do not Flush here: the server cannot
	// answer the PING while processSub is parked.
	if _, err := nc.SubscribeSync("foo.bar"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	const sid = "1"
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		c.mu.Lock()
		_, ok := c.subs[sid]
		c.mu.Unlock()
		if !ok {
			return fmt.Errorf("SUB not inserted yet")
		}
		return nil
	})
	if res := expAccA.sl.Match("foo.bar"); len(res.psubs)+len(res.qsubs) != 0 {
		t.Fatalf("EXP already has a shadow before the close: %d", len(res.psubs)+len(res.qsubs))
	}

	// Close the client while the SUB is parked before it announced its shadow.
	done := make(chan struct{})
	go func() {
		srvA.DisconnectClientByID(c.cid)
		close(done)
	}()
	// Wait until the close has unsubscribed the SUB: it is gone from IMP's
	// sublist, which happens under c.mu right before sub.close() in the same
	// critical section. Do not take c.mu or call (*client).isClosed() here:
	// saveClosedClient holds c.mu while blocking on the account lock this test
	// holds, and isClosed reads c.nc without a lock during connection teardown.
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		res := impAccA.sl.Match("foo.bar")
		if n := len(res.psubs) + len(res.qsubs); n != 0 {
			return fmt.Errorf("SUB still in IMP sublist")
		}
		return nil
	})

	// Let the parked SUB finish: it now creates the shadow for a closed client.
	impAccA.mu.Unlock()
	locked = false
	<-done

	// The parked SUB is the client's readLoop. Nothing holds the account lock
	// anymore, so it completes well within this window; on the buggy code the
	// interest it published stays behind forever.
	time.Sleep(250 * time.Millisecond)

	assertNoClosedClientInterest(t, []*Account{impAccA, expAccA}, "foo.bar")
	assertNoShadow(t, expAccA, "foo.bar")

	// The peer should have no interest for the subject once the client is gone.
	expAccB, err := srvB.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP on B: %v", err)
	}
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		res := expAccB.sl.Match("foo.bar")
		if n := len(res.psubs) + len(res.qsubs); n != 0 {
			return fmt.Errorf("peer still has %d subs for foo.bar", n)
		}
		return nil
	})
}

// TestProcessSubManyImportShadowLeakOnConcurrentClose is the import/export
// variant: a wildcard SUB matches four stream imports, so four shadows are
// created and announced after the unlock. Closing the client in that window
// must not leave any of them (or their rm/lws keys) behind.
func TestProcessSubManyImportShadowLeakOnConcurrentClose(t *testing.T) {
	srvA, _ := startProcessSubRaceCluster(t, processSubManyImportsConf)

	expAccA, err := srvA.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP: %v", err)
	}
	impAccA, err := srvA.LookupAccount("IMP")
	if err != nil {
		t.Fatalf("Error looking up IMP: %v", err)
	}

	nc := rawImpConn(t, srvA.ClientURL(), "imp-many")
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if findClientByName(srvA, "imp-many") == nil {
			return fmt.Errorf("client not registered")
		}
		return nil
	})
	c := findClientByName(srvA, "imp-many")

	impAccA.mu.Lock()
	locked := true
	defer func() {
		if locked {
			impAccA.mu.Unlock()
		}
	}()

	if _, err := nc.SubscribeSync(">"); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		c.mu.Lock()
		_, ok := c.subs["1"]
		c.mu.Unlock()
		if !ok {
			return fmt.Errorf("SUB not inserted yet")
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		srvA.DisconnectClientByID(c.cid)
		close(done)
	}()
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		res := impAccA.sl.Match(">")
		if n := len(res.psubs) + len(res.qsubs); n != 0 {
			return fmt.Errorf("SUB still in IMP sublist")
		}
		return nil
	})

	impAccA.mu.Unlock()
	locked = false
	<-done
	time.Sleep(250 * time.Millisecond)

	for _, subj := range manyImportSubjects {
		assertNoClosedClientInterest(t, []*Account{expAccA}, subj)
		assertNoShadow(t, expAccA, subj)
	}
}

// TestProcessSubConcurrentCloseStress is the stress form from the ticket:
// processSub is called in a loop from the test goroutine while a second
// goroutine closes the connection. After both finish the sublist and the
// account remote-interest maps must have no trace of the closed client.
func TestProcessSubConcurrentCloseStress(t *testing.T) {
	const iterations = 400
	const subsPerIter = 20

	srvA, _ := startProcessSubRaceCluster(t, processSubManyImportsConf)
	expAccA, err := srvA.LookupAccount("EXP")
	if err != nil {
		t.Fatalf("Error looking up EXP: %v", err)
	}
	impAccA, err := srvA.LookupAccount("IMP")
	if err != nil {
		t.Fatalf("Error looking up IMP: %v", err)
	}

	leftovers := func() int {
		n := 0
		for _, acc := range []*Account{impAccA, expAccA} {
			for _, subj := range []string{">", "s1"} {
				res := acc.sl.Match(subj)
				n += len(res.psubs) + len(res.qsubs)
			}
			for _, subj := range manyImportSubjects {
				if rm, lws := accRmValue(acc, "R "+subj); rm != 0 || lws {
					n++
				}
			}
		}
		return n
	}

	var failures int
	var examples []string
	for i := 0; i < iterations; i++ {
		before := leftovers()
		nc := rawImpConn(t, srvA.ClientURL(), fmt.Sprintf("stress-%d", i))
		var c *client
		checkFor(t, 2*time.Second, 2*time.Millisecond, func() error {
			if c = findClientByName(srvA, fmt.Sprintf("stress-%d", i)); c == nil {
				return fmt.Errorf("client not registered")
			}
			return nil
		})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for k := 0; k < subsPerIter; k++ {
				if _, err := c.processSub([]byte(">"), nil, []byte(fmt.Sprintf("%d", k+1)), nil, false); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			srvA.DisconnectClientByID(c.cid)
		}()
		wg.Wait()
		nc.Close()

		// Compare against the previous iteration's state: a leak is permanent,
		// so counting all leftovers would make every later iteration look
		// broken once one has leaked.
		if after := leftovers(); after > before {
			failures++
			if len(examples) < 3 {
				examples = append(examples, fmt.Sprintf("iteration %d: %d leftover entries in the IMP/EXP sublists or rm maps", i, after-before))
			}
		}
	}
	if failures > 0 {
		for _, ex := range examples {
			t.Logf("%s", ex)
		}
		t.Fatalf("%d/%d iterations left interest behind after a concurrent close", failures, iterations)
	}
}

// assertNoClosedClientInterest checks that neither account's sublist holds a
// subscription for subject and that no remote-interest key for it is left.
func assertNoClosedClientInterest(t *testing.T, accs []*Account, subject string) {
	t.Helper()
	var problems []string
	for _, acc := range accs {
		if res := acc.sl.Match(subject); len(res.psubs)+len(res.qsubs) != 0 {
			problems = append(problems, fmt.Sprintf("account %q has %d subscription(s) for %q", acc.Name, len(res.psubs)+len(res.qsubs), subject))
		}
		if rm, lws := accRmValue(acc, "R "+subject); rm != 0 || lws {
			problems = append(problems, fmt.Sprintf("account %q keeps remote interest for %q: rm=%d lws=%v", acc.Name, "R "+subject, rm, lws))
		}
	}
	if len(problems) > 0 {
		t.Fatalf("interest for a closed client was left behind: %v", problems)
	}
}

// assertNoShadow checks that the exporting account holds no shadow whose client
// is closed (or whose subscription is closed).
func assertNoShadow(t *testing.T, acc *Account, subject string) {
	t.Helper()
	res := acc.sl.Match(subject)
	for _, sub := range res.psubs {
		sub.client.mu.Lock()
		clientClosed := sub.client.isClosed()
		subID := string(sub.sid)
		sub.client.mu.Unlock()
		t.Logf("shadow: sid=%q subject=%q subClosed=%v clientClosed=%v clientInSubs=%v",
			subID, sub.subject, sub.isClosed(), clientClosed, clientHasSub(sub.client, subID))
	}
	if n := len(res.psubs) + len(res.qsubs); n != 0 {
		t.Fatalf("exporting account %q has %d shadow sub(s) for a closed client, expected 0", acc.Name, n)
	}
}

func clientHasSub(c *client, sid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.subs[sid]
	return ok
}

func accRmValue(acc *Account, key string) (int64, bool) {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	_, lws := acc.lws[key]
	return acc.rm[key], lws
}
