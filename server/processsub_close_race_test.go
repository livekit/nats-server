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
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

const processSubCloseRaceConf = `
	port: -1
	cluster {
		name: "A"
		listen: "127.0.0.1:-1"
	}
	accounts {
		EXP {
			users: [{user: exp, password: pwd}]
			exports: [{stream: "foo.>"}]
		}
		IMP {
			users: [{user: imp, password: pwd}]
			imports: [{stream: {account: EXP, subject: "foo.>"}, prefix: "imp"}]
		}
	}
`

func writeAndFlush(conn net.Conn, b []byte) error {
	_, err := conn.Write(b)
	return err
}

// processSubSnapshot reads the account sublist directly: it must not take
// acc.mu, since the test holds that write lock while parking the SUB.
func processSubSnapshot(acc *Account, subj string) []*subscription {
	sr := acc.sl.Match(subj)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	return subs
}

// processSubEx inserts the sub into c.subs and the account sublist under c.mu,
// then unlocks and only afterwards creates the stream-import shadows and
// announces route/gateway/leaf +1 interest. A close that runs in between
// removes and closes the sub but sees sub.shadow == nil, so the shadows the SUB
// is still creating are inserted for a client that is already gone: the
// exporting account keeps a shadow and its +1 with no owner, and c.subs is nil
// by the time the shadow handle is assigned, so nothing can ever clean it up.
//
// The interleaving is forced deterministically by holding the importing
// account's lock: addShadowSubscriptions takes acc.mu.RLock right after the
// insert and before creating any shadow, so the SUB parks there. Disconnecting
// the client then removes the original and closes it; releasing the lock lets
// the parked SUB insert the shadow for the dead sub.
func TestProcessSubPublishesShadowForClosedClient(t *testing.T) {
	s, opts := RunServerWithConfig(createConfFile(t, []byte(processSubCloseRaceConf)))
	defer s.Shutdown()

	// Raw socket so we control exactly when the SUB line is written and do not
	// depend on nats.go helpers that would wait for a PONG.
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", opts.Port)))
	require_NoError(t, err)
	defer conn.Close()
	br := bufio.NewReader(conn)

	require_NoError(t, writeAndFlush(conn, []byte("CONNECT {\"user\":\"imp\",\"pass\":\"pwd\",\"verbose\":false}\r\nPING\r\n")))
	for {
		line, err := br.ReadString('\n')
		require_NoError(t, err)
		if strings.TrimSpace(line) == "PONG" {
			break
		}
	}

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)
	impAcc, err := s.LookupAccount("IMP")
	require_NoError(t, err)

	// Find the importing client before the SUB.
	var c *client
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		impAcc.mu.RLock()
		for cl := range impAcc.clients {
			if cl.kind == CLIENT {
				c = cl
			}
		}
		impAcc.mu.RUnlock()
		if c == nil {
			return fmt.Errorf("importing client not registered yet")
		}
		return nil
	})

	// Park the SUB inside addShadowSubscriptions.
	impAcc.mu.Lock()
	require_NoError(t, writeAndFlush(conn, []byte("SUB imp.foo.bar 1\r\n")))

	// The sub is in c.subs (the insert happens before the parked call) but no
	// shadow exists yet.
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		c.mu.Lock()
		n := len(c.subs)
		c.mu.Unlock()
		if n != 1 {
			return fmt.Errorf("client tracks %d subs, want 1", n)
		}
		return nil
	})
	if n := len(processSubSnapshot(expAcc, "foo.bar")); n != 0 {
		t.Fatalf("EXP already has %d shadow sub(s) before the SUB parked", n)
	}

	// Disconnect the client while the SUB is parked. It removes the original
	// from IMP and closes it, but sees no shadow handle yet.
	disconnected := make(chan error, 1)
	go func() { disconnected <- s.DisconnectClientByID(c.cid) }()
	checkFor(t, 2*time.Second, time.Millisecond, func() error {
		if n := len(processSubSnapshot(impAcc, "imp.foo.bar")); n != 0 {
			return fmt.Errorf("original sub still in IMP: %d", n)
		}
		return nil
	})

	// Let the parked SUB finish: it creates the shadow for the closed client.
	impAcc.mu.Unlock()
	select {
	case err := <-disconnected:
		require_NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for the client disconnect")
	}

	// The parked SUB finishes asynchronously after the account lock is
	// released: it must not leave a shadow or remote interest for the closed
	// client. On main the leak is permanent, so this check times out.
	key := keyFromSubWithOrigin(&subscription{subject: []byte("foo.bar")})
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		expSubs := processSubSnapshot(expAcc, "foo.bar")
		for _, shadow := range expSubs {
			t.Logf("shadow for closed client: sid=%q subject=%q closed=%v clientClosed=%v",
				shadow.sid, shadow.subject, shadow.isClosed(),
				shadow.client == nil || shadow.client.isClosed())
		}
		expAcc.mu.RLock()
		rm, lws := expAcc.rm[key], expAcc.lws[key]
		expAcc.mu.RUnlock()
		if len(expSubs) != 0 {
			return fmt.Errorf("EXP has %d shadow sub(s) for a disconnected client (rm=%d lws=%d)", len(expSubs), rm, lws)
		}
		if rm != 0 || lws != 0 {
			return fmt.Errorf("EXP announces interest for a disconnected client: rm=%d lws=%d", rm, lws)
		}
		return nil
	})

	c.mu.Lock()
	nsubs := len(c.subs)
	c.mu.Unlock()
	if nsubs != 0 {
		t.Fatalf("closed client still tracks %d subs", nsubs)
	}
}
