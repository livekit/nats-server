// Copyright 2025 The NATS Authors
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

// TestDebugSubscribersDuringShutdownEventing delivers a real
// $SYS.DEBUG.SUBSCRIBERS request from a system-account client and keeps the
// handler blocked until the shutdown event has been queued, i.e. while
// s.sys.replies is nil. This is the in-flight-request window of Shutdown().
func TestDebugSubscribersDuringShutdownEventing(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		system_account: SYS
		accounts {
			SYS {
				users: [ {user: sys, password: pwd} ]
			}
		}
	`))
	defer removeFile(t, conf)

	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config: %v", err)
	}
	s, err := NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	go s.Start()

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !s.ReadyForConnections(50 * time.Millisecond) {
			return fmt.Errorf("server not ready")
		}
		return nil
	})

	acc := s.SystemAccount()
	if acc == nil {
		t.Fatal("System account not set")
	}
	// Make the handler solicit remote servers, which is what makes it create
	// a reply inbox (accounts.go expectedRemoteResponses).
	acc.mu.Lock()
	acc.strack = map[string]sconns{"remote": {conns: 1}}
	acc.mu.Unlock()

	// Raw client so that we control the exact framing and know when the
	// server has processed the request.
	nc, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("Error dialing: %v", err)
	}
	defer nc.Close()
	br := bufio.NewReader(nc)
	if line, err := br.ReadString('\n'); err != nil || !strings.HasPrefix(line, "INFO ") {
		t.Fatalf("Expected INFO, got %q err=%v", line, err)
	}
	fmt.Fprintf(nc, "CONNECT {\"user\":\"sys\",\"pass\":\"pwd\",\"verbose\":false,\"pedantic\":false}\r\nPING\r\n")
	readPong := func(timeout time.Duration) error {
		nc.SetReadDeadline(time.Now().Add(timeout))
		defer nc.SetReadDeadline(time.Time{})
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return err
			}
			switch {
			case line == "PONG\r\n":
				return nil
			case strings.HasPrefix(line, "-ERR"):
				return fmt.Errorf("server error: %s", line)
			}
		}
	}
	if err := readPong(5 * time.Second); err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	// Keep the handler from reaching the reply-map write until the shutdown
	// event has unhooked it.
	acc.mu.Lock()
	// PUB and PING in one write so the read loop processes them back to back.
	fmt.Fprintf(nc, "PUB %s _INBOX.reply 3\r\nfoo\r\nPING\r\n", accSubsSubj)

	// Wait for the request to be read and the handler to be blocked on acc.mu.
	time.Sleep(100 * time.Millisecond)

	// This nils s.sys.replies while the request is still in flight.
	s.sendShutdownEvent()
	acc.mu.Unlock()

	// PONG confirms that the client read loop read the request and queued it on
	// the system receive queue (a panic in the deferred handler is the failure
	// mode this test is looking for).
	if err := readPong(5 * time.Second); err != nil {
		t.Fatalf("Error waiting for the request to be queued: %v", err)
	}

	s.Shutdown()
	s.WaitForShutdown()
	// Give the internal receive loop a chance to dispatch the request against
	// the torn-down eventing state before the test binary can exit.
	time.Sleep(50 * time.Millisecond)
}
