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
	"fmt"
	"testing"
	"time"
)

// teardownEventingForTest reproduces the state left behind by
// shutdownEventing() (events.go:1904-1938) without waiting for it: the reply
// map and send queue are unhooked by sendShutdownEvent() and then s.sys is set
// to nil, while the internal receive loop can still be dispatching messages
// that were queued before the teardown.
func teardownEventingForTest(s *Server) {
	s.sendShutdownEvent()
	s.mu.Lock()
	s.sys = nil
	s.mu.Unlock()
}

// TestLeafNodeConnectedAfterShutdownEventing checks that the deferred
// $SYS.ACCOUNT.<acc>.LEAFNODE.CONNECT handler (registered with
// noInlineCallback at events.go:1490) does not dereference s.sys when it runs
// after eventing was torn down. It reports JSON errors through s.sys.client
// without any state check, so a malformed payload panics the server.
func TestLeafNodeConnectedAfterShutdownEventing(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		system_account: SYS
		accounts {
			SYS {}
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
	defer func() {
		s.Shutdown()
		s.WaitForShutdown()
	}()

	teardownEventingForTest(s)

	// Any leaf node connect event that was already queued on the internal
	// receive queue can be dispatched now. A malformed payload makes the
	// handler report through the system client, which no longer exists.
	s.leafNodeConnected(nil, nil, nil, fmt.Sprintf(leafNodeConnectEventSubj, "X"), _EMPTY_, nil, []byte("not json"))
}
