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

// TestDebugSubscribersAfterShutdownEvent checks that the $SYS.DEBUG.SUBSCRIBERS
// handler tolerates the state reached during shutdown, after
// sendShutdownEvent() has unhooked the reply map (s.sys.replies = nil) while
// the system client and its subscriptions are still alive.
func TestDebugSubscribersAfterShutdownEvent(t *testing.T) {
	sys := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		system_account: SYS
		accounts {
			SYS {}
		}
	`))
	_ = sys
	defer removeFile(t, sys)

	opts, err := ProcessConfigFile(sys)
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
	// Pretend a remote server reported connections for this account, which is
	// what makes the SUBSZ handler solicit remote servers and register a reply
	// inbox.
	acc.mu.Lock()
	acc.strack = map[string]sconns{"remote": {conns: 1}}
	acc.mu.Unlock()

	// This is what shutdownEventing() does before waiting for the internal send
	// loop: unhook the reply map while the system client is still alive and can
	// still deliver subscription callbacks.
	s.sendShutdownEvent()

	// A $SYS.DEBUG.SUBSCRIBERS request arriving now must not panic.
	s.debugSubscribers(nil, &client{kind: CLIENT}, nil, accSubsSubj, "$SYS.reply", nil, []byte("foo"))

	s.Shutdown()
}
