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
	"testing"
	"time"
)

// TestSystemAccountStartedAfterShutdown checks that a Start() call issued after
// Shutdown() has completed does not bring the system account/eventing stack up
// again.
//
// Shutdown() before Start() is an explicitly supported sequence (see
// TestServerShutdownDuringStart). When Shutdown() runs first, s.sys is still
// nil so shutdownEventing() returns early. Start() then sets s.running back to
// true and SetSystemAccount() builds a fresh internal client, send/receive
// loops with a closed quitCh, and the statsz/sweeper timers. Nothing can clean
// it up afterwards: shutdown is a one-shot CAS and a later Shutdown() returns
// immediately, so the timers keep firing and queueing messages into a send
// queue whose reader already exited.
func TestSystemAccountStartedAfterShutdown(t *testing.T) {
	o := DefaultOptions()
	o.ServerName = "server"
	o.Port = -1
	o.Accounts = []*Account{NewAccount("$SYS")}
	o.SystemAccount = "$SYS"

	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s.Shutdown()

	// Complete the shutdown before starting.
	s.Shutdown()
	s.WaitForShutdown()

	s.Start()

	if sa := s.SystemAccount(); sa != nil {
		t.Fatalf("System account %q was set up after the server was shut down", sa.GetName())
	}
	if s.sys != nil && s.sys.stmr != nil {
		// The statsz heartbeat is armed even though quitCh is closed. Show
		// that it is actually running: wait for at least one tick.
		s.mu.RLock()
		last := s.sys.lastStatsz
		s.mu.RUnlock()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			s.mu.RLock()
			advanced := s.sys.lastStatsz.After(last)
			s.mu.RUnlock()
			if advanced {
				t.Fatalf("statsz heartbeat still running after the server was shut down")
			}
		}
	}
}

// TestJetStreamStartedAfterShutdown is the same sequence with JetStream
// configured: Start() after Shutdown() must not enable JetStream on a server
// that has already signalled shutdownComplete, since nothing will ever stop it
// (Shutdown() becomes a no-op). The store directory is created and the
// JetStream subsystem is reported enabled by the public API.
func TestJetStreamStartedAfterShutdown(t *testing.T) {
	o := DefaultOptions()
	o.ServerName = "server"
	o.Port = -1
	o.JetStream = true
	o.StoreDir = t.TempDir()

	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s.Shutdown()

	s.Shutdown()
	s.WaitForShutdown()

	s.Start()

	if s.JetStreamEnabled() {
		t.Fatalf("JetStream was enabled after the server was shut down (store %q)", o.StoreDir)
	}
	if ports := s.PortsInfo(250 * time.Millisecond); ports != nil && len(ports.Nats) != 0 {
		t.Fatalf("Server reported listening ports after shutdown: %v", ports)
	}
}
