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

	"github.com/nats-io/nats.go"
)

// TestLameDuckShutdownWithDontListen verifies that LameDuckShutdown()
// performs a real lame duck shutdown (and terminates the server) when the
// server was started with Options.DontListen, i.e. without a client listener.
//
// LameDuckShutdown is documented to block until the server has completed the
// entire shutdown operation, so on return the server must not be running and
// in-process clients must have been disconnected.
func TestLameDuckShutdownWithDontListen(t *testing.T) {
	opts := DefaultOptions()
	opts.DontListen = true
	opts.NoSigs = true
	opts.NoLog = true
	// Isolate from the unrelated DontListen+cluster bug (upstream #5174), where
	// StartRouting blocks on clientListenReady forever.
	opts.Cluster = ClusterOpts{}
	opts.LameDuckDuration = 500 * time.Millisecond
	opts.LameDuckGracePeriod = -100 * time.Millisecond

	s, err := NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	go s.Start()
	defer s.Shutdown()

	// DontListen means no client listener, so wait for the server to be
	// running and retry the in-process connection until Start() has enabled
	// the go-routine tracking.
	var nc *nats.Conn
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !s.Running() {
			return fmt.Errorf("server not running yet")
		}
		var err error
		nc, err = nats.Connect(_EMPTY_,
			nats.InProcessServer(s),
			nats.NoReconnect(),
			nats.ClosedHandler(func(_ *nats.Conn) {}),
		)
		return err
	})
	defer nc.Close()

	// Make sure the server sees the client before entering lame duck mode.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := s.NumClients(); n != 1 {
			return fmt.Errorf("expected 1 client, got %d", n)
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		s.LameDuckShutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("LameDuckShutdown did not return")
	}

	if s.Running() {
		t.Fatalf("Server still running after LameDuckShutdown returned")
	}
	if !nc.IsClosed() {
		t.Fatalf("Client was not disconnected by lame duck shutdown")
	}
}
