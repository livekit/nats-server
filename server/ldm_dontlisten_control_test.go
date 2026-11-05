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

// Controls for the DontListen + cluster shutdown hang: each shape must shut
// down cleanly on unmodified main.
//
// TestShutdownControlListenWithCluster proves that a cluster port alone does
// not hang shutdown (the client accept loop closes clientListenReady).
func TestShutdownControlListenWithCluster(t *testing.T) {
	opts := DefaultOptions() // Cluster.Port = -1
	opts.DontListen = false
	opts.Port = -1

	s, err := NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	go s.Start()

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !s.Running() {
			return fmt.Errorf("server not running")
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		s.Shutdown()
		s.WaitForShutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Shutdown/WaitForShutdown hung with a client listener and cluster port")
	}
}

// TestShutdownControlDontListenNoCluster proves that DontListen alone does not
// hang shutdown (no StartRouting goroutine is waiting for clientListenReady).
func TestShutdownControlDontListenNoCluster(t *testing.T) {
	opts := DefaultOptions()
	opts.DontListen = true
	opts.Cluster = ClusterOpts{}

	s, err := NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	go s.Start()

	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !s.Running() {
			return fmt.Errorf("server not running")
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		s.Shutdown()
		s.WaitForShutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Shutdown/WaitForShutdown hung with DontListen and no cluster port")
	}
}
