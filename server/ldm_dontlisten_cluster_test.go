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

// TestShutdownWithDontListenAndCluster verifies that a server started with
// Options.DontListen and a cluster port can be shut down.
//
// Start() only closes the clientListenReady channel from AcceptLoop, and
// AcceptLoop is skipped when DontListen is set. StartRouting therefore blocks
// forever on <-clientListenReady (route.go), which makes Shutdown() block in
// grWG.Wait() and WaitForShutdown() never return. Related upstream issue:
// #5174 ("Embedded nats server with opts.DontListen=true fails to connect to
// cluster"), which does not mention the shutdown hang.
func TestShutdownWithDontListenAndCluster(t *testing.T) {
	opts := DefaultOptions() // Cluster.Port = -1
	opts.DontListen = true

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
		// Do not call Shutdown() again from the deferred path: it would block.
		t.Fatalf("Shutdown/WaitForShutdown hung with DontListen + cluster port")
	}
}
