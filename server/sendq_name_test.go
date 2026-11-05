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

import "testing"

// TestSendQIPQueueNamesPerAccount verifies that every per-account send queue
// gets its own ipQueue name, so that /ipqueuesz (and the $SYS IPQUEUESZ
// request) can report each account's outbound pipeline instead of only the
// one that was registered last.
func TestSendQIPQueueNamesPerAccount(t *testing.T) {
	opts := DefaultOptions()
	opts.Cluster = ClusterOpts{}
	s, err := NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}

	var accs []*Account
	for _, name := range []string{"A", "B"} {
		acc, err := s.RegisterAccount(name)
		if err != nil {
			t.Fatalf("Error registering account %q: %v", name, err)
		}
		accs = append(accs, acc)
	}

	// This is the same call the system account setup (server.go:1898) and
	// defaultTransport.Reset (raft_transport.go:92) make.
	for _, acc := range accs {
		s.newSendQ(acc)
	}

	queues := s.Ipqueuesz(&IpqueueszOptions{All: true, Filter: "SendQ"})
	if len(*queues) != len(accs) {
		names := make([]string, 0, len(*queues))
		for name := range *queues {
			names = append(names, name)
		}
		t.Fatalf("Expected %d SendQ ipQueues (one per account), got %d: %v",
			len(accs), len(*queues), names)
	}
}
