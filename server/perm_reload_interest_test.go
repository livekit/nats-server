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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const permReloadConf = `
	port: -1
	cluster {
		name: "A"
		listen: "127.0.0.1:-1"
	}
	accounts {
		A {
			users: [{user: u, password: p}]
		}
	}
`

const permReloadConfRestricted = `
	port: -1
	cluster {
		name: "A"
		listen: "127.0.0.1:-1"
	}
	accounts {
		A {
			users: [{user: u, password: p, permissions: {subscribe: {allow: ["foo.>"]}}}]
		}
	}
`

// A subscription removed by a permission-changing reload must withdraw the
// route interest it registered (rm/lws refcounts), like every other removal
// path does.
func TestPermReloadRemovedSubWithdrawsRouteInterest(t *testing.T) {
	s, _, confFile := runReloadServerWithContent(t, []byte(permReloadConf))
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("u", "p"))
	defer nc.Close()
	natsSubSync(t, nc, "bar")
	natsFlush(t, nc)

	acc, err := s.LookupAccount("A")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	key := "R bar"
	acc.mu.RLock()
	n := acc.rm[key]
	acc.mu.RUnlock()
	if n != 1 {
		t.Fatalf("Expected route interest for %q, got %d", key, n)
	}

	if err := os.WriteFile(confFile, []byte(permReloadConfRestricted), 0666); err != nil {
		t.Fatalf("Error rewriting conf: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// The sub is no longer allowed and must be removed.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(acc.sl.Match("bar").psubs); n != 0 {
			return fmt.Errorf("sub not removed, psubs=%d", n)
		}
		return nil
	})

	acc.mu.RLock()
	n = acc.rm[key]
	acc.mu.RUnlock()
	if n != 0 {
		t.Fatalf("Ghost route interest for %q after permission reload: %d", key, n)
	}
}
