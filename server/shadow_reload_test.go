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

const shadowReloadConf = `
	port: -1
	cluster {
		name: "A"
		listen: "127.0.0.1:-1"
	}
	accounts {
		EA {
			users: [{user: a, password: pwd}]
			exports: [{stream: "foo.>"}]
		}
		IB {
			users: [{user: b, password: pwd}]
			imports: [{stream: {account: EA, subject: "foo.>"}, prefix: "import"}]
		}
	}
`

const shadowReloadConfNoImport = `
	port: -1
	cluster {
		name: "A"
		listen: "127.0.0.1:-1"
	}
	accounts {
		EA {
			users: [{user: a, password: pwd}]
			exports: [{stream: "foo.>"}]
		}
		IB {
			users: [{user: b, password: pwd}]
		}
	}
`

// When a stream import is removed by a config reload, the client's shadow
// subscriptions are removed from the exporting account's sublist, but the
// interest they registered (account rm/lws, routes, gateways, leafnodes) is
// not withdrawn. Check the exporting account's remote-interest map directly.
func TestStreamImportReloadRemovesRouteInterest(t *testing.T) {
	s, _, confFile := runReloadServerWithContent(t, []byte(shadowReloadConf))
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("b", "pwd"))
	defer nc.Close()
	natsSubSync(t, nc, "import.foo.bar")
	natsFlush(t, nc)

	ea, err := s.LookupAccount("EA")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	ea.mu.RLock()
	n := len(ea.rm)
	rm := make(map[string]int64, len(ea.rm))
	for k, v := range ea.rm {
		rm[k] = v
	}
	ea.mu.RUnlock()
	if n == 0 {
		t.Fatalf("Expected route interest in exporting account")
	}
	t.Logf("EA.rm before reload: %v", rm)

	// Reload with the stream import removed.
	if err := os.WriteFile(confFile, []byte(shadowReloadConfNoImport), 0666); err != nil {
		t.Fatalf("Error rewriting conf: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// The shadow subscription should be gone from the exporting account...
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		var subs []*subscription
		ea.sl.All(&subs)
		for _, sub := range subs {
			if string(sub.subject) == "foo.bar" {
				return fmt.Errorf("shadow subscription not removed")
			}
		}
		return nil
	})

	// ... and so should the interest it registered for the route network.
	ea.mu.RLock()
	rm = make(map[string]int64, len(ea.rm))
	for k, v := range ea.rm {
		rm[k] = v
	}
	ea.mu.RUnlock()
	if len(rm) != 0 {
		t.Fatalf("Ghost route interest in exporting account after reload: %v", rm)
	}
}

// Same as above, but with a queue subscription: the exported queue weight
// advertised to routes (acc.rm/acc.lws) must also return to zero.
func TestStreamImportReloadRemovesQueueInterest(t *testing.T) {
	s, _, confFile := runReloadServerWithContent(t, []byte(shadowReloadConf))
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("b", "pwd"))
	defer nc.Close()
	sub := natsQueueSubSync(t, nc, "import.foo.bar", "workers")
	natsFlush(t, nc)

	ea, err := s.LookupAccount("EA")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	key := "R foo.bar workers"
	ea.mu.RLock()
	n, w := ea.rm[key], ea.lws[key]
	ea.mu.RUnlock()
	if n != 1 {
		t.Fatalf("Expected queue route interest for %q, got %d", key, n)
	}

	if err := os.WriteFile(confFile, []byte(shadowReloadConfNoImport), 0666); err != nil {
		t.Fatalf("Error rewriting conf: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		var subs []*subscription
		ea.sl.All(&subs)
		for _, sub := range subs {
			if string(sub.subject) == "foo.bar" {
				return fmt.Errorf("shadow queue subscription not removed")
			}
		}
		return nil
	})

	ea.mu.RLock()
	n, w = ea.rm[key], ea.lws[key]
	ea.mu.RUnlock()
	if n != 0 {
		t.Fatalf("Ghost queue route interest for %q after reload: rm=%d lws=%d", key, n, w)
	}

	// A later call to the client's own UNSUB must not resurrect it either.
	natsUnsub(t, sub)
	ea.mu.RLock()
	n, w = ea.rm[key], ea.lws[key]
	ea.mu.RUnlock()
	if n != 0 {
		t.Fatalf("Ghost queue route interest for %q after unsub: rm=%d lws=%d", key, n, w)
	}
}

// Peer-visible form: the route peer must drop its remote interest for the
// exported subject once the import is removed on the importing server.
func TestStreamImportReloadWithdrawsPeerInterest(t *testing.T) {
	s1, _, confFile := runReloadServerWithContent(t, []byte(shadowReloadConf))
	defer s1.Shutdown()
	cport := s1.ClusterAddr().Port

	conf2 := fmt.Sprintf(`
		port: -1
		cluster {
			name: "A"
			listen: "127.0.0.1:-1"
			routes: ["nats-route://127.0.0.1:%d"]
		}
		accounts {
			EA {
				users: [{user: a, password: pwd}]
				exports: [{stream: "foo.>"}]
			}
			IB {
				users: [{user: b, password: pwd}]
				imports: [{stream: {account: EA, subject: "foo.>"}, prefix: "import"}]
			}
		}
	`, cport)
	s2, _ := RunServerWithConfig(createConfFile(t, []byte(conf2)))
	defer s2.Shutdown()

	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		if s1.NumRoutes() == 0 || s2.NumRoutes() == 0 {
			return fmt.Errorf("routes not up: s1=%d s2=%d", s1.NumRoutes(), s2.NumRoutes())
		}
		return nil
	})

	nc := natsConnect(t, s1.ClientURL(), nats.UserInfo("b", "pwd"))
	defer nc.Close()
	natsSubSync(t, nc, "import.foo.bar")
	natsFlush(t, nc)

	ea2, err := s2.LookupAccount("EA")
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		if len(ea2.sl.Match("foo.bar").psubs) == 0 {
			return fmt.Errorf("peer has no interest yet")
		}
		return nil
	})

	confReload := fmt.Sprintf(`
		port: -1
		cluster {
			name: "A"
			listen: "127.0.0.1:%d"
		}
		accounts {
			EA {
				users: [{user: a, password: pwd}]
				exports: [{stream: "foo.>"}]
			}
			IB {
				users: [{user: b, password: pwd}]
			}
		}
	`, cport)
	if err := os.WriteFile(confFile, []byte(confReload), 0666); err != nil {
		t.Fatalf("Error rewriting conf: %v", err)
	}
	if err := s1.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if n := len(ea2.sl.Match("foo.bar").psubs); n != 0 {
			return fmt.Errorf("peer still has %d interest sub(s) for foo.bar", n)
		}
		return nil
	})
}
