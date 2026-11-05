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
	"strings"
	"testing"
	"time"
)

// createLeafMappingSmapPair creates a hub with a leaf listener and a spoke
// connected to it, both using the global account.
func createLeafMappingSmapPair(t *testing.T) (*Server, *Server) {
	t.Helper()

	stmpl := `
		listen: 127.0.0.1:-1
		server_name: test-server
		leaf { listen: 127.0.0.1:-1 }
	`
	conf := createConfFile(t, []byte(stmpl))
	s, o := RunServerWithConfig(conf)

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: test-leaf
		leaf {
			remotes: [ { url: "nats-leaf://127.0.0.1:{LEAF_PORT}" } ]
		}
	`
	tmpl = strings.Replace(tmpl, "{LEAF_PORT}", fmt.Sprintf("%d", o.LeafNode.Port), 1)
	lConf := createConfFile(t, []byte(tmpl))
	l, _ := RunServerWithConfig(lConf)

	checkLeafNodeConnected(t, l)
	return s, l
}

// A mapping source that already has a live subscription must get its own
// announced-interest unit. Removing the mapping must therefore leave the
// subscription's interest in place instead of sending LS- for the subject.
func TestLeafNodeMappingRemoveKeepsExistingSubInterest(t *testing.T) {
	s, l := createLeafMappingSmapPair(t)
	defer s.Shutdown()
	defer l.Shutdown()

	// Local subscriber on the hub for the mapping source.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "source1")
	natsFlush(t, nc)
	checkSubInterest(t, l, globalAccountName, "source1", 2*time.Second)

	// Publisher behind the leaf.
	ncl := natsConnect(t, l.ClientURL())
	defer ncl.Close()
	ncl.Publish("source1", []byte("one"))
	checkSubsPending(t, sub, 1)

	// Add and then remove a mapping for the same source that has a live sub.
	require_NoError(t, s.globalAccount().AddMapping("source1", "target"))
	require_True(t, s.globalAccount().RemoveMapping("source1"))

	// Inspect the hub's leaf smap: it must still count the local sub.
	s.mu.Lock()
	var leafs []*client
	for _, c := range s.leafs {
		leafs = append(leafs, c)
	}
	s.mu.Unlock()
	require_Equal(t, len(leafs), 1)
	c := leafs[0]
	c.mu.Lock()
	n := c.leaf.smap["source1"]
	c.mu.Unlock()
	require_Equal(t, n, int64(1))

	// End to end: the second message must still cross the leaf.
	ncl.Publish("source1", []byte("two"))
	checkSubsPending(t, sub, 2)

	// Removing the last subscription must now withdraw the interest exactly
	// once (the mapping contribution was already taken back above).
	require_NoError(t, sub.Unsubscribe())
	natsFlush(t, nc)
	checkSubNoInterest(t, l, globalAccountName, "source1", 2*time.Second)
}

// A mapping added while a source subscription exists must not lose its own
// interest when that subscription goes away.
func TestLeafNodeMappingInterestSurvivesSourceSubRemoval(t *testing.T) {
	s, l := createLeafMappingSmapPair(t)
	defer s.Shutdown()
	defer l.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	dsub := natsSubSync(t, nc, "target")
	ssub := natsSubSync(t, nc, "source1")
	natsFlush(t, nc)
	checkSubInterest(t, l, globalAccountName, "source1", 2*time.Second)

	ncl := natsConnect(t, l.ClientURL())
	defer ncl.Close()

	require_NoError(t, s.globalAccount().AddMapping("source1", "target"))
	// Remove the subscription that existed before the mapping was added.
	require_NoError(t, ssub.Unsubscribe())
	natsFlush(t, nc)

	// The mapping must still keep the interest alive.
	checkSubInterest(t, l, globalAccountName, "source1", 2*time.Second)
	ncl.Publish("source1", []byte("mapped"))
	checkSubsPending(t, dsub, 1)
}

// Control: without a subscription on the source, adding and removing a mapping
// announces and withdraws the interest normally.
func TestLeafNodeMappingAddRemoveNoSub(t *testing.T) {
	s, l := createLeafMappingSmapPair(t)
	defer s.Shutdown()
	defer l.Shutdown()

	ncl := natsConnect(t, l.ClientURL())
	defer ncl.Close()

	require_NoError(t, s.globalAccount().AddMapping("source1", "target"))
	checkSubInterest(t, l, globalAccountName, "source1", 2*time.Second)
	require_True(t, s.globalAccount().RemoveMapping("source1"))
	checkSubNoInterest(t, l, globalAccountName, "source1", 2*time.Second)

	ncl.Publish("source1", []byte("ok"))
}
