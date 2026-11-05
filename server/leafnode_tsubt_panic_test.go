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

// removeLeafNodeConnection() stops and clears leaf.tsubt but leaves leaf.tsub
// populated, and the client is still in acc.lleafs until acc.removeClient()
// runs later in closeConnection(). A +1 interest update for a sub that the
// init snapshot recorded then takes the "counted during init" branch in
// updateSmap and calls c.leaf.tsubt.Stop() on the nil timer, which panics with
// a nil pointer dereference.
func TestLeafNodeSmapUpdateAfterLeafCleanupPanics(t *testing.T) {
	hubConf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: tsub-hub
		leaf { listen: 127.0.0.1:-1 }
	`))
	hub, o := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	// Local subscribers that the leaf init snapshot will record in tsub.
	nc := natsConnect(t, hub.ClientURL())
	defer nc.Close()
	for i := 0; i < 4; i++ {
		natsSubSync(t, nc, fmt.Sprintf("foo.%d", i))
	}
	natsFlush(t, nc)

	spokeConf := createConfFile(t, []byte(strings.Replace(`
		listen: 127.0.0.1:-1
		server_name: tsub-spoke
		leaf {
			remotes: [ { url: "nats-leaf://127.0.0.1:{LEAF_PORT}" } ]
		}
	`, "{LEAF_PORT}", fmt.Sprintf("%d", o.LeafNode.Port), 1)))
	spoke, _ := RunServerWithConfig(spokeConf)
	defer spoke.Shutdown()
	checkLeafNodeConnected(t, spoke)

	lc := leafForRemoteServer(t, hub, "tsub-spoke")
	if lc == nil {
		t.Fatalf("no hub-side leaf connection")
	}

	// Wait for init to have built the snapshot.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if _, ok := leafSmapValue(lc, "foo.0"); !ok {
			return fmt.Errorf("leaf smap not built yet")
		}
		return nil
	})

	lc.mu.Lock()
	subs := make([]*subscription, 0, len(lc.leaf.tsub))
	for s := range lc.leaf.tsub {
		subs = append(subs, s)
	}
	tsubt := lc.leaf.tsubt
	lc.mu.Unlock()
	if len(subs) == 0 {
		t.Fatalf("init snapshot did not record any sub in tsub")
	}
	if tsubt == nil {
		t.Fatalf("tsub populated but tsubt is nil before cleanup")
	}

	// First stage of closeConnection(): stop the timer, leave the client
	// registered with the account, as the real close does before calling
	// acc.removeClient().
	hub.removeLeafNodeConnection(lc)

	// The connection is half closed now: let it finish its own cleanup so the
	// servers can shut down even if the checks below fail. This defer is
	// registered after the server shutdowns, so it runs before them.
	defer func() {
		lc.setNoReconnect()
		lc.closeConnection(ClientClosed)
	}()

	// The pair is inconsistent on main: tsub still holds every recorded sub
	// while the timer is gone. A fix may either clear tsub here or make the
	// updateSmap branch nil-safe; both keep the checks below meaningful.
	lc.mu.Lock()
	haveTsub, haveTsubt := lc.leaf.tsub != nil, lc.leaf.tsubt != nil
	lc.mu.Unlock()
	t.Logf("after removeLeafNodeConnection: tsub=%v tsubt=%v (recorded subs: %d)",
		haveTsub, haveTsubt, len(subs))

	acc := hub.globalAccount()
	var (
		panicVal any
		panicIdx int
	)
	for i, sub := range subs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicVal, panicIdx = r, i+1
				}
			}()
			// A subscription added while the leaf was initialising delivers
			// its pending +1 update through the normal path.
			acc.updateLeafNodes(sub, 1)
		}()
		if panicVal != nil {
			break
		}
	}
	if panicVal != nil {
		// updateLeafNodesEx holds the leaf lock across the updateSmap call and
		// unlocks it right after, so the recovered panic left it locked. A real
		// server has no recover here: the panic takes the process down. Release
		// it so the deferred connection close and server shutdown can finish.
		lc.mu.Unlock()
		t.Fatalf("panic in updateSmap for tsub entry %d/%d after removeLeafNodeConnection: %v",
			panicIdx, len(subs), panicVal)
	}
}
