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
	"testing"
	"time"
)

// When a client closes, clearAccountSubs removes its subscriptions from the
// account sublist with RemoveBatch and only then feeds the -delta updates to
// the leaf connections, with queue updates aggregated and delivered at the very
// end. A leaf connection that runs initLeafNodeSmapAndSendSubs inside that
// window snapshots the sublist without the closing client's subs, so it never
// counted them, yet it still receives their -delta. If another leaf has live
// interest for the same key, the delta lands on that count, drives it to zero
// and the hub sends LS-, so the new leaf stops forwarding a subject for which
// live interest still exists.
func TestLeafNodeSmapCloseDeltaStealsNewLeafInterest(t *testing.T) {
	const fillers = 2
	const closingSubs = 20000
	const attempts = 3

	hubConf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: cd-hub
		leaf { listen: 127.0.0.1:-1 }
	`))
	hub, o := RunServerWithConfig(hubConf)
	defer hub.Shutdown()

	spokeConf := func(name string) []byte {
		return []byte(fmt.Sprintf(`
			listen: 127.0.0.1:-1
			server_name: %s
			leaf {
				remotes: [ { url: "nats-leaf://127.0.0.1:%d" } ]
			}
		`, name, o.LeafNode.Port))
	}

	spokeZ, _ := RunServerWithConfig(createConfFile(t, spokeConf("cd-z")))
	defer spokeZ.Shutdown()
	checkLeafNodeConnected(t, spokeZ)
	ncZ := natsConnect(t, spokeZ.ClientURL())
	defer ncZ.Close()
	mZ := natsQueueSubSync(t, ncZ, "foo", "qq")
	natsFlush(t, ncZ)

	// Extra leaf connections make every interest delta walk more connections,
	// widening the gap between RemoveBatch and the deferred queue delta.
	for i := 0; i < fillers; i++ {
		fs, _ := RunServerWithConfig(createConfFile(t, spokeConf(fmt.Sprintf("cd-fill-%d", i))))
		defer fs.Shutdown()
		checkLeafNodeConnected(t, fs)
	}

	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := hubTotalQW(t, hub, "foo qq"); n != 1 {
			return fmt.Errorf("stored hub weight for foo qq = %d, want 1", n)
		}
		return nil
	})

	for i := 0; i < attempts; i++ {
		ncX := natsConnect(t, hub.ClientURL())
		for j := 0; j < closingSubs; j++ {
			if _, err := ncX.SubscribeSync(fmt.Sprintf("x.%06d", j)); err != nil {
				t.Fatalf("closing subscribe: %v", err)
			}
		}
		natsQueueSubSync(t, ncX, "foo", "qq")
		natsFlush(t, ncX)
		checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			if n := hub.globalAccount().Interest("foo"); n != 2 {
				return fmt.Errorf("hub interest for foo = %d, want 2", n)
			}
			// Make sure the hub parsed every SUB before the close so that the
			// close reaches clearAccountSubs promptly.
			if n := hub.globalAccount().Interest("x.019999"); n != 1 {
				return fmt.Errorf("hub has not parsed the last closing sub yet")
			}
			return nil
		})

		// Start the close first, then the new leaf, so that the leaf's init
		// lands between RemoveBatch and the deferred queue delta.
		ncX.Close()
		time.Sleep(time.Duration(i) * time.Millisecond)
		name := fmt.Sprintf("cd-y-%d", i)
		spokeY, _ := RunServerWithConfig(createConfFile(t, spokeConf(name)))
		checkLeafNodeConnected(t, spokeY)
		time.Sleep(500 * time.Millisecond)

		lcY := leafForRemoteServer(t, hub, name)
		if lcY == nil {
			spokeY.Shutdown()
			t.Fatalf("attempt %d: no hub-side leaf connection for %s", i, name)
		}
		n, _ := leafSmapValue(lcY, "foo qq")
		yInterest := spokeY.globalAccount().Interest("foo")
		ncY := natsConnect(t, spokeY.ClientURL())
		ncY.Publish("foo", []byte("hello"))
		ncY.Flush()
		ncY.Close()
		var delivered int
		for deadline := time.Now().Add(2 * time.Second); delivered == 0 && time.Now().Before(deadline); {
			if p, _, _ := mZ.Pending(); p > 0 {
				for ; p > 0; p-- {
					if _, err := mZ.NextMsg(time.Second); err == nil {
						delivered++
					}
				}
			} else {
				time.Sleep(5 * time.Millisecond)
			}
		}
		if n != 1 || delivered != 1 {
			spokeY.Shutdown()
			t.Fatalf("attempt %d: new leaf announces weight %d (want 1) for foo qq, holds %d foo subs and delivered %d/1; its close-window deltas stole the count of a live queue group",
				i, n, yInterest, delivered)
		}
		spokeY.Shutdown()
	}
}
