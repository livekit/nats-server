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
	"net/url"
	"testing"
	"time"
)

// A leafnode cluster splayed across two gateway regions. Each leaf server
// homes on a different region, so a message published on one leaf server can
// reach a subscriber on the other leaf server over two paths: the leaf cluster
// route directly, and leaf -> region -> gateway -> region -> leaf.
//
// A plain subscriber must still receive exactly one copy of each message. See
// https://github.com/nats-io/nats-server/issues/8339.
func TestLeafNodeSplayedAcrossGatewaysNoDuplicateDelivery(t *testing.T) {
	SetGatewaysSolicitDelay(0)
	defer ResetGatewaysSolicitDelay()

	withSystemAccount := func(o *Options) {
		o.Accounts = []*Account{NewAccount("SYS")}
		o.SystemAccount = "SYS"
	}

	// Region A: gateway endpoint plus the leafnode listener for leaf1.
	ao := testDefaultOptionsForGateway("A")
	ao.ServerName = "A"
	ao.LeafNode.Host = "127.0.0.1"
	ao.LeafNode.Port = -1
	withSystemAccount(ao)
	a := runGatewayServer(ao)
	defer a.Shutdown()

	ua, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ao.LeafNode.Port))

	// Region B: the other gateway endpoint, leafnode listener for leaf2.
	bo := testGatewayOptionsFromToWithServers(t, "B", "A", a)
	bo.ServerName = "B"
	bo.LeafNode.Host = "127.0.0.1"
	bo.LeafNode.Port = -1
	withSystemAccount(bo)
	b := runGatewayServer(bo)
	defer b.Shutdown()

	ub, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", bo.LeafNode.Port))

	// The two leaf servers form a cluster of their own ("leafcluster") but
	// solicit different regions.
	o1 := DefaultOptions()
	o1.ServerName = "leaf1"
	o1.Cluster.Name = "leafcluster"
	o1.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ua}}}
	leaf1 := RunServer(o1)
	defer leaf1.Shutdown()

	o2 := DefaultOptions()
	o2.ServerName = "leaf2"
	o2.Cluster.Name = "leafcluster"
	o2.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: []*url.URL{ub}}}
	o2.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	leaf2 := RunServer(o2)
	defer leaf2.Shutdown()

	checkClusterFormed(t, leaf1, leaf2)
	waitForOutboundGateways(t, b, 1, 2*time.Second)
	waitForInboundGateways(t, a, 1, 2*time.Second)
	checkLeafNodeConnectedCount(t, a, 1)
	checkLeafNodeConnectedCount(t, b, 1)

	ncSub := natsConnect(t, leaf2.ClientURL())
	defer ncSub.Close()
	ncPub := natsConnect(t, leaf1.ClientURL())
	defer ncPub.Close()

	sub := natsSubSync(t, ncSub, "foo")
	natsFlush(t, ncSub)

	// Make sure the interest is visible on both paths before publishing:
	// the leaf cluster route (leaf1) and the gateway (A's outbound connection
	// to B must have learned about B's interest for "foo").
	checkSubInterest(t, leaf1, globalAccountName, "foo", 2*time.Second)
	checkSubInterest(t, leaf2, globalAccountName, "foo", 2*time.Second)
	checkGWInterestOnlyModeInterestOn(t, a, "B", globalAccountName, "foo")

	const total = 20
	for i := 0; i < total; i++ {
		natsPub(t, ncPub, "foo", []byte(fmt.Sprintf("%d", i)))
	}
	natsFlush(t, ncPub)

	seen := make(map[string]int, total)
	for len(seen) <= 2*total {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			break
		}
		seen[string(msg.Data)]++
	}

	if len(seen) != total {
		t.Fatalf("Expected %d messages, got %d", total, len(seen))
	}
	for i := 0; i < total; i++ {
		if n := seen[fmt.Sprintf("%d", i)]; n != 1 {
			t.Fatalf("Message %d was delivered %d times", i, n)
		}
	}
}
