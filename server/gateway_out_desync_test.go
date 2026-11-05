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
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// desyncOutboundGateway creates a second outbound gateway connection for a
// remote gateway name that is already registered. The duplicate is rejected by
// registerOutboundGatewayConnection() (the setNoReconnect()+closeConnection()
// branch in processGatewayInfo) and its close runs
// removeRemoteGatewayConnection(), which does `delete(gw.out, gwName)` without
// checking that the client being removed is the one registered under that name.
//
// The live connection stays in `outo` so it keeps carrying messages, but the
// `out` map loses it.
func desyncOutboundGateway(t *testing.T, s *Server, gwName string, remote *Server) {
	t.Helper()
	cfg := s.getRemoteGateway(gwName)
	require_True(t, cfg != nil)
	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", remote.GatewayAddr().Port))
	require_NoError(t, err)
	conn, err := net.Dial("tcp", remote.GatewayAddr().String())
	require_NoError(t, err)
	base := tempClientCount(s)
	s.createGateway(cfg, u, conn)

	// The duplicate completes its handshake asynchronously. Wait for it to be
	// added to the temp clients and then removed by its close path, which is
	// when the (possibly wrong) map update has happened.
	deadline := time.Now().Add(2 * time.Second)
	for tempClientCount(s) == base && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	for tempClientCount(s) > base && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if tempClientCount(s) != base {
		t.Fatalf("duplicate outbound did not finish within 2s")
	}
}

func tempClientCount(s *Server) int {
	s.grMu.Lock()
	n := len(s.grTmpClients)
	s.grMu.Unlock()
	return n
}

// TestGatewayOutboundUnregisteredCloseKeepsLiveEntry creates a second outbound
// gateway connection for a remote gateway name that is already registered and
// checks that its close must not remove the live connection from the maps.
func TestGatewayOutboundUnregisteredCloseKeepsLiveEntry(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)
	defer sa.Shutdown()

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// Capture the live outbound connection: both maps must agree on it.
	sa.gateway.RLock()
	live := sa.gateway.out["B"]
	outoLen := len(sa.gateway.outo)
	sa.gateway.RUnlock()
	require_True(t, live != nil)
	require_Equal(t, outoLen, 1)

	desyncOutboundGateway(t, sa, "B", sb)

	sa.gateway.RLock()
	cur, outo := sa.gateway.out["B"], len(sa.gateway.outo)
	sa.gateway.RUnlock()

	if cur != live {
		t.Logf("NumOutboundGateways() = %v", sa.NumOutboundGateways())

		// Check whether Shutdown can still find the connection: it only closes
		// the connections found in s.gateway.out and s.gateway.in.
		done := make(chan struct{})
		go func() {
			sa.Shutdown()
			close(done)
		}()
		select {
		case <-done:
			t.Logf("sa.Shutdown() completed")
		case <-time.After(2 * time.Second):
			t.Logf("sa.Shutdown() did not complete: the live outbound is not in s.gateway.out")
			live.setNoReconnect()
			live.closeConnection(ServerShutdown)
			<-done
		}
		t.Fatalf("out[%q] = %v after a never-registered outbound closed, want the live connection %v (outo still has %v conn(s))",
			"B", cur, live, outo)
	}

	require_Equal(t, sa.NumOutboundGateways(), 1)
}

// TestGatewayOutboundUnregisteredCloseNewRoutePeer checks the user visible
// side of the same desync: a server that joins the cluster later is told about
// the remote gateways through sendGatewayConfigsToRoute(), which reads
// gw.out. If the live outbound is missing from gw.out, the new peer never
// learns about the remote gateway and never creates its own outbound.
func TestGatewayOutboundUnregisteredCloseNewRoutePeer(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	ncb, err := nats.Connect(sb.ClientURL())
	require_NoError(t, err)
	defer ncb.Close()
	sub, err := ncb.SubscribeSync("foo")
	require_NoError(t, err)
	ncb.Flush()

	// A1 has the gateway connection to B and starts the cluster.
	oa1 := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa1 := runGatewayServer(oa1)
	defer sa1.Shutdown()
	waitForOutboundGateways(t, sa1, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// Lose the live outbound from gw.out, as a never registered duplicate does.
	desyncOutboundGateway(t, sa1, "B", sb)
	t.Logf("A1 outbound gateways after the never registered duplicate closed: %d", sa1.NumOutboundGateways())
	// A desynced server never closes its outbound in Shutdown(), so release
	// the orphan explicitly (this also keeps the deferred Shutdown bounded).
	defer releaseDesyncedOutbound(sa1)

	// A2 joins the cluster. It can only learn about gateway B from A1.
	oa2 := testDefaultOptionsForGateway("A")
	ru, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", sa1.ClusterAddr().Port))
	require_NoError(t, err)
	oa2.Routes = []*url.URL{ru}
	sa2 := runGatewayServer(oa2)
	defer sa2.Shutdown()
	checkClusterFormed(t, sa1, sa2)

	// Give A2 a chance to process the gateway configs it may have received.
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if sa2.NumOutboundGateways() > 0 {
			break
		}
	}
	nA2 := sa2.NumOutboundGateways()

	nc2, err := nats.Connect(sa2.ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	// A cluster peer that has no gateway connection to B has no interest from
	// B either: gateway interest is only carried by each server's own gateway
	// connections, it is not relayed over routes. So publishes from A2 to a
	// subject only B subscribes to are dropped, while the sibling A1 is
	// perfectly healthy.
	up := publishAndCount(t, nc2, sub, "foo", 10, time.Second)

	t.Logf("A2 outbound gateways=%d, delivered=%d/10", nA2, up)

	if nA2 != 1 {
		t.Errorf("A2 has %d outbound gateway(s) to B, want 1: it was not told about B by A1", nA2)
	}
	if up != 10 {
		t.Errorf("A2 delivered %d/10 messages to B", up)
	}
}

// releaseDesyncedOutbound closes any outbound gateway connection that is still
// in `outo` so that Server.Shutdown() can complete, and prevents it from
// reconnecting.
func releaseDesyncedOutbound(s *Server) {
	s.gateway.RLock()
	var c *client
	if len(s.gateway.outo) > 0 {
		c = s.gateway.outo[0]
	}
	s.gateway.RUnlock()
	if c != nil {
		c.setNoReconnect()
		c.closeConnection(ClientClosed)
	}
}

func publishAndCount(t *testing.T, nc *nats.Conn, sub *nats.Subscription, subj string, n int, timeout time.Duration) int {
	t.Helper()
	for i := 0; i < n; i++ {
		require_NoError(t, nc.Publish(subj, []byte("hello")))
	}
	nc.Flush()
	got := 0
	for got < n {
		if _, err := sub.NextMsg(timeout); err != nil {
			break
		}
		got++
	}
	return got
}
