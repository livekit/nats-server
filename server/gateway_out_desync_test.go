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
)

// TestGatewayOutboundUnregisteredCloseKeepsLiveEntry creates a second outbound
// gateway connection for a remote gateway name that is already registered. The
// duplicate is rejected by registerOutboundGatewayConnection() (the
// setNoReconnect()+closeConnection() branch in processGatewayInfo) and its close
// runs removeRemoteGatewayConnection(), which does `delete(gw.out, gwName)`
// without checking that the client being removed is the one registered under
// that name.
//
// The live connection stays in `outo` so it keeps carrying messages, but the
// `out` map loses it. `out` is what monitoring, shutdown, reload, raft
// readiness and the gossip of gateway configs to newly joined route peers read.
func TestGatewayOutboundUnregisteredCloseKeepsLiveEntry(t *testing.T) {
	ob := testDefaultOptionsForGateway("B")
	sb := runGatewayServer(ob)
	defer sb.Shutdown()

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	sa := runGatewayServer(oa)

	waitForOutboundGateways(t, sa, 1, 2*time.Second)
	waitForInboundGateways(t, sb, 1, 2*time.Second)

	// Capture the live outbound connection: both maps must agree on it.
	sa.gateway.RLock()
	live := sa.gateway.out["B"]
	outoLen := len(sa.gateway.outo)
	sa.gateway.RUnlock()
	require_True(t, live != nil)
	require_Equal(t, outoLen, 1)

	// Create a second outbound connection for the same remote gateway name.
	// It will go through the INFO exchange, find the name taken and close.
	cfg := sa.getRemoteGateway("B")
	require_True(t, cfg != nil)
	u, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", sb.GatewayAddr().Port))
	require_NoError(t, err)
	conn, err := net.Dial("tcp", sb.GatewayAddr().String())
	require_NoError(t, err)
	sa.createGateway(cfg, u, conn)

	// The duplicate completes its handshake asynchronously. When it closes
	// without having registered, `out` can lose the live connection, and that
	// desync is permanent (the loser has noReconnect set, and the live
	// connection will not re-register itself), so polling cannot miss it.
	var violation error
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		sa.gateway.RLock()
		cur, outo := sa.gateway.out["B"], len(sa.gateway.outo)
		sa.gateway.RUnlock()
		if cur != live {
			violation = fmt.Errorf("out[%q] = %v after a never-registered outbound closed, want the live connection %v (outo still has %v conn(s))",
				"B", cur, live, outo)
			break
		}
	}
	if violation != nil {
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
		t.Fatal(violation)
	}

	require_Equal(t, sa.NumOutboundGateways(), 1)
	sa.Shutdown()
}
