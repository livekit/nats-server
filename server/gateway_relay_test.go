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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Discards writes while leaving the socket open, so the gateway looks connected.
type blackholeConn struct{ net.Conn }

func (blackholeConn) Write(p []byte) (int, error) { return len(p), nil }

func createRelayTestCluster(t *testing.T, name string, relay bool, gwTo []*Server) []*Server {
	t.Helper()
	var servers []*Server
	for i := 0; i < 3; i++ {
		o := testDefaultOptionsForGateway(name)
		o.ServerName = fmt.Sprintf("%s%d", name, i+1)
		o.Cluster.Host, o.Cluster.Port = "127.0.0.1", -1
		o.MaxPingsOut = 1000
		o.Gateway.Relay = relay
		o.Gateway.RelayPongWait = 200 * time.Millisecond
		if i > 0 {
			o.Routes = RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", servers[0].ClusterAddr().Port))
		}
		if len(gwTo) > 0 {
			u, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", gwTo[0].GatewayAddr().Port))
			o.Gateway.Gateways = []*RemoteGatewayOpts{{Name: gwTo[0].ClusterName(), URLs: []*url.URL{u}}}
		}
		servers = append(servers, runGatewayServer(o))
	}
	checkClusterFormed(t, servers...)
	return servers
}

type deliveryCounts struct {
	sync.Mutex
	m map[string]int
}

func (d *deliveryCounts) handler(tag string) nats.MsgHandler {
	return func(m *nats.Msg) {
		d.Lock()
		d.m[m.Subject+"/"+string(m.Data)]++
		d.m[tag+"/"+m.Subject]++
		d.Unlock()
	}
}

func (d *deliveryCounts) get(k string) int {
	d.Lock()
	defer d.Unlock()
	return d.m[k]
}

func TestGatewayRelayUnhealthyOutbound(t *testing.T) {
	defer func(d time.Duration) { gatewayMaxPingInterval = d }(gatewayMaxPingInterval)
	gatewayMaxPingInterval = 50 * time.Millisecond
	defer func(d time.Duration) { gatewayReconnectDelay = d }(gatewayReconnectDelay)
	gatewayReconnectDelay = 3 * time.Second

	for _, fault := range []string{"blackhole", "closed"} {
		for _, relay := range []bool{false, true} {
			t.Run(fault+"/relay="+strconv.FormatBool(relay), func(t *testing.T) {
				sb := createRelayTestCluster(t, "B", relay, nil)
				sa := createRelayTestCluster(t, "A", relay, sb)
				for _, s := range append(sa, sb...) {
					defer s.Shutdown()
					waitForOutboundGateways(t, s, 1, 2*time.Second)
				}

				dc := &deliveryCounts{m: map[string]int{}}
				ncA1, ncA2 := natsConnect(t, sa[0].ClientURL()), natsConnect(t, sa[1].ClientURL())
				ncB2, ncB3 := natsConnect(t, sb[1].ClientURL()), natsConnect(t, sb[2].ClientURL())
				for _, nc := range []*nats.Conn{ncA1, ncA2, ncB2, ncB3} {
					defer nc.Close()
				}
				natsSub(t, ncB2, "plain", dc.handler("B2"))
				natsQueueSub(t, ncB3, "qsvc", "svc", dc.handler("B3"))
				natsQueueSub(t, ncA2, "mix", "g", dc.handler("A2"))
				natsQueueSub(t, ncB2, "mix", "g", dc.handler("B2"))
				natsQueueSub(t, ncB3, "rpc", "svc", func(m *nats.Msg) { m.Respond(m.Data) })
				for _, nc := range []*nats.Conn{ncA2, ncB2, ncB3} {
					natsFlush(t, nc)
				}
				checkForRegisteredQSubInterest(t, sa[0], "B", globalAccountName, "qsvc", 1, 2*time.Second)
				checkForRegisteredQSubInterest(t, sa[0], "B", globalAccountName, "rpc", 1, 2*time.Second)
				checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
					natsPub(t, ncA1, "plain", []byte("probe"))
					natsFlush(t, ncA1)
					if dc.get("plain/probe") == 0 {
						return fmt.Errorf("plain interest not propagated")
					}
					return nil
				})

				gwc := sa[0].getOutboundGatewayConnection("B")
				if fault == "blackhole" {
					gwc.mu.Lock()
					gwc.nc = blackholeConn{gwc.nc}
					gwc.mu.Unlock()
					// Let an unanswered PING age past RelayPongWait.
					time.Sleep(400 * time.Millisecond)
				} else {
					gwc.closeConnection(ClientClosed)
					waitForOutboundGateways(t, sa[0], 0, time.Second)
				}

				const n = 100
				for i := 0; i < n; i++ {
					for _, subj := range []string{"plain", "qsvc", "mix"} {
						natsPub(t, ncA1, subj, []byte(strconv.Itoa(i)))
					}
				}
				natsFlush(t, ncA1)
				var replies int
				for i := 0; i < 10; i++ {
					if _, err := ncA1.Request("rpc", []byte("x"), 100*time.Millisecond); err == nil {
						replies++
					}
				}
				time.Sleep(300 * time.Millisecond)
				if fault == "closed" && sa[0].getOutboundGatewayConnection("B") != nil {
					t.Fatalf("outbound reconnected during the test; the fault window was not held")
				}

				if got := dc.get("A2/mix"); got != n {
					t.Fatalf("mix: local queue member got %d, want %d", got, n)
				}
				if got := dc.get("B2/mix"); got != 0 {
					t.Fatalf("mix: remote queue member got %d, want 0", got)
				}
				want, wantReplies := 0, 0
				if relay {
					want, wantReplies = n, 10
				}
				if got := dc.get("B2/plain") - dc.get("plain/probe"); got != want {
					t.Fatalf("plain: got %d, want %d", got, want)
				}
				if got := dc.get("B3/qsvc"); got != want {
					t.Fatalf("qsvc: got %d, want %d", got, want)
				}
				if replies != wantReplies {
					t.Fatalf("rpc: got %d replies, want %d", replies, wantReplies)
				}
			})
		}
	}
}
