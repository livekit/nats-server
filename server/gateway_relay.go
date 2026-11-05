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
	"bytes"
	"cmp"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Reports whether this outbound gateway should be bypassed in favor of a
// route peer: it is closed, has an unanswered PING older than pongWait, or
// its pending bytes have reached the fast-producer stall threshold.
func (c *client) gatewayRelayUnhealthy(pongWait time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed() {
		return true
	}
	if c.ping.out > 0 && time.Since(c.ping.first) > pongWait {
		return true
	}
	return c.out.mp > 0 && c.out.pb > c.out.mp/4*3
}

// Hands the message to one route peer, which sends it to the given clusters
// over its own outbound gateways. Every peer holds the same view of remote
// interest, so the subject hash only spreads load.
func (c *client) relayMsgToRoutePeer(acc *Account, msg, subject, reply []byte, qgroups [][]byte, clusters []string) bool {
	srv := c.srv
	var _routes [16]*client
	routes := _routes[:0]
	srv.mu.RLock()
	if idx := acc.routePoolIdx; idx < 0 {
		for _, r := range srv.accRoutes[acc.Name] {
			routes = append(routes, r)
		}
	} else {
		srv.forEachRouteIdx(idx, func(r *client) bool {
			routes = append(routes, r)
			return true
		})
	}
	srv.mu.RUnlock()
	if len(routes) == 0 {
		return false
	}
	slices.SortFunc(routes, func(a, b *client) int { return cmp.Compare(a.cid, b.cid) })
	h := fnv.New32a()
	h.Write(subject)
	rt := routes[h.Sum32()%uint32(len(routes))]
	if !rt.headers {
		return false
	}

	pa := c.pa
	keep := "0"
	if c.pa.hdr > 0 {
		keep = "1"
	}
	dmsg := c.setHeader(gwRelayHdr, keep+" "+strings.Join(clusters, ","), msg)
	if len(qgroups) > 0 {
		dmsg = c.setHeader(gwRelayQHdr, string(bytes.Join(qgroups, []byte{' '})), dmsg)
	}
	sub := &subscription{client: rt, subject: subject}
	mh := c.msgHeaderForRouteOrLeaf(subject, reply, &routeTarget{sub: sub}, acc, true)
	delivered := c.deliverMsg(c.isMqtt(), sub, acc, subject, reply, mh, dmsg, false)
	c.pa = pa
	return delivered
}

// Handles an RMSG carrying the relay header: strips the relay headers and
// sends the message only to the named clusters. It is never delivered locally
// (the origin server already did that) and never relayed again.
func (c *client) processGatewayRelay(msg []byte) bool {
	hdr := msg[:c.pa.hdr]
	v := sliceHeader(gwRelayHdr, hdr)
	if len(v) < 3 {
		return false
	}
	clusters := strings.Split(string(v[2:]), ",")
	var qgroups [][]byte
	for _, q := range bytes.Fields(sliceHeader(gwRelayQHdr, hdr)) {
		qgroups = append(qgroups, copyBytes(q))
	}

	var dmsg []byte
	if v[0] == '1' {
		h := removeHeaderIfPresent(copyBytes(hdr[:len(hdr)-LEN_CR_LF]), gwRelayHdr)
		h = removeHeaderIfPresent(h, gwRelayQHdr)
		if len(h) == 0 {
			h = []byte(hdrLine)
		}
		h = append(h, CR_LF...)
		dmsg = append(h, msg[c.pa.hdr:]...)
		c.pa.hdr = len(h)
		c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
	} else {
		dmsg = msg[c.pa.hdr:]
		c.pa.hdr = -1
		c.pa.hdb = nil
	}
	c.pa.size = len(dmsg) - LEN_CR_LF
	c.pa.szb = []byte(strconv.Itoa(c.pa.size))

	acc, _ := c.getAccAndResultFromCache()
	if acc == nil {
		return true
	}
	c.sendMsgToGatewaysEx(acc, dmsg, c.pa.subject, c.pa.reply, qgroups, false, clusters)
	return true
}
