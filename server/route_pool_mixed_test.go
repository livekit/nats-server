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

// Test routes that mix cluster pool sizes (supported for rolling upgrades)
// and perform a reload that adds a pinned (per-account) route. The account's
// pool index computed from the reloading remote's pool size used to be
// applied to *every* remote's connection slice, panicking the receiving
// server's route read loop when another remote had a smaller pool.

const routePoolMixedTmpl = `
	port: -1
	server_name: "%s"
	accounts {
		A { users: [{user: "A", password: "pwd"}] }
		B { users: [{user: "B", password: "pwd"}] }
	}
	cluster {
		port: -1
		name: "local"
		%s
		pool_size: %d
		%s
	}
	no_sys_acc: true
`

const routePoolMixedPinnedTmpl = `
	port: -1
	server_name: "%s"
	accounts {
		A { users: [{user: "A", password: "pwd"}] }
		B { users: [{user: "B", password: "pwd"}] }
	}
	cluster {
		port: -1
		name: "local"
		%s
		pool_size: %d
		accounts: [%q]
	}
	no_sys_acc: true
`

func routePoolMixedGen(name, routes string, pool int, accs string) string {
	return fmt.Sprintf(routePoolMixedTmpl, name, routes, pool, accs)
}

func routePoolMixedRouteTo(port int) string {
	return fmt.Sprintf(`routes: ["nats://127.0.0.1:%d"]`, port)
}

// routePoolMixedStart starts a 3-server cluster with the given pool sizes:
// S1 is the seed, S2 and S3 route to S1.
func routePoolMixedStart(t *testing.T, p1, p2, p3 int) (s1, s2, s3 *Server, conf2, conf3 string) {
	t.Helper()
	conf1 := createConfFile(t, []byte(routePoolMixedGen("S1", _EMPTY_, p1, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	t.Cleanup(s1.Shutdown)

	conf2 = createConfFile(t, []byte(routePoolMixedGen("S2", routePoolMixedRouteTo(o1.Cluster.Port), p2, _EMPTY_)))
	s2, _ = RunServerWithConfig(conf2)
	t.Cleanup(s2.Shutdown)

	conf3 = createConfFile(t, []byte(routePoolMixedGen("S3", routePoolMixedRouteTo(o1.Cluster.Port), p3, _EMPTY_)))
	s3, _ = RunServerWithConfig(conf3)
	t.Cleanup(s3.Shutdown)

	return s1, s2, s3, conf2, conf3
}

// checkRoutePoolMixedSlices waits for the seed to have two remotes whose
// pool slices have the expected lengths, i.e. one remote per configured pool
// size.
func checkRoutePoolMixedSlices(t *testing.T, s *Server, lens ...int) {
	t.Helper()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if len(s.routes) != len(lens) {
			return fmt.Errorf("expected %d remotes, got %d", len(lens), len(s.routes))
		}
		want := make(map[int]int, len(lens))
		for _, l := range lens {
			want[l]++
		}
		for _, conns := range s.routes {
			want[len(conns)]--
		}
		for l, n := range want {
			if n != 0 {
				return fmt.Errorf("expected one remote with slice length %d (mismatch %d)", l, n)
			}
		}
		return nil
	})
}

func TestRoutePoolIdxAcrossRemotesOnPinnedAccountReload(t *testing.T) {
	s1, s2, _, conf2, _ := routePoolMixedStart(t, 2, 5, 2)
	checkRoutePoolMixedSlices(t, s1, 2, 5)

	// computeRoutePoolIdx(5, "A") == 2, and S1's slice for S3 only has 2
	// entries, so the old code indexed past the end of S3's slice.
	reloadUpdateConfig(t, s2, conf2, routePoolMixedGen("S2", routePoolMixedRouteTo(s1.opts.Cluster.Port), 5, `accounts: ["A"]`))

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		s2.mu.RLock()
		r := s2.accRoutes["A"][s1.ID()]
		s2.mu.RUnlock()
		if r == nil {
			return fmt.Errorf("dedicated route for A not established")
		}
		return nil
	})
	require_True(t, s1.NumRoutes() > 0)
}

func TestRoutePoolIdxAcrossRemotesPinnedAccountB(t *testing.T) {
	s1, s2, _, conf2, _ := routePoolMixedStart(t, 2, 5, 2)
	checkRoutePoolMixedSlices(t, s1, 2, 5)
	require_Equal(t, computeRoutePoolIdx(5, "B"), 4)

	reloadUpdateConfig(t, s2, conf2, routePoolMixedGen("S2", routePoolMixedRouteTo(s1.opts.Cluster.Port), 5, `accounts: ["B"]`))

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		s2.mu.RLock()
		r := s2.accRoutes["B"][s1.ID()]
		s2.mu.RUnlock()
		if r == nil {
			return fmt.Errorf("dedicated route for B not established")
		}
		return nil
	})
	require_True(t, s1.NumRoutes() > 0)
}

// TestRoutePoolIdxFarEndReload has the leaf/far server reload, so the
// per-account INFO travels through the middle server, which is the one with
// a smaller pool slice for its other remote.
func TestRoutePoolIdxFarEndReload(t *testing.T) {
	conf1 := createConfFile(t, []byte(routePoolMixedGen("S1", _EMPTY_, 2, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	t.Cleanup(s1.Shutdown)

	conf2 := createConfFile(t, []byte(routePoolMixedGen("S2", routePoolMixedRouteTo(o1.Cluster.Port), 2, _EMPTY_)))
	s2, o2 := RunServerWithConfig(conf2)
	t.Cleanup(s2.Shutdown)

	conf3 := createConfFile(t, []byte(routePoolMixedGen("S3", routePoolMixedRouteTo(o2.Cluster.Port), 5, _EMPTY_)))
	s3, _ := RunServerWithConfig(conf3)
	t.Cleanup(s3.Shutdown)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		s2.mu.RLock()
		defer s2.mu.RUnlock()
		if len(s2.routes) != 2 {
			return fmt.Errorf("expected 2 remotes on S2, got %d", len(s2.routes))
		}
		return nil
	})
	require_Equal(t, computeRoutePoolIdx(5, "A"), 2)

	reloadUpdateConfig(t, s3, conf3, routePoolMixedGen("S3", routePoolMixedRouteTo(o2.Cluster.Port), 5, `accounts: ["A"]`))

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		s3.mu.RLock()
		r := s3.accRoutes["A"][s2.ID()]
		s3.mu.RUnlock()
		if r == nil {
			return fmt.Errorf("dedicated route for A not established")
		}
		return nil
	})
	require_True(t, s1.NumRoutes() > 0)
	require_True(t, s2.NumRoutes() > 0)
}

// TestRoutePoolIdxUniformPoolControl is the same pinned-account reload with
// every server at pool size 5, so no index can be out of range.
func TestRoutePoolIdxUniformPoolControl(t *testing.T) {
	s1, s2, s3, conf2, _ := routePoolMixedStart(t, 5, 5, 5)
	checkRoutePoolMixedSlices(t, s1, 5, 5)

	reloadUpdateConfig(t, s2, conf2, routePoolMixedGen("S2", routePoolMixedRouteTo(s1.opts.Cluster.Port), 5, `accounts: ["A"]`))

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		s2.mu.RLock()
		r := s2.accRoutes["A"][s1.ID()]
		s2.mu.RUnlock()
		if r == nil {
			return fmt.Errorf("dedicated route for A not established")
		}
		return nil
	})
	s1.mu.RLock()
	for _, conns := range s1.routes {
		require_Equal(t, len(conns), 5)
	}
	s1.mu.RUnlock()
	require_True(t, s1.NumRoutes() > 0)
	require_True(t, s3.NumRoutes() > 0)
}
