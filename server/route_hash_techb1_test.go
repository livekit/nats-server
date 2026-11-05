// tech-b1, ticket 0004: routesIDByHash key asymmetry between addRoute and
// removeRoute (gateway-only bookkeeping map).

package server

import (
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func techB1GwOpts(name string, pool int, routes []*url.URL) *Options {
	o := DefaultOptions()
	o.ServerName = name
	o.Host = "127.0.0.1"
	o.Port = -1
	o.NoSystemAccount = true
	o.Gateway.Name = "TECHB1GW"
	o.Gateway.Port = -1
	o.Cluster.Name = "TECHB1GW"
	o.Cluster.Host = "127.0.0.1"
	o.Cluster.Port = -1
	o.Cluster.PoolSize = pool
	o.Routes = routes
	return o
}

func techB1RouteURL(t *testing.T, s *Server) *url.URL {
	t.Helper()
	u, err := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", s.ClusterAddr().Port))
	if err != nil {
		t.Fatalf("bad route url: %v", err)
	}
	return u
}

func techB1HashKeys(t *testing.T, s *Server) map[string]*client {
	t.Helper()
	m := make(map[string]*client)
	s.gateway.routesIDByHash.Range(func(k, v any) bool {
		m[k.(string)] = v.(*client)
		return true
	})
	return m
}

// techB1HashKeysLocked returns the map's keys with the server lock held, to
// also capture the server's own pool size and the remote's route fields.
func techB1RouteHashState(t *testing.T, s *Server, remoteID string) (keys []string, idHash string, poolIdx int, noPool bool, poolSize int) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	poolSize = s.routesPoolSize
	s.gateway.routesIDByHash.Range(func(k, v any) bool {
		keys = append(keys, k.(string))
		return true
	})
	for rid, conns := range s.routes {
		if rid != remoteID {
			continue
		}
		for _, r := range conns {
			if r == nil {
				continue
			}
			r.mu.Lock()
			idHash, poolIdx, noPool = r.route.idHash, r.route.poolIdx, r.route.noPool
			r.mu.Unlock()
			break
		}
	}
	return keys, idHash, poolIdx, noPool, poolSize
}

func techB1WaitSomeRoutes(t *testing.T, servers ...*Server) {
	t.Helper()
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		for _, s := range servers {
			if n := s.NumRoutes(); n == 0 {
				return fmt.Errorf("%s: no routes yet", s.Name())
			}
		}
		return nil
	})
	// Let the pool finish forming before sampling the map.
	time.Sleep(300 * time.Millisecond)
}

// techB1DumpRoutes logs each connection of the remote's slice, so the exact
// pool shape is in the test output.
func techB1DumpRoutes(t *testing.T, label string, s *Server, remoteID string) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	t.Logf("%s: %s routesPoolSize=%d remoteRoutePoolSize=%d", label, s.Name(), s.routesPoolSize, s.remoteRoutePoolSize[remoteID])
	for i, r := range s.routes[remoteID] {
		if r == nil {
			t.Logf("%s: %s conn[%d]=nil", label, s.Name(), i)
			continue
		}
		r.mu.Lock()
		t.Logf("%s: %s conn[%d] cid=%d poolIdx=%d noPool=%v didSolicit=%v closed=%v",
			label, s.Name(), i, r.cid, r.route.poolIdx, r.route.noPool, r.route.didSolicit, r.isClosed())
		r.mu.Unlock()
	}
}

func techB1WaitNoRoutes(t *testing.T, servers ...*Server) {
	t.Helper()
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		for _, s := range servers {
			if n := s.NumRoutes(); n != 0 {
				return fmt.Errorf("%s: still %d routes", s.Name(), n)
			}
		}
		return nil
	})
}

func techB1ReportHash(t *testing.T, label string, s *Server, keys []string) {
	t.Helper()
	t.Logf("%s: routesPoolSize=%d, %d routesIDByHash entries %v", label, s.routesPoolSize, len(keys), keys)
	for k, c := range techB1HashKeys(t, s) {
		c.mu.Lock()
		t.Logf("%s: key %q -> cid=%d closed=%v kind=%v", label, k, c.cid, c.isClosed(), c.kind)
		c.mu.Unlock()
	}
}

// TestTechB1RouteHashLocalPooledRemoteUnpooled is case 1: local pool 3, remote
// advertises RoutePoolSize 0 (pre-2.11 peer or pool_size -1). addRoute stores the
// bare idHash, removeRoute deletes idHash+"0".
func TestTechB1RouteHashLocalPooledRemoteUnpooled(t *testing.T) {
	s1 := RunServer(techB1GwOpts("H1", 3, nil))
	defer s1.Shutdown()
	s2 := RunServer(techB1GwOpts("H2", -1, []*url.URL{techB1RouteURL(t, s1)}))
	defer s2.Shutdown()
	techB1WaitSomeRoutes(t, s1, s2)
	techB1DumpRoutes(t, "case1", s1, s2.ID())

	keys, idHash, poolIdx, noPool, poolSize := techB1RouteHashState(t, s1, s2.ID())
	stored, removed := idHash, idHash
	if !noPool {
		stored += strconv.Itoa(poolIdx)
	}
	if poolSize > 1 {
		removed += strconv.Itoa(poolIdx)
	}
	t.Logf("case1: s2 advertises RoutePoolSize=%d, s1 routesPoolSize=%d, remote noPool=%v poolIdx=%d",
		s2.getOpts().Cluster.PoolSize, poolSize, noPool, poolIdx)
	t.Logf("case1: addRoute stores %q, removeRoute deletes %q (main predicate) -> differ=%v",
		stored, removed, stored != removed)
	require_True(t, noPool)
	require_True(t, stored != removed)
	require_Equal(t, len(keys), 1)
	require_True(t, keys[0] == stored)
	techB1ReportHash(t, "case1 before close", s1, keys)

	s2.Shutdown()
	techB1WaitNoRoutes(t, s1)

	after := techB1HashKeys(t, s1)
	keyList := make([]string, 0, len(after))
	for k := range after {
		keyList = append(keyList, k)
	}
	techB1ReportHash(t, "case1 after close", s1, keyList)
	// Expected after a correct close: the map is empty.
	require_Equal(t, len(after), 0)
}

// TestTechB1RouteHashLocalUnpooledRemotePooled is case 2: local pool_size 1
// (routesPoolSize 1), remote pool 3. addRoute stores idHash+N per slot,
// removeRoute deletes the bare idHash.
func TestTechB1RouteHashLocalUnpooledRemotePooled(t *testing.T) {
	s1 := RunServer(techB1GwOpts("H1", 1, nil))
	defer s1.Shutdown()
	s2 := RunServer(techB1GwOpts("H2", 3, []*url.URL{techB1RouteURL(t, s1)}))
	defer s2.Shutdown()
	techB1WaitSomeRoutes(t, s1, s2)
	techB1DumpRoutes(t, "case2", s1, s2.ID())

	s1.mu.RLock()
	n := len(s1.routes[s2.ID()])
	poolSize := s1.routesPoolSize
	s1.mu.RUnlock()
	keys, idHash, poolIdx, noPool, _ := techB1RouteHashState(t, s1, s2.ID())
	stored, removed := idHash, idHash
	if !noPool {
		stored += strconv.Itoa(poolIdx)
	}
	if poolSize > 1 {
		removed += strconv.Itoa(poolIdx)
	}
	t.Logf("case2: s1 routesPoolSize=%d, remote slice len=%d, remote noPool=%v first poolIdx=%d",
		poolSize, n, noPool, poolIdx)
	t.Logf("case2: addRoute stores %q, removeRoute deletes %q (main predicate) -> differ=%v",
		stored, removed, stored != removed)
	techB1ReportHash(t, "case2 before close", s1, keys)
	// getRouteByHash uses the bare fallback on a routesPoolSize==1 server and
	// only accepts the fallback route if it is a "no pool" route.
	if r, perAcc := s1.getRouteByHash([]byte(idHash), []byte("$G")); r != nil {
		t.Logf("case2: getRouteByHash(%q, \"$G\") before close -> cid=%d (%v)", idHash, r.cid, perAcc)
	} else {
		t.Logf("case2: getRouteByHash(%q, \"$G\") before close -> nil (bare and idHash+0 fallback)", idHash)
	}
	require_Equal(t, len(keys), 3)
	require_True(t, stored != removed)

	s2.Shutdown()
	techB1WaitNoRoutes(t, s1)

	after := techB1HashKeys(t, s1)
	keyList := make([]string, 0, len(after))
	for k := range after {
		keyList = append(keyList, k)
	}
	techB1ReportHash(t, "case2 after close", s1, keyList)
	require_Equal(t, len(after), 0)
}

// TestTechB1RouteHashStaleGetAndReply is case 3: with the stale bare key still
// in the map, probe every getRouteByHash key path and record what a gateway
// reply would do with the closed client. The test itself is tree-agnostic
// (evidence-only): the failing oracle is the map emptiness in cases 1/2.
func TestTechB1RouteHashStaleGetAndReply(t *testing.T) {
	s1 := RunServer(techB1GwOpts("H1", 3, nil))
	defer s1.Shutdown()
	s2 := RunServer(techB1GwOpts("H2", -1, []*url.URL{techB1RouteURL(t, s1)}))
	defer s2.Shutdown()
	techB1WaitSomeRoutes(t, s1, s2)

	keys, idHash, _, _, _ := techB1RouteHashState(t, s1, s2.ID())
	require_Equal(t, len(keys), 1)

	s2.Shutdown()
	techB1WaitNoRoutes(t, s1)

	after := techB1HashKeys(t, s1)
	stale := after[idHash]
	t.Logf("case3: stale entry for bare idHash %q present=%v", idHash, stale != nil)
	if stale != nil {
		stale.mu.Lock()
		t.Logf("case3: stale client cid=%d kind=%v closed=%v", stale.cid, stale.kind, stale.isClosed())
		stale.mu.Unlock()
	}

	// Key paths of getRouteByHash: account with an accRouteByHash pool index,
	// account with no entry (bare lookup), and a name that is not an account.
	for _, accName := range []string{"$G", "", "NO_SUCH_ACCOUNT"} {
		route, perAcc := s1.getRouteByHash([]byte(idHash), []byte(accName))
		var closed bool
		if route != nil {
			route.mu.Lock()
			closed = route.isClosed()
			route.mu.Unlock()
		}
		t.Logf("case3: getRouteByHash(%q, acc=%q) -> route=%v (closed=%v) perAccount=%v stale=%v",
			idHash, accName, route != nil, closed, perAcc, route != nil && route == stale)
	}

	// A gateway reply to the stale client is accepted and silently dropped:
	// enqueueProto on a closed client is a no-op (no panic, nothing written).
	if stale != nil {
		stale.mu.Lock()
		pb0 := stale.out.pb
		stale.enqueueProto([]byte("RMSG x 1\r\n\r\n"))
		pb1 := stale.out.pb
		stale.mu.Unlock()
		t.Logf("case3: enqueueProto on the stale closed route: pending bytes %d -> %d (dropped)", pb0, pb1)
	}
}

// TestTechB1RouteHashReloadVariant is path 4: remove the route through a config
// reload (S2 drops its routes) instead of a shutdown, to show the bug is in the
// remove predicate and not shutdown ordering. Same pool shape as case 2
// (S1 pool 1, S2 pool 3) so S1 does not try to reconnect (no-pool remotes are
// reconnected by pooled servers in removeRoute).
func TestTechB1RouteHashReloadVariant(t *testing.T) {
	conf1 := createConfFile(t, []byte(`
		port: -1
		server_name: "R1"
		no_sys_acc: true
		gateway { name: "TECHB1GW", listen: "127.0.0.1:-1" }
		cluster { name: "TECHB1GW", listen: "127.0.0.1:-1", pool_size: 1 }
	`))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	routeTo1 := fmt.Sprintf(`routes: ["nats://127.0.0.1:%d"]`, o1.Cluster.Port)
	conf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "R2"
		no_sys_acc: true
		gateway { name: "TECHB1GW", listen: "127.0.0.1:-1" }
		cluster { name: "TECHB1GW", listen: "127.0.0.1:-1", pool_size: 3, %s }
	`, routeTo1)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()
	techB1WaitSomeRoutes(t, s1, s2)
	techB1DumpRoutes(t, "reload", s1, s2.ID())

	keys, idHash, _, _, _ := techB1RouteHashState(t, s1, s2.ID())
	require_Equal(t, len(keys), 3)

	// Reload S2 without the route; S2 closes it, S1 removes it.
	reloadUpdateConfig(t, s2, conf2, `
		port: -1
		server_name: "R2"
		no_sys_acc: true
		gateway { name: "TECHB1GW", listen: "127.0.0.1:-1" }
		cluster { name: "TECHB1GW", listen: "127.0.0.1:-1", pool_size: 3 }
	`)
	techB1WaitNoRoutes(t, s1)

	after := techB1HashKeys(t, s1)
	keyList := make([]string, 0, len(after))
	for k := range after {
		keyList = append(keyList, k)
	}
	t.Logf("reload variant: idHash=%q, after route removal entries=%v", idHash, keyList)
	for k, c := range after {
		c.mu.Lock()
		t.Logf("reload variant: key %q cid=%d closed=%v", k, c.cid, c.isClosed())
		c.mu.Unlock()
	}
	require_Equal(t, len(after), 0)
}

// Controls: equal pool sizes on both sides (pooled/pooled and no-pool/no-pool)
// and a single gateway server with no routes.

func TestTechB1RouteHashControlPooledPooled(t *testing.T) {
	s1 := RunServer(techB1GwOpts("C1", 3, nil))
	defer s1.Shutdown()
	s2 := RunServer(techB1GwOpts("C2", 3, []*url.URL{techB1RouteURL(t, s1)}))
	defer s2.Shutdown()
	techB1WaitSomeRoutes(t, s1, s2)
	keys, _, _, _, _ := techB1RouteHashState(t, s1, s2.ID())
	require_Equal(t, len(keys), 3)
	techB1ReportHash(t, "control 3/3 before close", s1, keys)
	s2.Shutdown()
	techB1WaitNoRoutes(t, s1)
	require_Equal(t, len(techB1HashKeys(t, s1)), 0)
}

func TestTechB1RouteHashControlNoPoolNoPool(t *testing.T) {
	s1 := RunServer(techB1GwOpts("C1", -1, nil))
	defer s1.Shutdown()
	s2 := RunServer(techB1GwOpts("C2", -1, []*url.URL{techB1RouteURL(t, s1)}))
	defer s2.Shutdown()
	techB1WaitSomeRoutes(t, s1, s2)
	keys, _, _, _, _ := techB1RouteHashState(t, s1, s2.ID())
	require_Equal(t, len(keys), 1)
	s2.Shutdown()
	techB1WaitNoRoutes(t, s1)
	require_Equal(t, len(techB1HashKeys(t, s1)), 0)
}

func TestTechB1RouteHashControlNoRoute(t *testing.T) {
	s1 := RunServer(techB1GwOpts("C1", 3, nil))
	defer s1.Shutdown()
	require_True(t, s1.gateway.enabled)
	require_Equal(t, len(techB1HashKeys(t, s1)), 0)
}
