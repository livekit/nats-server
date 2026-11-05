package server

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

// Probe for the R7 store/remove key asymmetry: local pooling enabled (pool 3)
// and a remote that advertises no pooling (pool 0). addRoute stores the route
// under the bare idHash (pool == info.RoutePoolSize > 0 == false), removeRoute
// deletes idHash+"0" (s.routesPoolSize > 1), so the entry survives the close.
// Both servers need gateways enabled for storeRouteByHash to do anything.
func TestRouteHashPredicateAsymmetry(t *testing.T) {
	o1 := DefaultOptions()
	o1.ServerName = "S1"
	o1.Cluster.Port = -1
	o1.Cluster.Name = "G"
	o1.Cluster.PoolSize = 3
	o1.Gateway.Name = "G"
	o1.Gateway.Port = -1
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.ServerName = "S2"
	o2.Cluster.Port = -1
	o2.Cluster.Name = "G"
	o2.Cluster.PoolSize = -1
	o2.Gateway.Name = "G"
	o2.Gateway.Port = -1
	o2.Routes = []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", s1.getOpts().Cluster.Port)}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		s1.mu.RLock()
		n := len(s1.routes)
		s1.mu.RUnlock()
		if n != 1 {
			return fmt.Errorf("expected 1 remote on S1, got %d", n)
		}
		return nil
	})

	var idHash string
	var noPool bool
	s1.mu.RLock()
	for _, conns := range s1.routes {
		for _, r := range conns {
			if r != nil {
				r.mu.Lock()
				idHash, noPool = r.route.idHash, r.route.noPool
				r.mu.Unlock()
			}
		}
	}
	s1.mu.RUnlock()
	require_True(t, idHash != _EMPTY_)
	require_True(t, noPool)

	keyCount := func() int {
		n := 0
		s1.gateway.routesIDByHash.Range(func(_, _ any) bool { n++; return true })
		return n
	}
	t.Logf("before close: idHash=%q map entries=%d", idHash, keyCount())

	s2.Shutdown()
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := s1.NumRoutes(); n != 0 {
			return fmt.Errorf("expected 0 routes on S1, got %d", n)
		}
		return nil
	})

	var keys []string
	s1.gateway.routesIDByHash.Range(func(k, _ any) bool { keys = append(keys, k.(string)); return true })
	t.Logf("after close: map entries=%d keys=%q (bare idHash=%q)", len(keys), keys, idHash)
	if _, ok := s1.gateway.routesIDByHash.Load(idHash); ok {
		t.Fatalf("stale routesIDByHash entry for closed route: key=%q (map=%q)", idHash, keys)
	}
}

// Control: both servers pool 3, so store and remove use the same pooled key
// and the map is empty after the route closes.
func TestRouteHashPredicateSymmetricControl(t *testing.T) {
	o1 := DefaultOptions()
	o1.ServerName = "S1"
	o1.Cluster.Port = -1
	o1.Cluster.Name = "G"
	o1.Cluster.PoolSize = 3
	o1.Gateway.Name = "G"
	o1.Gateway.Port = -1
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.ServerName = "S2"
	o2.Cluster.Port = -1
	o2.Cluster.Name = "G"
	o2.Cluster.PoolSize = 3
	o2.Gateway.Name = "G"
	o2.Gateway.Port = -1
	o2.Routes = []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", s1.getOpts().Cluster.Port)}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		s1.mu.RLock()
		n := len(s1.routes)
		s1.mu.RUnlock()
		if n != 1 {
			return fmt.Errorf("expected 1 remote on S1, got %d", n)
		}
		return nil
	})

	s2.Shutdown()
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := s1.NumRoutes(); n != 0 {
			return fmt.Errorf("expected 0 routes on S1, got %d", n)
		}
		return nil
	})

	n := 0
	s1.gateway.routesIDByHash.Range(func(_, _ any) bool { n++; return true })
	require_Equal(t, n, 0)
}

// Reverse direction: local pool_size: 1 (so s.routesPoolSize == 1 and addRoute
// does not force no-pool), peer pool 3. addRoute stored idHash+idx while
// removeRoute deletes the bare idHash, so the slot keys stay behind. On main
// the map is not empty after the route is gone, which fails the assertion.
func TestRouteHashPredicateAsymmetryLocalPoolOne(t *testing.T) {
	o1 := DefaultOptions()
	o1.ServerName = "S1"
	o1.Cluster.Port = -1
	o1.Cluster.Name = "G"
	o1.Cluster.PoolSize = 3
	o1.Gateway.Name = "G"
	o1.Gateway.Port = -1
	s1 := RunServer(o1)
	defer s1.Shutdown()

	o2 := DefaultOptions()
	o2.ServerName = "S2"
	o2.Cluster.Port = -1
	o2.Cluster.Name = "G"
	o2.Cluster.PoolSize = 1
	o2.Gateway.Name = "G"
	o2.Gateway.Port = -1
	o2.Routes = []*url.URL{{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", s1.getOpts().Cluster.Port)}}
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		s2.mu.RLock()
		n := len(s2.routes)
		s2.mu.RUnlock()
		if n != 1 {
			return fmt.Errorf("expected remotes on S2, got %d", n)
		}
		return nil
	})

	var idHash string
	s2.mu.RLock()
	for _, conns := range s2.routes {
		for _, r := range conns {
			if r != nil {
				r.mu.Lock()
				idHash = r.route.idHash
				r.mu.Unlock()
			}
		}
	}
	s2.mu.RUnlock()
	require_True(t, idHash != _EMPTY_)

	s1.Shutdown()
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := s2.NumRoutes(); n != 0 {
			return fmt.Errorf("expected 0 routes on S2, got %d", n)
		}
		return nil
	})

	var keys []string
	s2.gateway.routesIDByHash.Range(func(k, _ any) bool { keys = append(keys, k.(string)); return true })
	t.Logf("after close on pool-size-1 server: map entries=%d keys=%q (bare idHash=%q)", len(keys), keys, idHash)
	require_Equal(t, len(keys), 0)
}
