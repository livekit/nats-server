// pass 2, route.go: routesIDByHash key namespace collision between a pooled
// slot key (idHash + strconv.Itoa(idx)) and a dedicated per-account route key
// (idHash + accountName) when an account is named like a pool index.

package server

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func pass2PooledAccAtIdx0(pool int) string {
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("acc%d", i)
		if computeRoutePoolIdx(pool, name) == 0 {
			return name
		}
	}
	return ""
}

func pass2CollisionOpts(name string, pool int, routes []*url.URL) *Options {
	o := DefaultOptions()
	o.ServerName = name
	o.Host = "127.0.0.1"
	o.Port = -1
	o.NoSystemAccount = true
	o.Gateway.Name = "PASS2GW"
	o.Gateway.Port = -1
	o.Cluster.Name = "PASS2GW"
	o.Cluster.Host = "127.0.0.1"
	o.Cluster.Port = -1
	o.Cluster.PoolSize = pool
	o.Cluster.PinnedAccounts = []string{"0"}
	o.Accounts = []*Account{NewAccount("0"), NewAccount(pass2PooledAccAtIdx0(pool))}
	o.Routes = routes
	return o
}

func pass2RouteURL(t *testing.T, s *Server) *url.URL {
	t.Helper()
	u, err := url.Parse(fmt.Sprintf("nats-route://127.0.0.1:%d", s.ClusterAddr().Port))
	if err != nil {
		t.Fatalf("bad route url: %v", err)
	}
	return u
}

func TestPass2RouteHashKeyCollisionNumericAccount(t *testing.T) {
	s1 := RunServer(pass2CollisionOpts("S1", 3, nil))
	defer s1.Shutdown()
	s2 := RunServer(pass2CollisionOpts("S2", 3, []*url.URL{pass2RouteURL(t, s1)}))
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)
	// Wait for the dedicated route for account "0" on S1 and the pooled pool.
	checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		s1.mu.RLock()
		nd := len(s1.accRoutes["0"])
		np := 0
		for _, conns := range s1.routes {
			for _, c := range conns {
				if c != nil {
					np++
				}
			}
		}
		s1.mu.RUnlock()
		if nd != 1 || np != 3 {
			return fmt.Errorf("dedicated=%d pooled=%d (want 1/3)", nd, np)
		}
		return nil
	})
	time.Sleep(200 * time.Millisecond)

	s1.mu.RLock()
	var remoteID, idHash string
	var pooled0 *client
	for rid, conns := range s1.routes {
		remoteID = rid
		pooled0 = conns[0]
	}
	var ded *client
	for _, c := range s1.accRoutes["0"] {
		ded = c
	}
	s1.mu.RUnlock()
	if pooled0 == nil || ded == nil {
		t.Fatalf("missing clients: pooled0=%v ded=%v", pooled0, ded)
	}
	ded.mu.Lock()
	dedIDHash := ded.route.idHash
	dedAcc := string(ded.route.accName)
	ded.mu.Unlock()
	pooled0.mu.Lock()
	pooledIDHash := pooled0.route.idHash
	pooled0.mu.Unlock()
	if dedIDHash != pooledIDHash {
		t.Fatalf("different idHash? ded=%q pooled=%q", dedIDHash, pooledIDHash)
	}
	idHash = dedIDHash

	// The pooled slot 0 key and the dedicated account "0" key are identical.
	wantKey := idHash + "0"
	keys := make(map[string]*client)
	s1.gateway.routesIDByHash.Range(func(k, v any) bool {
		keys[k.(string)] = v.(*client)
		return true
	})
	if len(keys) != 4 {
		t.Errorf("routesIDByHash has %d entries, want 4 (3 pooled + dedicated): keys=%v", len(keys), keys)
	}
	c, ok := keys[wantKey]
	if !ok {
		t.Fatalf("no entry for %q, map=%v", wantKey, keys)
	}
	c.mu.Lock()
	gotAcc := string(c.route.accName)
	c.mu.Unlock()
	t.Logf("key %q -> accName=%q (dedicated accName=%q)", wantKey, gotAcc, dedAcc)
	if gotAcc != "0" {
		t.Errorf("key %q holds pooled route, expected the dedicated route to have overwritten it", wantKey)
	}

	// A pooled account whose index is 0 now resolves to the dedicated route.
	var pooledAcc string
	s1.accRouteByHash.Range(func(k, v any) bool {
		if idx, ok := v.(int); ok && idx == 0 && k.(string) != "0" {
			pooledAcc = k.(string)
			return false
		}
		return true
	})
	if pooledAcc == "" {
		t.Fatalf("no pooled account with index 0")
	}
	got, perAccount := s1.getRouteByHash([]byte(idHash), []byte(pooledAcc))
	if got != pooled0 {
		t.Errorf("getRouteByHash(%q, %q) = %p (accName=%q), want the pooled slot-0 client %p",
			idHash, pooledAcc, got, func() string {
				if got == nil {
					return "<nil>"
				}
				got.mu.Lock()
				defer got.mu.Unlock()
				return string(got.route.accName)
			}(), pooled0)
	}
	if perAccount {
		t.Logf("lookup treated %q as per-account", pooledAcc)
	}

	// Removing the dedicated route deletes the shared key, so the live pooled
	// route slot 0 is no longer addressable either.
	s1.removeRoute(ded)
	s1.mu.RLock()
	stillPooled := s1.routes[remoteID][0] != nil
	_, keyStillThere := func() (any, bool) { return s1.gateway.routesIDByHash.Load(wantKey) }()
	s1.mu.RUnlock()
	t.Logf("after dedicated removal: pooled0 alive=%v, key %q present=%v", stillPooled, wantKey, keyStillThere)
	if stillPooled && !keyStillThere {
		t.Errorf("dedicated removal deleted key %q while pooled slot 0 is still alive", wantKey)
	}
}
