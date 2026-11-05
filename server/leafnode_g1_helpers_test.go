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
	"sync/atomic"
	"testing"
)

// leafForRemoteServer returns the hub-side leaf connection whose remote server
// name matches name.
func leafForRemoteServer(t *testing.T, s *Server, name string) *client {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, lc := range s.leafs {
		lc.mu.Lock()
		remote := lc.leaf.remoteServer
		lc.mu.Unlock()
		if remote == name {
			return lc
		}
	}
	return nil
}

// hubStoredQW returns the queue weight this server currently stores for the
// remote subscription announced with the given "subject queue" key.
func hubStoredQW(t *testing.T, s *Server, key string) int32 {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, lc := range s.leafs {
		lc.mu.Lock()
		var qw int32
		if sub := lc.subs[key]; sub != nil {
			qw = atomic.LoadInt32(&sub.qw)
		}
		lc.mu.Unlock()
		if qw != 0 {
			return qw
		}
	}
	return 0
}

func leafSmapValue(lc *client, key string) (int64, bool) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.leaf.smap == nil {
		return 0, false
	}
	return lc.leaf.smap[key], true
}

// hubTotalQW returns the sum of the queue weights this server stores for all
// remote subscriptions announced with the given "subject queue" key.
func hubTotalQW(t *testing.T, s *Server, key string) int32 {
	t.Helper()
	var total int32
	s.mu.RLock()
	for _, lc := range s.leafs {
		lc.mu.Lock()
		if sub := lc.subs[key]; sub != nil {
			total += atomic.LoadInt32(&sub.qw)
		}
		lc.mu.Unlock()
	}
	s.mu.RUnlock()
	return total
}
