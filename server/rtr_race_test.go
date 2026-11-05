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
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
)

const mappedImportRtrRaceConf = `
	port: -1
	accounts {
		EA {
			users: [{user: a, password: p}]
			exports: [{stream: "foo.>"}]
		}
		IB {
			users: [{user: b, password: p}]
			imports: [{stream: {account: EA, subject: "foo.*"}, to: "bar.*"}]
		}
	}
`

// A mapped stream import has a reverse subject transform that addShadowSub
// builds lazily (streamImport.rtr) with no account lock held. Concurrent
// subscribes in the importing account race on that field.
func TestMappedStreamImportReverseTransformRace(t *testing.T) {
	s, _ := RunServerWithConfig(createConfFile(t, []byte(mappedImportRtrRaceConf)))
	defer s.Shutdown()

	const n = 8
	ncs := make([]*nats.Conn, 0, n)
	for i := 0; i < n; i++ {
		ncs = append(ncs, natsConnect(t, s.ClientURL(), nats.UserInfo("b", "p")))
	}
	defer func() {
		for _, nc := range ncs {
			nc.Close()
		}
	}()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if _, err := ncs[i].SubscribeSync(fmt.Sprintf("bar.%d", i)); err != nil {
				t.Errorf("Error subscribing: %v", err)
				return
			}
			if err := ncs[i].Flush(); err != nil {
				t.Errorf("Error flushing: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
}

// Control: the same mapped import with serialized subscribes must be clean
// under -race.
func TestMappedStreamImportReverseTransformSerial(t *testing.T) {
	s, _ := RunServerWithConfig(createConfFile(t, []byte(mappedImportRtrRaceConf)))
	defer s.Shutdown()

	const n = 8
	ncs := make([]*nats.Conn, 0, n)
	for i := 0; i < n; i++ {
		nc := natsConnect(t, s.ClientURL(), nats.UserInfo("b", "p"))
		if _, err := nc.SubscribeSync(fmt.Sprintf("bar.%d", i)); err != nil {
			t.Fatalf("Error subscribing: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("Error flushing: %v", err)
		}
		ncs = append(ncs, nc)
	}
	for _, nc := range ncs {
		nc.Close()
	}
}
