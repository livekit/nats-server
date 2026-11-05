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

	"github.com/nats-io/nats.go"
)

// Two stream imports from the SAME exporting account whose subjects both match
// one subscription produce two shadows with the same subject and sid in the
// exporting account. addShadowSubscriptions creates one shadow per matching
// import entry and never dedupes by resolved subject, so every message in the
// overlap is delivered once per shadow: the subscriber gets N copies of a
// single publish, and the exporting account's route/leaf interest is inflated
// by the same factor.
func TestOverlappingStreamImportsDeliverDuplicates(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		cluster {
			name: "A"
			listen: 127.0.0.1:-1
		}
		accounts: {
			EXP: {
				users = [{user: "exp", password: "pwd"}]
				exports = [{stream: "foo.>"}]
			}
			IMP: {
				users = [{user: "imp", password: "pwd"}]
				imports = [
					{stream: {account: "EXP", subject: "foo.>"}}
					{stream: {account: "EXP", subject: "foo.bar"}}
				]
			}
		}
	`
	s, _ := RunServerWithConfig(createConfFile(t, []byte(conf)))
	defer s.Shutdown()

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)

	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	sub, err := ncImp.SubscribeSync("foo.bar")
	require_NoError(t, err)
	require_NoError(t, ncImp.Flush())

	key := keyFromSubWithOrigin(&subscription{subject: []byte("foo.bar")})
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(overlapSubs(expAcc, "foo.bar")); n != 2 {
			return fmt.Errorf("EXP has %d shadow(s) for foo.bar, want 2", n)
		}
		return nil
	})
	shadows := overlapSubs(expAcc, "foo.bar")
	expAcc.mu.RLock()
	rm := expAcc.rm[key]
	expAcc.mu.RUnlock()
	t.Logf("EXP shadows=%d (sid %q/%q) rm[%q]=%d",
		len(shadows), shadows[0].sid, shadows[1].sid, key, rm)

	ncExp := natsConnect(t, s.ClientURL(), nats.UserInfo("exp", "pwd"))
	defer ncExp.Close()
	require_NoError(t, ncExp.Publish("foo.bar", []byte("hello")))
	require_NoError(t, ncExp.Flush())

	var got int
	for i := 0; i < 5; i++ {
		if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
			break
		}
		got++
	}
	t.Logf("subscriber received %d copy(ies) of one publish; EXP rm[%q]=%d", got, key, rm)

	var problems []string
	if got != 1 {
		problems = append(problems, fmt.Sprintf("subscriber received %d copies of one publish, want 1", got))
	}
	if rm != 1 {
		problems = append(problems, fmt.Sprintf("EXP remote interest for one subscriber is %d, want 1", rm))
	}
	if len(problems) > 0 {
		t.Fatalf("overlapping stream imports: %v", problems)
	}
}

// Queue flavour: delivery stays single-copy (one member of the group is
// chosen), but the exporting account still sees two shadows in the group, so
// the announced queue weight is 2 for a group with one member.
func TestOverlappingStreamImportsInflateQueueWeight(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		cluster {
			name: "A"
			listen: 127.0.0.1:-1
		}
		accounts: {
			EXP: {
				users = [{user: "exp", password: "pwd"}]
				exports = [{stream: "foo.>"}]
			}
			IMP: {
				users = [{user: "imp", password: "pwd"}]
				imports = [
					{stream: {account: "EXP", subject: "foo.>"}}
					{stream: {account: "EXP", subject: "foo.bar"}}
				]
			}
		}
	`
	s, _ := RunServerWithConfig(createConfFile(t, []byte(conf)))
	defer s.Shutdown()

	expAcc, err := s.LookupAccount("EXP")
	require_NoError(t, err)

	ncImp := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "pwd"))
	defer ncImp.Close()
	_, err = ncImp.QueueSubscribeSync("foo.bar", "q")
	require_NoError(t, err)
	require_NoError(t, ncImp.Flush())

	key := keyFromSubWithOrigin(&subscription{subject: []byte("foo.bar"), queue: []byte("q")})
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if n := len(overlapSubs(expAcc, "foo.bar")); n != 2 {
			return fmt.Errorf("EXP has %d shadow(s) for foo.bar, want 2", n)
		}
		return nil
	})
	expAcc.mu.RLock()
	rm := expAcc.rm[key]
	expAcc.mu.RUnlock()
	t.Logf("EXP queue shadows=2 rm[%q]=%d", key, rm)
	if rm != 1 {
		t.Fatalf("EXP queue weight for a one-member group is %d, want 1", rm)
	}
}

func overlapSubs(a *Account, subject string) []*subscription {
	a.mu.RLock()
	sr := a.sl.Match(subject)
	subs := append([]*subscription(nil), sr.psubs...)
	for _, qs := range sr.qsubs {
		subs = append(subs, qs...)
	}
	a.mu.RUnlock()
	return subs
}
