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

//go:build !windows && !wasm

package server

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

const g2SigChildEnv = "G2_SIGNAL_CHILD_MODE"

func runG2SignalChild(name string) {
	opts := DefaultOptions()
	opts.NoSigs = false
	opts.DontListen = true
	opts.Cluster.Port = 0
	opts.HTTPPort = 0
	s, err := NewServer(opts)
	if err != nil {
		fmt.Printf("NewServer: %v\n", err)
		os.Exit(4)
	}
	s.Start()

	switch name {
	case "after-shutdown":
		s.Shutdown()
		s.WaitForShutdown()
		// Give the signals goroutine a chance to observe the closed quitCh.
		time.Sleep(500 * time.Millisecond)
		// A single SIGTERM is enough on a fixed server (default disposition
		// is restored once the goroutine returns), but retry so the test does
		// not depend on that goroutine being scheduled within the sleep.
		for i := 0; i < 20; i++ {
			syscall.Kill(os.Getpid(), syscall.SIGTERM)
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Println("ALIVE AFTER SHUTDOWN + SIGTERM")
		os.Exit(3)
	case "while-running":
		// Control: the signal handler must still be active while the server
		// runs, and SIGTERM must shut it down and exit 0.
		time.Sleep(200 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
		time.Sleep(3 * time.Second)
		fmt.Println("ALIVE WHILE RUNNING + SIGTERM")
		os.Exit(5)
	}
	os.Exit(6)
}

func TestG2SignalAfterShutdownChild(t *testing.T) {
	if os.Getenv(g2SigChildEnv) != "after-shutdown" {
		t.Skip()
	}
	runG2SignalChild("after-shutdown")
}

func TestG2SignalWhileRunningChild(t *testing.T) {
	if os.Getenv(g2SigChildEnv) != "while-running" {
		t.Skip()
	}
	runG2SignalChild("while-running")
}

func runG2SignalTest(t *testing.T, child string, mode string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+child+"$", "-test.v")
	cmd.Env = append(os.Environ(), g2SigChildEnv+"="+mode)
	return cmd.CombinedOutput()
}

// TestG2SignalAfterShutdown verifies that once a server has shut down, it no
// longer intercepts process signals. On main, handleSignals registers the
// channels in NewServer and never calls signal.Stop: the goroutine returns on
// s.quitCh, but the registration stays, so SIGTERM is delivered to a channel
// with no reader and the process keeps running instead of taking the default
// action.
func TestG2SignalAfterShutdown(t *testing.T) {
	if os.Getenv(g2SigChildEnv) != "" {
		t.Skip()
	}
	out, err := runG2SignalTest(t, "TestG2SignalAfterShutdownChild", "after-shutdown")
	if err == nil {
		// The child exited 0: the server's signal loop handled SIGTERM.
		t.Logf("child terminated by the server's signal handler\n%s", out)
		return
	}
	if ee, ok := err.(*exec.ExitError); ok {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			t.Logf("child terminated by default signal action (%v)\n%s", ws.Signal(), out)
			return
		}
		if ee.ExitCode() == 3 {
			t.Fatalf("process survived SIGTERM after server Shutdown: signal registration was not released\n%s", out)
		}
	}
	t.Fatalf("child failed: %v\n%s", err, out)
}

// TestG2SignalWhileRunning is the control: a running server must keep handling
// SIGTERM (graceful shutdown, exit 0).
func TestG2SignalWhileRunning(t *testing.T) {
	if os.Getenv(g2SigChildEnv) != "" {
		t.Skip()
	}
	out, err := runG2SignalTest(t, "TestG2SignalWhileRunningChild", "while-running")
	if err != nil {
		t.Fatalf("running server did not handle SIGTERM cleanly: %v\n%s", err, out)
	}
	t.Logf("running server handled SIGTERM and exited 0\n%s", out)
}
