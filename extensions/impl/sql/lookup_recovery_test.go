// Copyright 2026 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

// blackholeListener accepts TCP connections and never answers, so a dial
// hangs until its attempt timeout (or context cancellation) fires. It binds
// an ephemeral port itself and returns it, so callers never race on a
// released port.
type blackholeListener struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
}

func newBlackholeListener(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	b := &blackholeListener{listener: l}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.conns = append(b.conns, c)
			b.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		// Wait for the accept loop to exit first: otherwise a connection
		// accepted concurrently with cleanup could be appended after the
		// drain below and never be closed.
		<-done
		b.mu.Lock()
		conns := b.conns
		b.conns = nil
		b.mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return port
}

// newRejectListener accepts TCP connections and closes them immediately, so
// a dial completes but the handshake fails fast and the retry loop spends
// its time in the backoff sleep. It binds an ephemeral port itself and
// returns it, so callers never race on a released port.
func newRejectListener(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = l.Close() })
	return port
}

// TestLookupConcurrentAccess exercises the lazy connection state machine
// under concurrency: concurrent first accesses, recoveries and a concurrent
// Close. Run with -race.
func TestLookupConcurrentAccess(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	// Simulate the lookup framework injection (real framework context).
	ctx := connection.WithLookupRefID(kctx.Background(), "lookup:race")

	dburl := fmt.Sprintf("sqlite://%s/lookup_race.db", t.TempDir())
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, map[string]any{
		"dburl":      dburl,
		"datasource": "t",
	}))
	require.NoError(t, ls.Connect(ctx, nil))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				// Table "t" does not exist, so queries fail; each failure
				// toggles the recovery state and re-enters ensureConnection.
				_, _ = ls.Lookup(ctx, []string{"a"}, []string{"a"}, []any{1})
			}
		}()
	}
	// Close concurrently with the in-flight lookups.
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = ls.Close(ctx)
	}()
	wg.Wait()
}
