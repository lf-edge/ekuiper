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
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// blackholeListener accepts TCP connections and never answers, so a dial
// hangs until its attempt timeout (or context cancellation) fires.
type blackholeListener struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
}

func newBlackholeListener(t *testing.T, port int) *blackholeListener {
	t.Helper()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	b := &blackholeListener{listener: l}
	go func() {
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
		b.mu.Lock()
		conns := b.conns
		b.conns = nil
		b.mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return b
}

// newRejectListener accepts TCP connections and closes them immediately, so
// a dial completes but the handshake fails fast and the retry loop spends
// its time in the backoff sleep.
func newRejectListener(t *testing.T, port int) {
	t.Helper()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
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
}

// TestLookupConcurrentAccess exercises the lazy connection state machine
// under concurrency: concurrent first accesses, recoveries and a concurrent
// Close. Run with -race.
func TestLookupConcurrentAccess(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("lookup_race", "op1")

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

// TestRetryReconnectCancelDuringAttempt verifies that canceling the rule
// context interrupts a reconnect attempt that is blocked on the network
// instead of waiting out the full attempt timeout.
func TestRetryReconnectCancelDuringAttempt(t *testing.T) {
	rootCtx := mockContext.NewMockContext("reconnect_attempt", "op1")
	ctx, cancel := rootCtx.WithCancel()

	port := closedPort(t)
	newBlackholeListener(t, port)
	conn := client2.CreateConnection(ctx).(*client2.SQLConnection)
	require.NoError(t, conn.Provision(ctx, "attempt", map[string]any{
		"dburl": fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port),
	}))

	result := make(chan error, 1)
	go func() {
		result <- retryReconnect(ctx, conn)
	}()
	time.Sleep(500 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt the reconnect attempt")
	}
}

// TestRetryReconnectCancelDuringBackoff verifies that canceling the rule
// context during the backoff sleep between two attempts exits promptly.
func TestRetryReconnectCancelDuringBackoff(t *testing.T) {
	rootCtx := mockContext.NewMockContext("reconnect_backoff", "op1")
	ctx, cancel := rootCtx.WithCancel()

	// A rejecting endpoint fails each attempt fast (accepted then closed),
	// so the retry loop spends its time in the backoff sleep.
	port := closedPort(t)
	newRejectListener(t, port)
	conn := client2.CreateConnection(ctx).(*client2.SQLConnection)
	require.NoError(t, conn.Provision(ctx, "backoff", map[string]any{
		"dburl": fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port),
	}))

	result := make(chan error, 1)
	go func() {
		result <- retryReconnect(ctx, conn)
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt the backoff sleep")
	}
}
