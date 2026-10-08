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

package connection

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// blockingDialConnection blocks its first Dial until released, then
// succeeds even if the lifecycle was canceled meanwhile: it models an
// initial Dial outliving the teardown that raced it. Close counts
// calls and signals, so tests pin exactly-once teardown. The
// late-Dial scenarios below were first identified in #4135 against
// the previous lifecycle model and are re-expressed here against
// Meta-owned teardown.
type blockingDialConnection struct {
	mockConnection
	started    chan struct{}
	release    chan struct{}
	startOnce  sync.Once
	closeCalls atomic.Int32
	events     chan string
	name       string
}

func (c *blockingDialConnection) Dial(ctx api.StreamContext) error {
	c.startOnce.Do(func() { close(c.started) })
	<-c.release
	return nil
}

func (c *blockingDialConnection) Close(ctx api.StreamContext) error {
	if c.closeCalls.Add(1) == 1 && c.events != nil {
		c.events <- c.name + "-closed"
	}
	return nil
}

func waitDialBlocked(t *testing.T, c *blockingDialConnection) {
	t.Helper()
	select {
	case <-c.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Dial did not start")
	}
}

// waitRemoving waits until the key flips to teardown ownership. It is
// hang insurance only: once the entry is removing while the old Dial
// is still blocked, Meta.stop structurally cannot complete, so a
// non-blocking check suffices to prove the waiter is still parked.
func waitRemoving(t *testing.T, key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		m := globalConnectionManager
		m.RLock()
		defer m.RUnlock()
		e, ok := m.connectionPool[key]
		return ok && e.state == entryRemoving
	}, 5*time.Second, 10*time.Millisecond, "entry must flip to removing")
}

// TestNamedReplacementDuringLateDial pins the named-update teardown
// contract when the old initial Dial outlives cancellation: Update
// stays blocked on the initial worker, a late Dial success still
// routes through Meta.stop (provider Close exactly once), and only
// after old teardown completes may the replacement be created.
func TestNamedReplacementDuringLateDial(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("late", "op1")
	events := make(chan string, 4)
	old := &blockingDialConnection{
		started: make(chan struct{}),
		release: make(chan struct{}),
		events:  events,
		name:    "old",
	}
	modules.RegisterConnection("late-named", func(api.StreamContext) modules.Connection { return old })
	modules.RegisterConnection("late-new", func(api.StreamContext) modules.Connection {
		events <- "new-created"
		return &mockConnection{}
	})
	_, err := CreateNamedConnection(ctx, "late-named", "late-named", nil)
	require.NoError(t, err)
	waitDialBlocked(t, old)

	updated := make(chan error, 1)
	go func() {
		replacement, err := UpdateConnection(ctx, "late-named", "late-new", nil)
		if err != nil {
			updated <- err
			return
		}
		_, err = replacement.Wait(ctx)
		updated <- err
	}()
	// Update must stay blocked on the old initial worker: the entry is
	// already removing while Dial is still blocked, so Meta.stop
	// structurally cannot have completed.
	waitRemoving(t, "late-named")
	select {
	case err := <-updated:
		t.Fatalf("Update returned while old Dial blocked: %v", err)
	default:
	}

	// Late success despite cancellation: the worker still routes
	// through Meta.stop before the replacement is created.
	close(old.release)
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Update did not complete after Dial release")
	}
	require.Equal(t, int32(1), old.closeCalls.Load(), "old provider must be Closed exactly once")
	select {
	case got := <-events:
		require.Equal(t, "old-closed", got)
	case <-time.After(2 * time.Second):
		t.Fatal("old teardown was not observed")
	}
	select {
	case got := <-events:
		require.Equal(t, "new-created", got)
	case <-time.After(2 * time.Second):
		t.Fatal("replacement was not created after old teardown")
	}
	require.True(t, checkConn("late-named"))
	require.NoError(t, DropNameConnection(ctx, "late-named"))
}

// TestAnonymousReleaseDuringLateDial pins the same Meta teardown
// contract on the anonymous path: Release stays blocked on the
// initial worker, a late Dial success still routes through stop
// (provider Close exactly once), and the pool entry is gone when
// Release completes.
func TestAnonymousReleaseDuringLateDial(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("late", "op1")
	old := &blockingDialConnection{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	modules.RegisterConnection("late-anon", func(api.StreamContext) modules.Connection { return old })
	lease, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "late-anon", RefID: "r1", Type: "late-anon",
	})
	require.NoError(t, err)
	waitDialBlocked(t, old)

	released := make(chan error, 1)
	go func() {
		released <- lease.Release(ctx)
	}()
	// Release stays blocked waiting for the initial worker: same
	// structural argument as above.
	waitRemoving(t, "late-anon")
	select {
	case err := <-released:
		t.Fatalf("Release returned while Dial blocked: %v", err)
	default:
	}

	close(old.release)
	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Release did not complete after Dial release")
	}
	require.Equal(t, int32(1), old.closeCalls.Load(), "provider must be Closed exactly once")
	require.False(t, checkConn("late-anon"), "pool entry must be gone after Release")
	require.Equal(t, 0, getConnectionRef("late-anon"))
}

// flakyDialConnection fails its first Dial fast with an I/O error and
// counts attempts, so tests can prove the retry loop never runs again.
type flakyDialConnection struct {
	mockConnection
	dialCalls atomic.Int32
	dialed    chan struct{}
}

func (c *flakyDialConnection) Dial(ctx api.StreamContext) error {
	if c.dialCalls.Add(1) == 1 {
		close(c.dialed)
		return errorx.NewIOErr("dial down")
	}
	return nil
}

// TestDialInitialBackoffInterrupted pins the teardown contract fixed in
// #4179: when the lifecycle dies while the worker sleeps between
// retries, the backoff wait ends immediately instead of sleeping out
// the interval. The existing late-Dial tests only cover cancellation
// while Dial itself is blocked and never enter backoff, so they stay
// green even without backoff.WithContext. Here the first Dial fails
// fast, cancel lands before the retry wait, and dialInitial must exit
// promptly with exactly one attempt.
//
// The 40ms bound is not arbitrary: without interruption the first
// backoff sleep lasts at least 50ms (100ms initial interval with the
// backoff default 0.5 randomization factor), so a return within 40ms
// proves the wait was cut short. Do not relax it toward 100ms.
func TestDialInitialBackoffInterrupted(t *testing.T) {
	m := newStateMeta(t)
	conn := &flakyDialConnection{dialed: make(chan struct{})}
	ctx := mockContext.NewMockContext("backoff", "op1")
	connCtx, cancel := ctx.WithCancel()

	done := make(chan error, 1)
	go func() {
		_, err := dialInitial(connCtx, m, conn)
		done <- err
	}()
	// Let the first Dial fail, then kill the lifecycle before retry.
	select {
	case <-conn.dialed:
	case <-time.After(5 * time.Second):
		t.Fatal("first Dial did not run")
	}
	cancel()
	start := time.Now()
	select {
	case err := <-done:
		require.Error(t, err)
		require.Less(t, time.Since(start), 40*time.Millisecond)
	case <-time.After(5 * time.Second):
		t.Fatal("dialInitial did not exit after lifecycle cancel")
	}
	require.Equal(t, int32(1), conn.dialCalls.Load(), "no retry attempt may run after cancel")
}
