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
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// countCloseConnection counts logical Close calls; Close blocks on
// countCloseRelease so tests can hold the removing window open
// deterministically and observe Fetch/Create rejection mid-stop.
var (
	countCloseRelease = make(chan struct{}, 1)
	countCloseCalls   atomic.Int32
)

type countCloseConnection struct {
	id string
}

func (c *countCloseConnection) GetId(ctx api.StreamContext) string {
	return c.id
}

func (c *countCloseConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	c.id = conId
	return nil
}

func (c *countCloseConnection) Dial(ctx api.StreamContext) error {
	return nil
}

func (c *countCloseConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (c *countCloseConnection) Close(ctx api.StreamContext) error {
	countCloseCalls.Add(1)
	<-countCloseRelease
	return nil
}

func CreateCountCloseConnection(ctx api.StreamContext) modules.Connection {
	return &countCloseConnection{}
}

func drainCountCloseRelease() {
	select {
	case <-countCloseRelease:
	default:
	}
}

// TestZeroRefRemovingFetchWaits holds the removing window open with a
// blocked Close and proves a concurrent Fetch waits the teardown out
// (instead of failing or attaching to the dying Meta), then creates fresh
// once cleanup deletes the entry.
func TestZeroRefRemovingFetchWaits(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	drainCountCloseRelease()
	countCloseCalls.Store(0)
	ctx := mockContext.NewMockContext("stop", "op1")

	oldCW, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "stop-anon", RefID: "r1", Type: "countclose",
	})
	require.NoError(t, err)

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- DetachConnectionByRef(ctx, "stop-anon", "r1")
	}()
	// Wait until the stopper owns the key: the entry flips to removing
	// synchronously inside Detach, before Close blocks.
	require.Eventually(t, func() bool {
		m := globalConnectionManager.Load()
		m.RLock()
		defer m.RUnlock()
		e, ok := m.connectionPool["stop-anon"]
		return ok && e.state == entryRemoving
	}, 5*time.Second, 10*time.Millisecond, "entry must flip to removing")

	// Deterministic branch proof, no goroutine scheduling involved: a
	// canceled caller on a removing key must come back through the
	// wait path (ctx.Err()), never through fail-fast Removing.
	canceled, cancel := ctx.WithCancel()
	cancel()
	_, err = FetchConnectionWithOptions(canceled, FetchOptions{
		ConnectionKey: "stop-anon", RefID: "r2", Type: "countclose",
	})
	require.ErrorIs(t, err, canceled.Err())

	// Release the teardown, then prove the waiter-visible outcome: a
	// fresh generation is created and attached after cleanup.
	countCloseRelease <- struct{}{}
	require.NoError(t, <-stopDone)
	require.Equal(t, int32(1), countCloseCalls.Load())
	newCW, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "stop-anon", RefID: "r2", Type: "countclose",
	})
	require.NoError(t, err)
	require.NotSame(t, oldCW, newCW, "post-cleanup fetch must attach to the fresh generation")
	_, ok := globalConnectionManager.Load().connectionPool["stop-anon"]
	require.True(t, ok)

	stopDone2 := make(chan error, 1)
	go func() {
		stopDone2 <- DetachConnectionByRef(ctx, "stop-anon", "r2")
	}()
	countCloseRelease <- struct{}{}
	require.NoError(t, <-stopDone2)
	require.Equal(t, int32(2), countCloseCalls.Load())
}

// TestAttachVsZeroRefFetchWins pins the fetch-first order
// deterministically (no concurrency): with a second ref attached, the
// last detach must stand down instead of stopping.
func TestAttachVsZeroRefFetchWins(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	drainCountCloseRelease()
	countCloseCalls.Store(0)
	ctx := mockContext.NewMockContext("stop", "op1")

	_, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "fetch-wins", RefID: "rA", Type: "countclose",
	})
	require.NoError(t, err)
	_, err = FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "fetch-wins", RefID: "rB", Type: "countclose",
	})
	require.NoError(t, err)

	// Fetch won: one detach leaves a live ref, so no stop runs.
	require.NoError(t, DetachConnectionByRef(ctx, "fetch-wins", "rA"))
	require.Equal(t, 1, getConnectionRef("fetch-wins"))
	require.Equal(t, int32(0), countCloseCalls.Load())
	meta := getReadyTestMeta("fetch-wins")
	require.NotNil(t, meta, "entry must stay ready while refs remain")

	// Last detach stops and removes; Close runs exactly once.
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- DetachConnectionByRef(ctx, "fetch-wins", "rB")
	}()
	countCloseRelease <- struct{}{}
	require.NoError(t, <-stopDone)
	require.Equal(t, int32(1), countCloseCalls.Load())
	_, ok := globalConnectionManager.Load().connectionPool["fetch-wins"]
	require.False(t, ok)
}

// TestNamedDropRemovingRejectsNamedFetch proves the fail-fast half of
// the removing contract: a RequireExisting (selector/named) fetch on a
// teardown-owned key errors immediately with ErrConnectionRemoving
// instead of waiting. Anonymous fetches wait instead (see
// TestZeroRefRemovingFetchWaits); after cleanup the selector reports the
// key as gone.
func TestNamedDropRemovingRejectsNamedFetch(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	drainCountCloseRelease()
	countCloseCalls.Store(0)
	ctx := mockContext.NewMockContext("stop", "op1")

	_, err := CreateNamedConnection(ctx, "stop-named", "countclose", nil)
	require.NoError(t, err)

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- DropNameConnection(ctx, "stop-named")
	}()
	require.Eventually(t, func() bool {
		m := globalConnectionManager.Load()
		m.RLock()
		defer m.RUnlock()
		e, ok := m.connectionPool["stop-named"]
		return ok && e.state == entryRemoving
	}, 5*time.Second, 10*time.Millisecond, "entry must flip to removing")

	_, err = FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "stop-named", RefID: "r1", RequireExisting: true, Type: "countclose",
	})
	require.ErrorIs(t, err, ErrConnectionRemoving)

	countCloseRelease <- struct{}{}
	require.NoError(t, <-stopDone)
	require.Equal(t, int32(1), countCloseCalls.Load())

	_, err = FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "stop-named", RefID: "r2", RequireExisting: true, Type: "countclose",
	})
	require.ErrorContains(t, err, "not existed")
}

// TestConcurrentDropClosesOnce proves stop idempotency with a
// deterministic dual caller: A owns the stop (Close blocked), B's
// concurrent public Drop observes removing and returns nil under the
// idempotent DELETE policy. Exactly one Close runs.
func TestConcurrentDropClosesOnce(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	drainCountCloseRelease()
	countCloseCalls.Store(0)
	ctx := mockContext.NewMockContext("stop", "op1")

	_, err := CreateNamedConnection(ctx, "stop-once", "countclose", nil)
	require.NoError(t, err)

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- DropNameConnection(ctx, "stop-once")
	}()
	// A owns the stop: entry flips to removing synchronously, before
	// Close blocks.
	require.Eventually(t, func() bool {
		m := globalConnectionManager.Load()
		m.RLock()
		defer m.RUnlock()
		e, ok := m.connectionPool["stop-once"]
		return ok && e.state == entryRemoving
	}, 5*time.Second, 10*time.Millisecond, "entry must flip to removing")

	// B runs synchronously while A is blocked: idempotent nil.
	require.NoError(t, DropNameConnection(ctx, "stop-once"))

	countCloseRelease <- struct{}{}
	require.NoError(t, <-stopDone)
	require.Equal(t, int32(1), countCloseCalls.Load(), "Close runs exactly once")
	_, ok := globalConnectionManager.Load().connectionPool["stop-once"]
	require.False(t, ok, "entry removed after stop completes")
}

// TestRepeatedDetachStaysNil verifies double-Close idempotency on the
// public path: the second detach finds nothing and stays nil.
func TestRepeatedDetachStaysNil(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("stop", "op1")
	_, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "dbl", RefID: "r1", Type: "mock",
	})
	require.NoError(t, err)
	require.NoError(t, DetachConnectionByRef(ctx, "dbl", "r1"))
	require.NoError(t, DetachConnectionByRef(ctx, "dbl", "r1"))
	require.Equal(t, 0, getConnectionRef("dbl"))
}
