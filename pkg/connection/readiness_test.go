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
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func newStateMeta(t *testing.T) *Meta {
	t.Helper()
	m := newMeta(nil, "state-test", "mock", nil, false)
	// Every Meta births a dispatcher; bare test Metas never go through
	// stop(), so the test owns its cleanup and join here.
	t.Cleanup(m.shutdownDispatcherForTest)
	return m
}

// shutdownDispatcherForTest drains pending events, then stops and joins
// the dispatcher. Test-only cleanup for bare Metas that never go through
// stop() (which performs the same sequence as its final phase). Safe to
// run after stop(): an already-stopped dispatcher is detected under
// eventMu and skipped — stop() joined it already.
func (meta *Meta) shutdownDispatcherForTest() {
	meta.eventMu.Lock()
	if meta.dispatcherStopping {
		meta.eventMu.Unlock()
		return
	}
	meta.eventMu.Unlock()
	meta.drainEvents()
	meta.eventMu.Lock()
	meta.dispatcherStopping = true
	meta.eventMu.Unlock()
	select {
	case meta.eventWake <- struct{}{}:
	default:
	}
	<-meta.dispatcherDone
}

func requireState(t *testing.T, m *Meta, status, errMsg string) {
	t.Helper()
	s, e := m.GetStatus()
	require.Equal(t, status, s)
	require.Equal(t, errMsg, e)
}

// TestStateTransitionTable pins the readiness generation contract:
// status, lastError and readyCh move atomically; leaving connected
// always opens a new parked generation; disconnected<->recovering
// share one generation without wakeups.
func TestStateTransitionTable(t *testing.T) {
	m := newStateMeta(t)
	// Initial: connecting, open generation 0.
	requireState(t, m, api.ConnectionConnecting, "")
	require.False(t, isClosed(m.readyCh))
	gen0 := m.generation

	// Initial dial retries re-report connecting: pure no-op.
	m.NotifyStatus(api.ConnectionConnecting, "")
	requireState(t, m, api.ConnectionConnecting, "")
	require.Equal(t, gen0, m.generation)

	// First failure: still generation 0, channel stays open.
	m.NotifyStatus(api.ConnectionDisconnected, "timeout")
	requireState(t, m, api.ConnectionDisconnected, "timeout")
	require.False(t, isClosed(m.readyCh))
	require.Equal(t, gen0, m.generation)
	ch0 := m.readyCh

	// Runtime reconnect shares the generation: no wakeup, error kept.
	m.NotifyStatus(ConnectionRecovering, "")
	requireState(t, m, ConnectionRecovering, "timeout")
	require.False(t, isClosed(ch0))

	// Recovering may refresh the error, still no new generation.
	m.NotifyStatus(ConnectionRecovering, "reset")
	requireState(t, m, ConnectionRecovering, "reset")
	require.False(t, isClosed(ch0))

	// Recovery completes: generation ends, error cleared.
	m.NotifyStatus(api.ConnectionConnected, "")
	requireState(t, m, api.ConnectionConnected, "")
	require.True(t, isClosed(ch0))

	// Duplicate connected reports are no-ops, never double-close.
	m.NotifyStatus(api.ConnectionConnected, "")
	requireState(t, m, api.ConnectionConnected, "")
	require.True(t, isClosed(ch0))

	// Leaving connected opens a parked generation: new open channel,
	// bumped generation, fresh error.
	m.NotifyStatus(api.ConnectionDisconnected, "boom")
	requireState(t, m, api.ConnectionDisconnected, "boom")
	require.False(t, isClosed(m.readyCh))
	gen1 := m.generation
	require.Greater(t, gen1, gen0)
	ch1 := m.readyCh

	// disconnected<->recovering share generation 1.
	m.NotifyStatus(ConnectionRecovering, "")
	requireState(t, m, ConnectionRecovering, "boom")
	require.False(t, isClosed(ch1))
	require.Equal(t, gen1, m.generation)
	m.NotifyStatus(api.ConnectionDisconnected, "still down")
	requireState(t, m, api.ConnectionDisconnected, "still down")
	require.False(t, isClosed(ch1))
	require.Equal(t, gen1, m.generation)

	// Reconnect ends generation 1.
	m.NotifyStatus(api.ConnectionConnected, "")
	requireState(t, m, api.ConnectionConnected, "")
	require.True(t, isClosed(ch1))

	// Unknown statuses never corrupt state.
	m.NotifyStatus("flying", "x")
	requireState(t, m, api.ConnectionConnected, "")
	require.True(t, isClosed(ch1))
}

// TestConnectedClearsStaleError is the A2 stale-error regression:
// an error recorded by an older generation must not leak into the
// connected state the next generation reaches.
func TestConnectedClearsStaleError(t *testing.T) {
	m := newStateMeta(t)
	m.NotifyStatus(api.ConnectionDisconnected, "timeout")
	m.NotifyStatus(api.ConnectionConnected, "")
	s, e := m.GetStatus()
	require.Equal(t, api.ConnectionConnected, s)
	require.Equal(t, "", e)
}

// TestWaitReadyImmediateConnected returns without blocking.
func TestWaitReadyImmediateConnected(t *testing.T) {
	m := newStateMeta(t)
	m.NotifyStatus(api.ConnectionConnected, "")
	cw := &connWrapper{ID: m.ID, meta: m}
	ctx := mockContext.NewMockContext("r1", "op1")
	require.NoError(t, cw.WaitReady(ctx))
}

// TestWaitReadyRecheckAfterWake parks the waiter across a full
// recover->disconnect cycle (shared generation, no spurious
// success) and releases it only on connected.
func TestWaitReadyRecheckAfterWake(t *testing.T) {
	m := newStateMeta(t)
	m.NotifyStatus(api.ConnectionConnected, "")
	m.NotifyStatus(api.ConnectionDisconnected, "down")
	cw := &connWrapper{ID: m.ID, meta: m}
	ctx := mockContext.NewMockContext("r1", "op1")

	done := make(chan error, 1)
	go func() { done <- cw.WaitReady(ctx) }()

	// disconnected -> recovering -> disconnected must not release.
	m.NotifyStatus(ConnectionRecovering, "")
	m.NotifyStatus(api.ConnectionDisconnected, "still down")
	select {
	case err := <-done:
		t.Fatalf("WaitReady returned early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	m.NotifyStatus(api.ConnectionConnected, "")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitReady did not return after connected")
	}
}

// TestWaitReadyCallerCancelFirst fixes precedence: a canceled caller
// observes ctx.Err(), never a state outcome.
func TestWaitReadyCallerCancelFirst(t *testing.T) {
	m := newStateMeta(t)
	cw := &connWrapper{ID: m.ID, meta: m}
	ctx := mockContext.NewMockContext("r1", "op1")
	canceled, cancel := ctx.WithCancel()
	cancel()
	require.ErrorIs(t, cw.WaitReady(canceled), canceled.Err())

	// Even when connected is racing, a canceled caller wins.
	m.NotifyStatus(api.ConnectionConnected, "")
	require.ErrorIs(t, cw.WaitReady(canceled), canceled.Err())
}

// TestWaitReadyLifecycleClosed maps termination to
// ErrConnectionClosed, both before and during the wait.
func TestWaitReadyLifecycleClosed(t *testing.T) {
	m := newStateMeta(t)
	cw := &connWrapper{ID: m.ID, meta: m}
	ctx := mockContext.NewMockContext("r1", "op1")

	m.lifecycleCancel()
	require.ErrorIs(t, cw.WaitReady(ctx), ErrConnectionClosed)

	// A waiter parked in a dead generation is released as well.
	m2 := newStateMeta(t)
	cw2 := &connWrapper{ID: m2.ID, meta: m2}
	done := make(chan error, 1)
	go func() { done <- cw2.WaitReady(ctx) }()
	time.Sleep(50 * time.Millisecond)
	m2.lifecycleCancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrConnectionClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitReady did not observe lifecycle termination")
	}
}

// TestSuspectClosesGateKeepsStatus pins hard invariant 1: the first
// suspect of a connected episode closes the internal readiness gate
// and opens a fresh parked generation while the public status stays
// connected, plus exactly one worker wakeup.
func TestSuspectClosesGateKeepsStatus(t *testing.T) {
	m := newStateMeta(t)
	m.NotifyStatus(api.ConnectionConnected, "")
	gen0 := m.generation

	m.reportSuspect()

	s, _ := m.GetStatus()
	require.Equal(t, api.ConnectionConnected, s)
	m.stateMu.RLock()
	ready, verifying := m.ready, m.verifying
	m.stateMu.RUnlock()
	require.False(t, ready)
	require.True(t, verifying)
	require.Greater(t, m.generation, gen0)
	require.False(t, isClosed(m.readyCh))
	select {
	case <-m.suspectCh:
	default:
		t.Fatal("expected one worker wakeup")
	}
}

// TestSuspectThenDisconnectSingleGeneration pins the no-double-open
// rule: a verifying episode followed by connected->disconnected opens
// exactly one generation for the outage.
func TestSuspectThenDisconnectSingleGeneration(t *testing.T) {
	m := newStateMeta(t)
	m.NotifyStatus(api.ConnectionConnected, "")
	m.reportSuspect()
	gen, ch := m.generation, m.readyCh

	m.NotifyStatus(api.ConnectionDisconnected, "boom")

	require.Equal(t, gen, m.generation)
	require.True(t, ch == m.readyCh)
	requireState(t, m, api.ConnectionDisconnected, "boom")
}

// TestSuspectByState pins the report table: disconnected nudges
// without state change, connecting and recovering are ignored.
func TestSuspectByState(t *testing.T) {
	disconnected := newStateMeta(t)
	disconnected.NotifyStatus(api.ConnectionDisconnected, "down")
	gen := disconnected.generation
	disconnected.reportSuspect()
	require.Equal(t, gen, disconnected.generation)
	requireState(t, disconnected, api.ConnectionDisconnected, "down")
	select {
	case <-disconnected.suspectCh:
	default:
		t.Fatal("expected a wakeup for the disconnected suspect")
	}

	connecting := newStateMeta(t)
	connecting.reportSuspect()
	require.Equal(t, 0, len(connecting.suspectCh))

	recovering := newStateMeta(t)
	recovering.NotifyStatus(api.ConnectionConnected, "")
	recovering.NotifyStatus(ConnectionRecovering, "")
	recovering.reportSuspect()
	require.Equal(t, 0, len(recovering.suspectCh))
}

// TestWaitReadyParksDuringVerifying is the c2 acceptance: after the
// first suspect, subsequent WaitReady callers park while the status
// still reads connected, and resume when the gate reopens.
func TestWaitReadyParksDuringVerifying(t *testing.T) {
	m := newStateMeta(t)
	m.NotifyStatus(api.ConnectionConnected, "")
	m.reportSuspect()
	cw := &connWrapper{ID: m.ID, meta: m}
	ctx := mockContext.NewMockContext("r1", "op1")

	done := make(chan error, 1)
	go func() { done <- cw.WaitReady(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("WaitReady returned during verification: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	m.NotifyStatus(api.ConnectionConnected, "")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitReady did not return after the gate reopened")
	}
}
