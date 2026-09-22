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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// recoverableFakeConn is a scripted PoolRecoverableConnection: it
// reuses probeConn for Provision/Dial/Ping/Close and adds a scripted
// Recover plus an order log for the stop-join test.
type recoverableFakeConn struct {
	probeConn
	recoverErr   error
	recoverCalls atomic.Int32
	// onRecover scripts per-attempt errors when set (call count
	// starts at 1); nil falls back to recoverErr.
	onRecover func(call int32) error
	// recoverRelease gates Recover's return; nil returns at once.
	recoverRelease chan struct{}
	// slowCancelTail simulates an uncancellable provider wind-down
	// after ctx death (kept short, only for the join test).
	slowCancelTail time.Duration
	mu             sync.Mutex
	events         []string
	closedCalls    atomic.Int32
}

func (f *recoverableFakeConn) Recover(ctx api.StreamContext) error {
	call := f.recoverCalls.Add(1)
	if f.recoverRelease != nil {
		select {
		case <-f.recoverRelease:
		case <-ctx.Done():
			if f.slowCancelTail > 0 {
				time.Sleep(f.slowCancelTail)
			}
		}
	}
	f.mu.Lock()
	f.events = append(f.events, "recover-return")
	f.mu.Unlock()
	if f.onRecover != nil {
		return f.onRecover(call)
	}
	return f.recoverErr
}

func (f *recoverableFakeConn) Close(ctx api.StreamContext) error {
	f.closedCalls.Add(1)
	f.mu.Lock()
	f.events = append(f.events, "close")
	f.mu.Unlock()
	return nil
}

func (f *recoverableFakeConn) recordedEvents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

// newRecoverableFake builds a fake with live counters (the embedded
// probeConn counters must be non-nil before any Ping/Dial).
func newRecoverableFake() *recoverableFakeConn {
	return &recoverableFakeConn{
		probeConn: probeConn{pingCalls: &atomic.Int32{}, dialCalls: &atomic.Int32{}},
	}
}

// bothFakeConn implements StatefulDialer AND PoolRecoverableConnection:
// the Pool must treat it as self-recovering and start no worker.
type bothFakeConn struct {
	recoverableFakeConn
	handler api.StatusChangeHandler
}

func (b *bothFakeConn) SetStatusChangeHandler(ctx api.StreamContext, handler api.StatusChangeHandler) {
	b.handler = handler
}

func (b *bothFakeConn) Status(ctx api.StreamContext) modules.ConnectionStatus {
	return modules.ConnectionStatus{}
}

// newWorkerMeta builds a connected Meta with a running recovery
// worker on millisecond backoff. Caller owns lifecycleCancel.
func newWorkerMeta(t *testing.T, fake *recoverableFakeConn) *Meta {
	t.Helper()
	old := newRecoveryBackoff
	newRecoveryBackoff = func() *backoff.ExponentialBackOff {
		b := backoff.NewExponentialBackOff()
		b.InitialInterval = time.Millisecond
		b.RandomizationFactor = 0
		b.MaxInterval = 5 * time.Millisecond
		b.MaxElapsedTime = 0
		b.Reset()
		return b
	}
	t.Cleanup(func() { newRecoveryBackoff = old })
	m := newStateMeta()
	m.NotifyStatus(api.ConnectionConnected, "")
	// Wire the handle back-pointer like attachToMeta does; the
	// worker tests drive suspects through the public ConnWrapper API.
	m.cw = &ConnWrapper{ID: m.ID, meta: m}
	m.startRecoveryWorker(fake)
	require.NotNil(t, m.recoveryDone)
	return m
}

func workerGate(t *testing.T, m *Meta) (ready, verifying bool) {
	t.Helper()
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.ready, m.verifying
}

// TestRecoveryWorkerNotStarted pins the c1 ownership rule: plain
// providers and self-recovering (stateful, even if also
// pool-recoverable) connections get no worker.
func TestRecoveryWorkerNotStarted(t *testing.T) {
	plain := newStateMeta()
	plain.NotifyStatus(api.ConnectionConnected, "")
	plain.startRecoveryWorker(&probeConn{})
	require.Nil(t, plain.recoveryDone)

	both := newStateMeta()
	both.NotifyStatus(api.ConnectionConnected, "")
	both.startRecoveryWorker(&bothFakeConn{})
	require.Nil(t, both.recoveryDone)
}

// TestRecoveryFalseAlarmReopensGate: a suspect whose verification
// Ping passes reopens the gate without ever calling Recover.
func TestRecoveryFalseAlarmReopensGate(t *testing.T) {
	fake := newRecoverableFake()
	m := newWorkerMeta(t, fake)
	defer m.lifecycleCancel()

	m.cw.ReportSuspectedFailure()
	require.Eventually(t, func() bool {
		ready, _ := workerGate(t, m)
		return ready
	}, 3*time.Second, 5*time.Millisecond)
	s, _ := m.GetStatus()
	require.Equal(t, api.ConnectionConnected, s)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), fake.recoverCalls.Load())
}

// TestRecoveryVerifyFailRecovers drives the full episode:
// suspect -> failed verify -> disconnected -> backoff ->
// recovering -> Recover -> connected, with recovering observed on
// the event stream.
func TestRecoveryVerifyFailRecovers(t *testing.T) {
	fake := newRecoverableFake()
	fake.pingErr = errors.New("verify down")
	m := newWorkerMeta(t, fake)
	defer m.lifecycleCancel()

	var mu sync.Mutex
	var seen []string
	m.AddRef("watcher", func(s, e string) {
		mu.Lock()
		seen = append(seen, s)
		mu.Unlock()
	})

	m.cw.ReportSuspectedFailure()
	require.Eventually(t, func() bool {
		ready, _ := workerGate(t, m)
		s, _ := m.GetStatus()
		return ready && s == api.ConnectionConnected
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), fake.recoverCalls.Load())
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, seen, ConnectionRecovering)
}

// TestRecoveryRetriesDisconnectedEpisode: the worker owns a failed
// episode — it keeps attempting with backoff (gate closed, latest
// error recorded) until the lifecycle ends, so a parked consumer
// never needs to fail again to keep recovery going.
func TestRecoveryRetriesDisconnectedEpisode(t *testing.T) {
	fake := newRecoverableFake()
	fake.recoverErr = errors.New("recover down")
	fake.pingErr = errors.New("verify down")
	m := newWorkerMeta(t, fake)

	m.cw.ReportSuspectedFailure()
	// The episode persists across attempts: more than one Recover,
	// still disconnected, gate still closed.
	require.Eventually(t, func() bool {
		return fake.recoverCalls.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond)
	s, e := m.GetStatus()
	require.Equal(t, api.ConnectionDisconnected, s)
	require.Equal(t, "recover down", e)
	ready, _ := workerGate(t, m)
	require.False(t, ready)

	// Lifecycle end exits the episode: no hang, worker joined.
	m.lifecycleCancel()
	require.Eventually(t, func() bool {
		select {
		case <-m.recoveryDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, 5*time.Millisecond)
}

// TestRecoverySucceedsAfterRetries: failed attempts keep the episode
// open; a later success reopens the gate for all parked waiters.
func TestRecoverySucceedsAfterRetries(t *testing.T) {
	fake := newRecoverableFake()
	fake.pingErr = errors.New("verify down")
	fake.onRecover = func(call int32) error {
		if call < 3 {
			return errors.New("still down")
		}
		return nil
	}
	m := newWorkerMeta(t, fake)
	defer m.lifecycleCancel()

	m.cw.ReportSuspectedFailure()
	require.Eventually(t, func() bool {
		ready, _ := workerGate(t, m)
		s, _ := m.GetStatus()
		return ready && s == api.ConnectionConnected
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(3), fake.recoverCalls.Load())
}

// TestRecoveryStormSingleAttempt: a suspect storm collapsing into one
// episode produces exactly one Recover — reports landing before and
// during the attempt are absorbed by its verdict.
func TestRecoveryStormSingleAttempt(t *testing.T) {
	fake := newRecoverableFake()
	fake.recoverRelease = make(chan struct{})
	fake.pingErr = errors.New("verify down")
	m := newWorkerMeta(t, fake)
	defer m.lifecycleCancel()

	for i := 0; i < 50; i++ {
		m.cw.ReportSuspectedFailure()
	}
	require.Eventually(t, func() bool {
		return fake.recoverCalls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond)
	// The whole storm lands while the attempt is gated.
	time.Sleep(100 * time.Millisecond)
	close(fake.recoverRelease)
	require.Eventually(t, func() bool {
		ready, _ := workerGate(t, m)
		return ready
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), fake.recoverCalls.Load())
}

// TestRecoveryStaleWakeupIgnored: a buffered slot with no sequence
// bump wakes the worker and re-parks without touching the provider.
func TestRecoveryStaleWakeupIgnored(t *testing.T) {
	fake := newRecoverableFake()
	m := newWorkerMeta(t, fake)
	defer m.lifecycleCancel()
	pingsBefore := fake.pingCalls.Load()

	m.suspectCh <- struct{}{}
	time.Sleep(150 * time.Millisecond)

	require.Equal(t, pingsBefore, fake.pingCalls.Load())
	require.Equal(t, int32(0), fake.recoverCalls.Load())
	ready, _ := workerGate(t, m)
	require.True(t, ready)
}

// TestRecoveryStopJoinsWorker pins hard invariant 2: stop() waits for
// an in-flight Recover (even through its uncancellable tail) and
// Close runs strictly after the worker exits.
func TestRecoveryStopJoinsWorker(t *testing.T) {
	fake := newRecoverableFake()
	fake.recoverRelease = make(chan struct{})
	fake.slowCancelTail = 300 * time.Millisecond
	fake.pingErr = errors.New("verify down")
	m := newWorkerMeta(t, fake)
	m.cw = &ConnWrapper{ID: m.ID, meta: m, initialized: true, conn: fake, readCh: make(chan struct{})}
	close(m.cw.readCh)
	// Simulate the initial worker already exited.
	close(m.done)

	m.cw.ReportSuspectedFailure()
	require.Eventually(t, func() bool {
		return fake.recoverCalls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond)

	ctx := mockContext.NewMockContext("stop", "op1")
	stopped := make(chan struct{})
	go func() {
		m.stop(ctx)
		close(stopped)
	}()
	// stop() canceled the lifecycle, but the worker's uncancellable
	// tail still holds it: stop must stay blocked here.
	select {
	case <-stopped:
		t.Fatal("stop returned while Recover was still in flight")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return after the worker exited")
	}
	require.Equal(t, []string{"recover-return", "close"}, fake.recordedEvents())
	require.Equal(t, int32(1), fake.closedCalls.Load())
}

// TestProbeNudgesWorkerOnFlip pins hard invariant 3 at the probe
// boundary: the flip the probe performs pairs with exactly one
// worker wakeup sequence.
func TestProbeNudgesWorkerOnFlip(t *testing.T) {
	registerProbeProviders()
	ctx := probeTestCtx()
	_, err := CreateNamedConnection(ctx, "probe-nudge", "failping", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-nudge")

	requireConnected(t, "probe-nudge")
	meta := probeMeta(t, "probe-nudge")
	require.Equal(t, uint64(0), meta.currentSuspectSeq())

	probeConnections(time.Second)

	s, _ := meta.GetStatus()
	require.Equal(t, api.ConnectionDisconnected, s)
	require.Equal(t, uint64(1), meta.currentSuspectSeq())
	require.Equal(t, 1, len(meta.suspectCh))
}
