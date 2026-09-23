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
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = time.Millisecond
	bo.RandomizationFactor = 0
	bo.MaxInterval = 5 * time.Millisecond
	bo.MaxElapsedTime = 0
	bo.Reset()
	m := newStateMeta(t)
	m.NotifyStatus(api.ConnectionConnected, "")
	// Wire the handle back-pointer like attachToMeta does; the
	// worker tests drive suspects through the public ConnWrapper API.
	// The worker is started directly (same package): recoveryDone is
	// initialized here exactly as startRecoveryWorker would, and stop
	// paths join it the same way.
	m.cw = &ConnWrapper{ID: m.ID, meta: m}
	m.recoveryDone = make(chan struct{})
	go m.recoveryLoop(fake, bo)
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
	plain := newStateMeta(t)
	plain.NotifyStatus(api.ConnectionConnected, "")
	plain.startRecoveryWorker(&probeConn{})
	require.Nil(t, plain.recoveryDone)

	both := newStateMeta(t)
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
	// The episode persists across attempts: more than one Recover
	// while the gate stays closed and the latest error is kept. The
	// public phase is deliberately not pinned — a persistent episode
	// cycles disconnected<->recovering, so asserting one exact phase
	// after "attempts >= 2" races the worker's own announcement
	// (Recover increments its counter before returning, i.e. before
	// the next disconnected is announced).
	require.Eventually(t, func() bool {
		if fake.recoverCalls.Load() < 2 {
			return false
		}
		_, errMsg := m.GetStatus()
		ready, _ := workerGate(t, m)
		return !ready && errMsg == "recover down"
	}, 5*time.Second, 5*time.Millisecond)

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

// TestRecoverySecondEpisodeNotSwallowed pins that a newer episode
// reported during a slow status delivery is honored, never absorbed:
// the first suspect verifies healthy and reopens the gate, a waiter
// fails on the new handle and reports again while the reopen delivery
// is still blocked, and the worker must verify a second time.
func TestRecoverySecondEpisodeNotSwallowed(t *testing.T) {
	fake := newRecoverableFake()
	m := newWorkerMeta(t, fake)
	defer m.lifecycleCancel()
	ctx := mockContext.NewMockContext("r1", "op1")

	// The first reopen delivery blocks the dispatcher (FIFO stalls
	// behind the slow consumer), while state already reopened.
	blocked := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	first.Store(true)
	m.AddRef("blocker", func(s, e string) {
		if first.CompareAndSwap(true, false) {
			close(blocked)
			<-release
		}
	})

	m.cw.ReportSuspectedFailure()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("reopen delivery did not start")
	}
	// The gate already reopened in state (delivery is only the
	// callback): a waiter passes and reports the new-handle failure
	// while the old delivery is still blocked.
	require.NoError(t, m.cw.WaitReady(ctx))
	m.cw.ReportSuspectedFailure()
	ready, verifying := workerGate(t, m)
	require.False(t, ready)
	require.True(t, verifying)
	close(release)

	// The worker must act on the second episode: a second Ping and a
	// reopened gate. Absorbing the new report would park here forever.
	require.Eventually(t, func() bool {
		ready, _ := workerGate(t, m)
		return ready && fake.pingCalls.Load() == 2
	}, 5*time.Second, 5*time.Millisecond)
}

// TestRecoveryNotBlockedBySlowCallback is the dispatcher blocker
// proof: a consumer callback parked downstream must not stall the
// recovery worker. The suspect's Ping fails, disconnected lands in
// state (gate closes, waiters park) while its delivery is still
// queued behind the blocked handler — and Recover proceeds to
// success regardless.
func TestRecoveryNotBlockedBySlowCallback(t *testing.T) {
	fake := newRecoverableFake()
	fake.pingErr = errors.New("verify down")
	// Pin the episode open: Recover blocks until released, so the
	// disconnected middle state is stable while we observe it.
	recoverRelease := make(chan struct{})
	fake.recoverRelease = recoverRelease
	m := newWorkerMeta(t, fake)
	defer m.lifecycleCancel()

	entered := make(chan struct{})
	handlerRelease := make(chan struct{})
	var enterOnce sync.Once
	m.AddRef("blocked", func(s, e string) {
		enterOnce.Do(func() { close(entered) })
		<-handlerRelease
	})

	m.cw.ReportSuspectedFailure()
	// The disconnected event reaches the parked handler: delivery
	// backpressure genuinely exists in this scenario.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher never reached the blocked handler")
	}
	// The worker proceeds into Recover while the callback is still
	// blocked and the gate stays closed: delivery backpressure never
	// becomes recovery backpressure. (The public status by now is
	// disconnected or recovering — both transient on the way into
	// the owned episode, so the stable assertions are the gate and
	// the Recover call.)
	require.Eventually(t, func() bool {
		ready, _ := workerGate(t, m)
		return !ready && fake.recoverCalls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond)
	close(handlerRelease)
	close(recoverRelease)
	require.Eventually(t, func() bool {
		ready, _ := workerGate(t, m)
		s, _ := m.GetStatus()
		return ready && s == api.ConnectionConnected
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), fake.recoverCalls.Load())
}

// TestRecoveryStaleWakeupIgnored: a buffered slot against settled
// state wakes the worker and re-parks without touching the provider.
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

// TestProbeNudgesWorkerToRecover is the probe→worker behavior
// contract: a probe-confirmed flip hands the episode to the Pool
// worker, whose Recover runs and converges once the backend heals.
// It asserts behavior (Recover ran, gate reopened), never channel
// buffer internals.
func TestProbeNudgesWorkerToRecover(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	registerEpisodeProvider()
	episodeFake.sick.Store(false)
	defer episodeFake.sick.Store(false)
	ctx := probeTestCtx()

	cw, err := CreateNamedConnection(ctx, "probe-recovers", "recovtest", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "probe-recovers")
	require.Eventually(t, func() bool {
		s, _ := cw.Status()
		return s == api.ConnectionConnected
	}, 5*time.Second, 5*time.Millisecond)
	recoversBefore := episodeFake.recoverCalls.Load()

	// Outage: the probe flips connected→disconnected and nudges the
	// worker, which owns the episode from here.
	episodeFake.sick.Store(true)
	probeConnections(time.Second)
	require.Eventually(t, func() bool {
		s, _ := cw.Status()
		return s == api.ConnectionDisconnected
	}, 5*time.Second, 5*time.Millisecond)

	// Heal: the worker's retries converge and reopen the gate.
	episodeFake.sick.Store(false)
	require.Eventually(t, func() bool {
		s, _ := cw.Status()
		return s == api.ConnectionConnected
	}, 15*time.Second, 10*time.Millisecond)
	require.Greater(t, episodeFake.recoverCalls.Load(), recoversBefore)
}

// episodeFakeConn is a scriptable PoolRecoverableConnection for the
// end-to-end test: sickness toggles Ping/Recover outcomes at runtime.
type episodeFakeConn struct {
	id           string
	sick         atomic.Bool
	pingCalls    atomic.Int32
	recoverCalls atomic.Int32
}

func (e *episodeFakeConn) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	e.id = conId
	return nil
}

func (e *episodeFakeConn) Dial(ctx api.StreamContext) error { return nil }

func (e *episodeFakeConn) GetId(ctx api.StreamContext) string { return e.id }

func (e *episodeFakeConn) Ping(ctx api.StreamContext) error {
	e.pingCalls.Add(1)
	if e.sick.Load() {
		return errors.New("episode down")
	}
	return nil
}

func (e *episodeFakeConn) Recover(ctx api.StreamContext) error {
	e.recoverCalls.Add(1)
	if e.sick.Load() {
		return errors.New("episode still down")
	}
	return nil
}

func (e *episodeFakeConn) Close(ctx api.StreamContext) error { return nil }

var episodeFake = &episodeFakeConn{}

func registerEpisodeProvider() {
	modules.RegisterConnection("recovtest", func(ctx api.StreamContext) modules.Connection {
		return episodeFake
	})
}

// TestRecoveryEpisodeEndToEnd drives a full outage through public
// APIs only: suspect -> verify -> disconnected (waiters park) ->
// worker-owned retries -> heal -> connected (waiters resume).
func TestRecoveryEpisodeEndToEnd(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	registerEpisodeProvider()
	episodeFake.sick.Store(false)
	ctx := probeTestCtx()

	cw, err := CreateNamedConnection(ctx, "recov-e2e", "recovtest", nil)
	require.NoError(t, err)
	defer DropNameConnection(ctx, "recov-e2e")
	require.Eventually(t, func() bool {
		s, _ := cw.Status()
		return s == api.ConnectionConnected
	}, 5*time.Second, 5*time.Millisecond)

	// Outage begins: the first failure surfaces and reports.
	episodeFake.sick.Store(true)
	cw.ReportSuspectedFailure()

	// Two waiters park on the closed gate while the worker owns the
	// failing episode.
	waiter := func() <-chan error {
		done := make(chan error, 1)
		go func() { done <- cw.WaitReady(ctx) }()
		return done
	}
	w1, w2 := waiter(), waiter()
	select {
	case err := <-w1:
		t.Fatalf("waiter released during outage: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.Eventually(t, func() bool {
		s, _ := cw.Status()
		return s == api.ConnectionDisconnected
	}, 5*time.Second, 5*time.Millisecond)

	// Heal: the worker's next attempt succeeds and both waiters resume.
	episodeFake.sick.Store(false)
	for i, w := range []<-chan error{w1, w2} {
		select {
		case err := <-w:
			require.NoError(t, err, "waiter %d", i)
		case <-time.After(15 * time.Second):
			t.Fatalf("waiter %d did not resume after recovery", i)
		}
	}
	s, _ := cw.Status()
	require.Equal(t, api.ConnectionConnected, s)
	require.GreaterOrEqual(t, episodeFake.recoverCalls.Load(), int32(1))
}
