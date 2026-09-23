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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// TestSlowInitialCallbackDoesNotBlockPool proves the A2 lock
// invariant on the fetch path: registration happens under the
// Manager lock, but the initial snapshot delivery runs after
// unlock. A consumer that blocks inside its status handler stalls
// only its own fetch; a concurrent fetch on the same key completes.
func TestSlowInitialCallbackDoesNotBlockPool(t *testing.T) {
	ctx := mockContext.NewMockContext("slowcb", "op1")
	// Seed a ready anonymous Meta (no handler, no delivery).
	_, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "slow-cb",
		RefID:         "seed",
		Type:          "mock",
	})
	require.NoError(t, err)

	release := make(chan struct{})
	var slowEntered atomic.Bool
	slow := func(string, string) {
		slowEntered.Store(true)
		<-release
	}
	slowDone := make(chan error, 1)
	go func() {
		_, err := FetchConnectionWithOptions(ctx, FetchOptions{
			ConnectionKey: "slow-cb",
			RefID:         "slow",
			Type:          "mock",
			StatusHandler: slow,
		})
		slowDone <- err
	}()

	// Wait until the slow delivery is parked inside the handler.
	require.Eventually(t, slowEntered.Load, 2*time.Second, time.Millisecond)

	// A second fetch on the same key must complete while the first
	// delivery is still blocked: nothing under the Manager lock
	// waits for consumer callbacks.
	fastDone := make(chan error, 1)
	go func() {
		_, err := FetchConnectionWithOptions(ctx, FetchOptions{
			ConnectionKey: "slow-cb",
			RefID:         "fast",
			Type:          "mock",
		})
		fastDone <- err
	}()
	select {
	case err := <-fastDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent fetch blocked behind a slow status handler")
	}

	close(release)
	select {
	case err := <-slowDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("slow fetch did not return after handler release")
	}
}

// TestDeliverInitialSkipsDetached pins the detach-wins rule: a ref
// removed between registration and delivery gets no initial call,
// while a live ref gets exactly the current snapshot.
func TestDeliverInitialSkipsDetached(t *testing.T) {
	m := newStateMeta()
	var calls atomic.Int32
	sc := func(string, string) { calls.Add(1) }

	m.AddRef("gone", sc)
	require.True(t, m.DeRef("gone"))
	m.deliverInitial("gone", sc)
	m.drainEvents()
	require.Equal(t, int32(0), calls.Load())

	m.AddRef("live", sc)
	// Snapshot content matches current state (connecting, no error).
	var gotS, gotE string
	m.deliverInitial("live", func(s, e string) { gotS, gotE = s, e })
	m.drainEvents()
	s, e := m.GetStatus()
	require.Equal(t, s, gotS)
	require.Equal(t, e, gotE)
	require.Equal(t, api.ConnectionConnecting, gotS)
}

// TestAttemptStreamContextIsBoundedServerScope pins the Ping/Recover
// attempt-scope rule: those scopes are explicitly deadline-bounded and
// server-owned — never a rule/request lifetime. (Dial is intentionally
// excluded: it waits for initial usability under plain lifecycle
// cancellation, see the modules.Connection attempt contract.)
func TestAttemptStreamContextIsBoundedServerScope(t *testing.T) {
	actx, cancel := attemptStreamContext(context.Background(), 50*time.Millisecond)
	defer cancel()
	dl, ok := actx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(50*time.Millisecond), dl, 5*time.Second)
	require.Equal(t, "", actx.GetRuleId())
	select {
	case <-actx.Done():
		t.Fatal("attempt scope died before its deadline")
	case <-time.After(5 * time.Millisecond):
	}
}

type statusEvent struct {
	status string
	errMsg string
}

// TestEventOrderingPreservesEveryTransition pins the eventMu contract
// for sequential transitions: each NotifyStatus is delivered exactly
// once, in order, with its own exact event snapshot. Transitions are
// never coalesced: a disconnected event followed by connected must
// both reach the consumer (metrics record per-transition side
// effects such as lastDisconnectTime, so swallowing is data loss).
func TestEventOrderingPreservesEveryTransition(t *testing.T) {
	m := newStateMeta()
	var mu sync.Mutex
	var got []statusEvent
	m.AddRef("r1", func(s, e string) {
		mu.Lock()
		got = append(got, statusEvent{s, e})
		mu.Unlock()
	})
	m.NotifyStatus(api.ConnectionDisconnected, "down")
	m.NotifyStatus(api.ConnectionConnected, "")
	// Delivery is async; the barrier flush makes the assertion
	// deterministic without timing.
	m.drainEvents()
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []statusEvent{
		{api.ConnectionDisconnected, "down"},
		{api.ConnectionConnected, ""},
	}, got)
}

// TestInitialDeliveryJoinsEventSerialization pins the gated
// interleaving of deliverInitial with a transition: the dispatcher is
// parked inside a slow consumer, the initial delivery enqueues behind
// it, and neither delivery inverts nor swallows. The new consumer
// observes exactly the serialized latest state. A same-state
// duplicate (connected, connected) would be allowed here by design;
// this scenario asserts the exact single each, which is the
// deterministic outcome of this gating.
func TestInitialDeliveryJoinsEventSerialization(t *testing.T) {
	m := newStateMeta()
	var mu sync.Mutex
	var oldGot, newGot []statusEvent
	entered := make(chan struct{})
	var enterOnce sync.Once
	release := make(chan struct{})
	m.AddRef("old", func(s, e string) {
		mu.Lock()
		oldGot = append(oldGot, statusEvent{s, e})
		mu.Unlock()
		enterOnce.Do(func() { close(entered) })
		<-release
	})

	// Phase 1: the transition enqueues and returns immediately; the
	// dispatcher parks inside the slow consumer.
	m.NotifyStatus(api.ConnectionDisconnected, "down")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not reach the slow consumer")
	}

	// Register + enqueue while the dispatcher is still parked: the
	// initial event lands behind the transition deterministically.
	newHandler := func(s, e string) {
		mu.Lock()
		newGot = append(newGot, statusEvent{s, e})
		mu.Unlock()
	}
	m.AddRef("new", newHandler)
	m.deliverInitial("new", newHandler)

	close(release)
	// The barrier flush makes the assertions deterministic: every
	// event enqueued so far is delivered before it returns.
	m.drainEvents()

	mu.Lock()
	require.Equal(t, []statusEvent{{api.ConnectionDisconnected, "down"}}, oldGot)
	require.Equal(t, []statusEvent{{api.ConnectionDisconnected, "down"}}, newGot)
	mu.Unlock()

	// Phase 2: the next transition reaches both consumers exactly once.
	m.NotifyStatus(api.ConnectionConnected, "")
	m.drainEvents()
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []statusEvent{
		{api.ConnectionDisconnected, "down"},
		{api.ConnectionConnected, ""},
	}, oldGot)
	require.Equal(t, []statusEvent{
		{api.ConnectionDisconnected, "down"},
		{api.ConnectionConnected, ""},
	}, newGot)
}
