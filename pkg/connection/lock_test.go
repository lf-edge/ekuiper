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
	require.Equal(t, int32(0), calls.Load())

	m.AddRef("live", sc)
	// Snapshot content matches current state (connecting, no error).
	var gotS, gotE string
	m.deliverInitial("live", func(s, e string) { gotS, gotE = s, e })
	s, e := m.GetStatus()
	require.Equal(t, s, gotS)
	require.Equal(t, e, gotE)
	require.Equal(t, api.ConnectionConnecting, gotS)
}

// TestDropReservationAbortRestoresReady drives the KV-failure branch
// white-box (no failpoint needed): reserve parks the key as
// removing with a live removed channel; abort flips it back to
// ready and wakes parked waiters, which re-resolve against the
// restored entry. A subsequent real Drop completes the cycle.
func TestDropReservationAbortRestoresReady(t *testing.T) {
	ctx := mockContext.NewMockContext("dropabort", "op1")
	_, err := CreateNamedConnection(ctx, "drop-abort", "mock", map[string]any{})
	require.NoError(t, err)

	m := globalConnectionManager.Load()
	m.Lock()
	meta, err := reserveDropLocked(m, "drop-abort")
	require.NoError(t, err)
	require.NotNil(t, meta)
	e := m.connectionPool["drop-abort"]
	require.Equal(t, entryRemoving, e.state)
	require.NotNil(t, e.removed)
	removed := e.removed
	m.Unlock()

	woken := make(chan struct{})
	go func() { <-removed; close(woken) }()

	m.Lock()
	abortDropLocked(m, "drop-abort")
	e = m.connectionPool["drop-abort"]
	require.Equal(t, entryReady, e.state)
	require.Nil(t, e.removed)
	m.Unlock()

	select {
	case <-woken:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter parked on the reservation was not woken by abort")
	}

	// The restored entry is fully usable again.
	cw, err := attachConnection("drop-abort", "ref1", nil)
	require.NoError(t, err)
	require.NotNil(t, cw)
	require.NoError(t, DetachConnectionByRef(ctx, "drop-abort", "ref1"))
	require.NoError(t, DropNameConnection(ctx, "drop-abort"))
}

// TestAttemptStreamContextIsBoundedServerScope pins the attempt-scope
// rule: every provider attempt runs under an explicitly bounded,
// server-owned scope — never a rule/request lifetime.
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
