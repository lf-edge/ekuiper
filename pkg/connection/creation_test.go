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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// singleFlightStarted/singleFlightGo pile fetchers up: every goroutine
// counts in and parks before the start gun, and the release only fires
// after the creator is observed blocked inside Provision. What this
// proves deterministically is structural: reservation precedes the
// Provision block under one critical section, so at most one Provision
// can ever run per key (countProvCalls == 1 regardless of scheduling).
// It does not prove every goroutine took the waiter path — a straggler
// woken after publish legitimately takes the ready fast path.
var (
	singleFlightStarted atomic.Int32
	singleFlightGo      = make(chan struct{})
)

func awaitSingleFlightArmed(t *testing.T, total int32) {
	t.Helper()
	require.Eventually(t, func() bool {
		return singleFlightStarted.Load() == total
	}, 5*time.Second, 5*time.Millisecond, "all fetchers must launch")
}

func awaitSingleProvision(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		return countProvCalls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond, "exactly one creator must enter Provision")
}

// TestConcurrentFetchSingleFlight proves same-key single-flight: N
// concurrent fetchers trigger exactly one static Provision and share one
// Meta, each holding its own ref.
func TestConcurrentFetchSingleFlight(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	countProvCalls.Store(0)
	singleFlightStarted.Store(0)
	singleFlightGo = make(chan struct{})
	// Drain stale release tokens; the test controls exactly one round.
	select {
	case <-countProvRelease:
	default:
	}
	ctx := mockContext.NewMockContext("race", "op1")

	const fetchers = 8
	var wg sync.WaitGroup
	cws := make([]*ConnectionLease, fetchers)
	errs := make([]error, fetchers)
	for i := 0; i < fetchers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			singleFlightStarted.Add(1)
			<-singleFlightGo
			cw, err := FetchConnectionWithOptions(ctx, FetchOptions{
				ConnectionKey: "single-flight",
				RefID:         fmt.Sprintf("ref-%d", i),
				Type:          "countprov",
			})
			cws[i], errs[i] = cw, err
		}(i)
	}
	awaitSingleFlightArmed(t, fetchers)
	close(singleFlightGo)
	// The creator is now blocked inside Provision with the reservation
	// held. Most fetchers therefore take the waiter path, but that is
	// not asserted per-goroutine: a straggler woken after publish
	// legitimately takes the ready fast path. What is asserted below
	// holds regardless of scheduling: exactly one Provision ran and
	// every fetcher shares its handle.
	awaitSingleProvision(t)
	countProvRelease <- struct{}{}
	wg.Wait()

	for i := 1; i < fetchers; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, cws[i])
		require.NotSame(t, cws[0], cws[i], "each fetcher mints its own lease")
		require.Same(t, cws[0].cw, cws[i].cw, "all fetchers must share one handle")
	}
	require.Equal(t, int32(1), countProvCalls.Load(), "exactly one Provision per key")
	require.Equal(t, fetchers, getConnectionRef("single-flight"))

	for i := 0; i < fetchers; i++ {
		require.NoError(t, cws[i].Release(ctx))
	}
	_, ok := globalConnectionManager.connectionPool["single-flight"]
	require.False(t, ok)
}

// TestConcurrentNamedCreateSecondGetsAlreadyExists proves the waiter is
// not promoted to creator: the loser observes already-exists, never a
// second success.
func TestConcurrentNamedCreateSecondGetsAlreadyExists(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	countProvCalls.Store(0)
	singleFlightStarted.Store(0)
	singleFlightGo = make(chan struct{})
	select {
	case <-countProvRelease:
	default:
	}
	ctx := mockContext.NewMockContext("race", "op1")

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			singleFlightStarted.Add(1)
			<-singleFlightGo
			_, err := CreateNamedConnection(ctx, "cc-race", "countprov", nil)
			results[i] = err
		}(i)
	}
	awaitSingleFlightArmed(t, 2)
	close(singleFlightGo)
	awaitSingleProvision(t)
	countProvRelease <- struct{}{}
	wg.Wait()

	succeeded := 0
	for _, err := range results {
		if err == nil {
			succeeded++
		} else {
			require.ErrorContains(t, err, "already been created")
		}
	}
	require.Equal(t, 1, succeeded, "exactly one creator wins")
	require.Equal(t, int32(1), countProvCalls.Load())
	require.NoError(t, DropNameConnection(ctx, "cc-race"))
}

// TestCreatingWaiterCallerCancel proves a waiter parked on a creating
// entry observes caller cancellation: with the creator blocked inside
// Provision (reservation held, so the lookup deterministically hits the
// waiter path), a Fetch with an already-canceled context returns
// ctx.Err() without disturbing the round — Provision count stays 1.
func TestCreatingWaiterCallerCancel(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	failProvCalls.Store(0)
	select {
	case <-failProvRelease:
	default:
	}
	ctx := mockContext.NewMockContext("race", "op1")

	creatorDone := make(chan error, 1)
	go func() {
		_, err := FetchConnectionWithOptions(ctx, FetchOptions{
			ConnectionKey: "prov-cancel",
			RefID:         "creator",
			Type:          "failprov",
		})
		creatorDone <- err
	}()
	// The creator is now blocked inside Provision with the reservation
	// held, so the Fetch below must take the waiter path.
	require.Eventually(t, func() bool {
		return failProvCalls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond, "exactly one creator must enter Provision")

	canceled, cancel := ctx.WithCancel()
	cancel()
	_, err := FetchConnectionWithOptions(canceled, FetchOptions{
		ConnectionKey: "prov-cancel",
		RefID:         "waiter",
		Type:          "failprov",
	})
	// Must come from the waiter path observing cancellation, not from
	// the creation round itself.
	require.Equal(t, canceled.Err(), err)
	require.Equal(t, int32(1), failProvCalls.Load())

	failProvRelease <- struct{}{}
	require.ErrorContains(t, <-creatorDone, "failprov: static provision failure")
}

// TestCreationFailureCleaned proves a failed round removes its
// reservation: a single creator hits a blocking static failure, observes
// the error, and a later Fetch starts a fresh round instead of hanging
// on a stuck creating entry.
func TestCreationFailureCleaned(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	failProvCalls.Store(0)
	select {
	case <-failProvRelease:
	default:
	}
	ctx := mockContext.NewMockContext("race", "op1")

	creatorDone := make(chan error, 1)
	go func() {
		_, err := FetchConnectionWithOptions(ctx, FetchOptions{
			ConnectionKey: "prov-fail",
			RefID:         "creator",
			Type:          "failprov",
		})
		creatorDone <- err
	}()
	require.Eventually(t, func() bool {
		return failProvCalls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond, "exactly one creator must enter Provision")
	failProvRelease <- struct{}{}
	require.ErrorContains(t, <-creatorDone, "failprov: static provision failure")
	require.Equal(t, int32(1), failProvCalls.Load())
	_, ok := globalConnectionManager.connectionPool["prov-fail"]
	require.False(t, ok, "failed reservation must be removed")

	// A later call starts a fresh round (and fails the same static way).
	failProvRelease <- struct{}{}
	_, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "prov-fail",
		RefID:         "r-new",
		Type:          "failprov",
	})
	require.ErrorContains(t, err, "failprov: static provision failure")
}
