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
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// Gate local provider work so the tests can introduce real concurrent Pool
// operations without changing entries by hand or depending on sleeps.
type updateTestConnection struct {
	mockConnection
	provisionStarted chan struct{}
	provisionGate    <-chan struct{}
	closeStarted     chan struct{}
	closeGate        <-chan struct{}
	provisions       atomic.Int32
	closes           atomic.Int32
}

func (c *updateTestConnection) Provision(ctx api.StreamContext, id string, props map[string]any) error {
	if c.provisions.Add(1) == 1 {
		close(c.provisionStarted)
	}
	if c.provisionGate != nil {
		<-c.provisionGate
	}
	return c.mockConnection.Provision(ctx, id, props)
}

func (c *updateTestConnection) Close(api.StreamContext) error {
	if c.closes.Add(1) == 1 {
		close(c.closeStarted)
	}
	if c.closeGate != nil {
		<-c.closeGate
	}
	return nil
}

func updateFixture(t *testing.T, oldProvision, oldClose, candidateProvision <-chan struct{}) (api.StreamContext, *updateTestConnection, *updateTestConnection) {
	t.Helper()
	require.NoError(t, InitConnectionManager4Test())
	// Registered first, so provider gates and call joins in each test complete
	// before resetting the Manager, whose reset contract is serial.
	t.Cleanup(func() { require.NoError(t, InitConnectionManager4Test()) })
	newConn := func(provision, closing <-chan struct{}) *updateTestConnection {
		return &updateTestConnection{
			provisionStarted: make(chan struct{}), provisionGate: provision,
			closeStarted: make(chan struct{}), closeGate: closing,
		}
	}
	old, candidate := newConn(oldProvision, oldClose), newConn(candidateProvision, nil)
	modules.RegisterConnection("update-old", func(api.StreamContext) modules.Connection { return old })
	modules.RegisterConnection("update-candidate", func(api.StreamContext) modules.Connection { return candidate })
	return mockContext.NewMockContext("update", "op1"), old, candidate
}

func runUpdateCall(t *testing.T, call func() error) <-chan error {
	t.Helper()
	result, finished := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(finished)
		result <- call()
	}()
	t.Cleanup(func() {
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("Pool call did not finish during cleanup")
		}
	})
	return result
}

func awaitUpdateResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Pool call did not finish")
		return nil
	}
}

func releaseUpdateGate(gate chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// Done is only read on the caller-owned wait in Update. Observing it proves
// that Update reached the wait before the test releases Provision or cancels.
type updateWaitContext struct {
	api.StreamContext
	waiting chan struct{}
	once    sync.Once
}

func (c *updateWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.StreamContext.Done()
}

func TestUpdateWaitsForCreation(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		name := "published"
		if cancelWait {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			gate := make(chan struct{})
			release := releaseUpdateGate(gate)
			defer release()
			ctx, old, candidate := updateFixture(t, gate, nil, nil)
			creating := runUpdateCall(t, func() error {
				_, err := CreateNamedConnection(ctx, "update-wait", "update-old", nil)
				return err
			})
			select {
			case <-old.provisionStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("creation did not start")
			}
			waitCtx, cancel := ctx.WithCancel()
			defer cancel()
			observed := &updateWaitContext{StreamContext: waitCtx, waiting: make(chan struct{})}
			updating := runUpdateCall(t, func() error {
				_, err := UpdateConnection(observed, "update-wait", "update-candidate", nil)
				return err
			})
			select {
			case <-observed.waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("Update did not wait for creation")
			}
			if cancelWait {
				cancel()
				require.ErrorIs(t, awaitUpdateResult(t, updating), context.Canceled)
				require.Zero(t, candidate.provisions.Load())
				require.Zero(t, old.closes.Load())
			}
			release()
			require.NoError(t, awaitUpdateResult(t, creating))
			if !cancelWait {
				require.NoError(t, awaitUpdateResult(t, updating))
				require.Equal(t, int32(1), candidate.provisions.Load())
				require.Equal(t, int32(1), old.closes.Load())
			}
		})
	}
}

func TestUpdateRejectsReferencedConnection(t *testing.T) {
	for _, attachDuringProvision := range []bool{false, true} {
		name := "already_referenced"
		if attachDuringProvision {
			name = "attached_during_provision"
		}
		t.Run(name, func(t *testing.T) {
			var gate chan struct{}
			if attachDuringProvision {
				gate = make(chan struct{})
			}
			release := releaseUpdateGate(gate)
			if gate != nil {
				defer release()
			}
			ctx, old, candidate := updateFixture(t, nil, nil, gate)
			_, err := CreateNamedConnection(ctx, "update-ref", "update-old", nil)
			require.NoError(t, err)
			var updating <-chan error
			if attachDuringProvision {
				updating = runUpdateCall(t, func() error {
					_, err := UpdateConnection(ctx, "update-ref", "update-candidate", nil)
					return err
				})
				select {
				case <-candidate.provisionStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("candidate was not provisioned")
				}
			}
			lease, err := FetchConnectionWithOptions(ctx, FetchOptions{
				ConnectionKey: "update-ref", RefID: "rule", Type: "update-old", RequireExisting: true,
			})
			require.NoError(t, err)
			defer lease.Release(ctx)
			if attachDuringProvision {
				release()
				err = awaitUpdateResult(t, updating)
				require.Equal(t, int32(1), candidate.closes.Load(), "release the rejected candidate once")
			} else {
				_, err = UpdateConnection(ctx, "update-ref", "update-candidate", nil)
				require.Zero(t, candidate.provisions.Load())
			}
			require.ErrorContains(t, err, "rule references")
			require.Zero(t, old.closes.Load())
			meta, err := GetConnectionDetail(ctx, "update-ref")
			require.NoError(t, err)
			require.Equal(t, "update-old", meta.Typ)
			conn, err := lease.Wait(ctx)
			require.NoError(t, err)
			require.Same(t, old, conn)
		})
	}
}

func TestUpdateRejectsRemovingConnection(t *testing.T) {
	gate := make(chan struct{})
	release := releaseUpdateGate(gate)
	defer release()
	ctx, old, candidate := updateFixture(t, nil, gate, nil)
	_, err := CreateNamedConnection(ctx, "update-removing", "update-old", nil)
	require.NoError(t, err)
	dropping := runUpdateCall(t, func() error { return DropNameConnection(ctx, "update-removing") })
	select {
	case <-old.closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("old connection did not start closing")
	}
	_, err = UpdateConnection(ctx, "update-removing", "update-candidate", nil)
	require.ErrorIs(t, err, ErrConnectionRemoving)
	require.Zero(t, candidate.provisions.Load())
	release()
	require.NoError(t, awaitUpdateResult(t, dropping))
}

func TestUpdatePersistFailureReleasesCandidate(t *testing.T) {
	ctx, old, candidate := updateFixture(t, nil, nil, nil)
	_, err := CreateNamedConnection(ctx, "update-store", "update-old", nil)
	require.NoError(t, err)
	const point = "github.com/lf-edge/ekuiper/v2/pkg/connection/storeConnectionErr"
	require.NoError(t, failpoint.Enable(point, "return(true)"))
	defer failpoint.Disable(point)
	_, err = UpdateConnection(ctx, "update-store", "update-candidate", nil)
	require.ErrorContains(t, err, "storeConnectionErr")
	require.Equal(t, int32(1), old.closes.Load())
	require.Equal(t, int32(1), candidate.provisions.Load())
	require.Equal(t, int32(1), candidate.closes.Load())
	_, err = GetConnectionDetail(ctx, "update-store")
	require.Error(t, err, "persist failure must not publish a replacement")
}

func TestUpdateWaitsForCreationAfterProvision(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		name := "published"
		if cancelWait {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			candidateGate, otherGate := make(chan struct{}), make(chan struct{})
			releaseCandidate, releaseOther := releaseUpdateGate(candidateGate), releaseUpdateGate(otherGate)
			defer releaseCandidate()
			defer releaseOther()
			ctx, old, candidate := updateFixture(t, nil, nil, candidateGate)
			_, err := CreateNamedConnection(ctx, "update-race", "update-old", nil)
			require.NoError(t, err)
			waitCtx, cancel := ctx.WithCancel()
			defer cancel()
			observed := &updateWaitContext{StreamContext: waitCtx, waiting: make(chan struct{})}
			updating := runUpdateCall(t, func() error {
				_, err := UpdateConnection(observed, "update-race", "update-candidate", nil)
				return err
			})
			select {
			case <-candidate.provisionStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("candidate was not provisioned")
			}
			// Retire the old generation and start a real, blocked creation round
			// on the key while Update is still preparing its candidate.
			require.NoError(t, DropNameConnection(ctx, "update-race"))
			other := &updateTestConnection{
				provisionStarted: make(chan struct{}), provisionGate: otherGate,
				closeStarted: make(chan struct{}),
			}
			modules.RegisterConnection("update-other", func(api.StreamContext) modules.Connection { return other })
			creating := runUpdateCall(t, func() error {
				_, err := CreateNamedConnection(ctx, "update-race", "update-other", nil)
				return err
			})
			select {
			case <-other.provisionStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("concurrent creation did not start")
			}
			releaseCandidate()
			select {
			case <-observed.waiting:
			case err := <-updating:
				t.Fatalf("Update failed instead of waiting for concurrent creation: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("Update did not wait for concurrent creation")
			}
			require.Zero(t, candidate.closes.Load(), "keep ownership of the prepared candidate while waiting")
			if cancelWait {
				cancel()
				require.ErrorIs(t, awaitUpdateResult(t, updating), context.Canceled)
				require.Equal(t, int32(1), candidate.closes.Load())
			}
			releaseOther()
			require.NoError(t, awaitUpdateResult(t, creating))
			if !cancelWait {
				require.NoError(t, awaitUpdateResult(t, updating))
				require.Equal(t, int32(1), other.closes.Load())
				require.Zero(t, candidate.closes.Load())
			}
			require.Equal(t, int32(1), candidate.provisions.Load(), "do not repeat static provisioning")
			require.Equal(t, int32(1), old.closes.Load())
		})
	}
}
