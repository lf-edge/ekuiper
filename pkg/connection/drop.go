// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

// reserveDropLocked marks a ready key as removing and parks late
// arrivals on a fresh removed channel. Caller holds m's lock and keeps
// using m afterwards. It performs no I/O: the KV delete runs outside
// the lock (lock invariant), and complete/abortDropLocked settles the
// reservation afterwards. The removing window behaves exactly as
// before, only slightly longer — it now spans the KV delete.
func reserveDropLocked(m *Manager, selId string) (meta *Meta, err error) {
	e, ok := m.connectionPool[selId]
	if !ok {
		return nil, nil
	}
	if e.state != entryReady || e.meta == nil {
		return nil, ErrConnectionRemoving
	}
	meta = e.meta
	isInternal, err := isInternalConnection(m, selId)
	if err != nil {
		return nil, err
	}
	if isInternal {
		return nil, fmt.Errorf("internal connection %v can't be edit", selId)
	}
	if meta.GetRefCount() > 0 {
		return nil, fmt.Errorf("connection %s can't be dropped due to rule references %v", selId, meta.GetRefNames())
	}
	e.state = entryRemoving
	e.removed = make(chan struct{})
	return meta, nil
}

// abortDropLocked rolls a failed reservation back to ready. Caller
// holds m's lock. The removed channel is closed (never reused) so
// waiters parked during the reservation wake, re-resolve, and observe
// the restored ready entry.
func abortDropLocked(m *Manager, selId string) {
	if e, ok := m.connectionPool[selId]; ok && e.state == entryRemoving {
		e.state = entryReady
		if e.removed != nil {
			close(e.removed)
			e.removed = nil
		}
	}
}

// completeDropLocked hands out the stop ownership for a reservation
// whose KV delete succeeded. Caller holds m's lock; the stop itself
// still runs outside it via finishStop.
func completeDropLocked(m *Manager, selId string) (meta *Meta, stop func(api.StreamContext)) {
	if e, ok := m.connectionPool[selId]; ok && e.state == entryRemoving && e.meta != nil {
		return e.meta, e.meta.stop
	}
	return nil, nil
}

func finishStop(m *Manager, key string, stop func(api.StreamContext), ctx api.StreamContext) {
	stop(ctx)
	m.Lock()
	// Delete only our own entry: a re-init swaps the whole manager, and
	// a concurrent round cannot reuse the key while it is removing.
	// Closing removed wakes Fetch waiters so they retry on the key.
	if e, ok := m.connectionPool[key]; ok && e.state == entryRemoving {
		delete(m.connectionPool, key)
		if e.removed != nil {
			close(e.removed)
		}
	}
	m.Unlock()
}

// dropPlan is the locked decision for Drop/Update: the lock is held only
// inside planDrop/planUpdateDrop (defer-unlocked). Execution (KV delete,
// waiting, stopping) always happens outside the lock.
//   - reserved != nil: the key is marked removing; caller must run the KV
//     delete outside the lock, then settle the reservation under the lock
//     (completeDropLocked on success, abortDropLocked on failure) and
//     finishStop the handed-out stop outside the lock.
//   - wait != nil: a creation round owns the key; caller waits outside
//     the lock and retries (bounded: Provision + persist only, no Dial).
//   - err != nil: terminal error.
//   - otherwise (all nil): key is missing/removing; Drop reports success,
//     Update reports its own error.
type dropPlan struct {
	wait     <-chan struct{}
	err      error
	reserved *Meta
}

// planDrop decides one Drop step under a single critical section.
func (m *Manager) planDrop(ctx api.StreamContext, selId string) dropPlan {
	m.Lock()
	defer m.Unlock()
	meta, err := reserveDropLocked(m, selId)
	if err == nil {
		// Missing key (meta == nil) stays idempotent success; a
		// reservation hands the KV phase to the caller.
		return dropPlan{reserved: meta}
	}
	if !errors.Is(err, ErrConnectionRemoving) {
		return dropPlan{err: err}
	}
	// ErrConnectionRemoving covers both creating and removing.
	// Removing stays idempotent (nil). Creating must not report
	// success: wait out the round and retry, mirroring the old
	// global-lock behavior where Drop blocked until Create published.
	e, ok := m.connectionPool[selId]
	if !ok {
		// Lost a race with a failed round's cleanup.
		return dropPlan{}
	}
	if e.state == entryCreating {
		return dropPlan{wait: e.ready}
	}
	return dropPlan{}
}

// planUpdateDrop decides the validate + reserve handoff for Update under
// one critical section, so no state change can slip between validation
// and the drop. Creating yields wait (retry whole Update); removing
// yields err. The KV delete runs outside the lock in the caller.
func (m *Manager) planUpdateDrop(ctx api.StreamContext, id string) dropPlan {
	m.Lock()
	defer m.Unlock()
	isInternal, err := isInternalConnection(m, id)
	if err != nil {
		if errors.Is(err, ErrConnectionRemoving) {
			if e, ok := m.connectionPool[id]; ok && e.state == entryCreating {
				return dropPlan{wait: e.ready}
			}
		}
		return dropPlan{err: err}
	}
	if isInternal {
		return dropPlan{err: fmt.Errorf("internal connection %v can't be edit", id)}
	}
	// Validation passed under this hold, so the key is ready; the
	// reservation cannot observe a mid-transition entry. The creating
	// branch below is defensive only.
	meta, err := reserveDropLocked(m, id)
	if err != nil {
		if errors.Is(err, ErrConnectionRemoving) {
			if e, ok := m.connectionPool[id]; ok && e.state == entryCreating {
				return dropPlan{wait: e.ready}
			}
		}
		return dropPlan{err: err}
	}
	if meta == nil {
		// Unreachable: validation above already rejected missing
		// keys. Defensive so a bypassed check can never silently
		// turn Update into an upsert.
		return dropPlan{err: fmt.Errorf("connection %s not existed", id)}
	}
	return dropPlan{reserved: meta}
}

// waitForRound waits out a creation round outside the Manager lock.
func waitForRound(ctx api.StreamContext, m *Manager, wait <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wait:
		if globalConnectionManager.Load() != m {
			return ErrConnectionClosed
		}
		return nil
	}
}

func DropNameConnection(ctx api.StreamContext, selId string) error {
	if selId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	m := globalConnectionManager.Load()
	for {
		plan := m.planDrop(ctx, selId)
		if plan.err != nil {
			return plan.err
		}
		if plan.wait != nil {
			if err := waitForRound(ctx, m, plan.wait); err != nil {
				return err
			}
			continue
		}
		if plan.reserved == nil {
			return nil
		}
		// KV delete runs outside the Manager lock (lock invariant).
		// On failure the reservation is rolled back; parked waiters
		// wake and re-resolve against the restored ready entry.
		if err := dropConnectionStore(plan.reserved.Typ, selId); err != nil {
			m.Lock()
			abortDropLocked(m, selId)
			m.Unlock()
			return fmt.Errorf("drop connection %s failed, err:%v", selId, err)
		}
		m.Lock()
		_, stop := completeDropLocked(m, selId)
		m.Unlock()
		if stop != nil {
			finishStop(m, selId, stop, ctx)
		}
		return nil
	}
}

func UpdateConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	// Validation + reservation run under one hold of a single captured
	// manager; the KV delete and creation re-lock internally. A
	// concurrent creation round is waited out and retried, consistent
	// with DropNameConnection; a key already owned by teardown stays a
	// hard error.
	for {
		m := globalConnectionManager.Load()
		plan := m.planUpdateDrop(ctx, id)
		if plan.wait != nil {
			if err := waitForRound(ctx, m, plan.wait); err != nil {
				return nil, err
			}
			continue
		}
		if plan.err != nil {
			return nil, plan.err
		}
		if err := dropConnectionStore(plan.reserved.Typ, id); err != nil {
			m.Lock()
			abortDropLocked(m, id)
			m.Unlock()
			return nil, fmt.Errorf("drop connection %s failed, err:%v", id, err)
		}
		m.Lock()
		_, stop := completeDropLocked(m, id)
		m.Unlock()
		if stop != nil {
			finishStop(m, id, stop, ctx)
		}
		return createNamedConnection(ctx, id, typ, props)
	}
}

func isInternalConnection(m *Manager, id string) (bool, error) {
	meta, err := readyMeta(m, id)
	if err != nil {
		return false, err
	}
	if meta == nil {
		return false, fmt.Errorf("connection %s not existed", id)
	}
	return !meta.Named, nil
}

func DetachConnection(ctx api.StreamContext, conId string) error {
	return DetachConnectionByRef(ctx, conId, extractRefId(ctx))
}

// DetachConnectionByRef detaches a connection using the reference ID supplied
// to FetchConnection.
func DetachConnectionByRef(ctx api.StreamContext, conId, refId string) error {
	if conId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	if refId == "" {
		return fmt.Errorf("connection reference id should be defined")
	}
	m := globalConnectionManager.Load()
	m.Lock()
	_, stop, err := detachLocked(m, ctx, conId, refId)
	m.Unlock()
	if err != nil {
		return err
	}
	if stop != nil {
		finishStop(m, conId, stop, ctx)
	}
	return nil
}

// readyMeta resolves the published Meta for key. Callers must hold m's
// lock and must keep using the same m afterwards. A missing key yields
// (nil, nil), preserving idempotent Drop/Detach. A key mid-transition
// (creating/removing) yields ErrConnectionRemoving so callers retry
// instead of racing the round.
func readyMeta(m *Manager, key string) (*Meta, error) {
	e, ok := m.connectionPool[key]
	if !ok {
		return nil, nil
	}
	if e.state != entryReady || e.meta == nil {
		return nil, ErrConnectionRemoving
	}
	return e.meta, nil
}

func getConnectionRef(id string) int {
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	meta, err := readyMeta(m, id)
	if err != nil || meta == nil {
		return 0
	}
	return meta.GetRefCount()
}

func attachConnection(conId string, refId string, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	if conId == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	// Test/compat helper: same atomic attach as the fast path, just
	// resolved by key instead of by entry. No defer: the initial
	// delivery must run after the Manager lock is released (lock
	// invariant).
	m := globalConnectionManager.Load()
	m.Lock()
	meta, err := readyMeta(m, conId)
	if err != nil {
		m.Unlock()
		return nil, err
	}
	if meta == nil {
		m.Unlock()
		return nil, fmt.Errorf("connection %s not existed", conId)
	}
	meta.AddRef(refId, sc)
	if conId != refId {
		conf.Log.Infof("action=attach_connection_ref connId=%s type=%s connectionKey=%s refId=%s refCount=%d", conId, meta.Typ, conId, refId, meta.GetRefCount())
	}
	m.Unlock()
	meta.deliverInitial(refId, sc)
	return meta.cw, nil
}

// detachLocked removes one consumer reference. The caller must hold m's
// lock and keep using m afterwards. If an anonymous Meta reaches zero
// refs it flips to removing and hands out its stop ownership; the caller
// must run the stop outside the lock and delete the entry after it
// completes. Refs on an already-removing Meta are released best-effort
// so repeated Close stays nil and never triggers a second stop.
func detachLocked(m *Manager, ctx api.StreamContext, conId, refId string) (meta *Meta, stop func(api.StreamContext), err error) {
	e, ok := m.connectionPool[conId]
	if !ok {
		conf.Log.Infof("detachConnection not found:%v", conId)
		return nil, nil, nil
	}
	if e.state != entryReady || e.meta == nil {
		if e.state == entryRemoving && e.meta != nil {
			e.meta.DeRef(refId)
			return nil, nil, nil
		}
		return nil, nil, ErrConnectionRemoving
	}
	meta = e.meta
	// Only an actually-removed ref can drive teardown: a stray detach
	// for an unknown refId must neither decrement (DeRef already
	// no-ops) nor retire a Meta nobody attached to yet.
	removed := meta.DeRef(refId)
	conf.Log.Infof("detachConnection remove conn:%v,ref:%v", conId, refId)
	if conId != refId {
		conf.Log.Infof("action=detach_connection_ref connId=%s type=%s connectionKey=%s rule=%s op=%s refId=%s refCount=%d", conId, meta.Typ, conId, ctx.GetRuleId(), ctx.GetOpId(), refId, meta.GetRefCount())
	}
	if removed && !meta.Named && meta.GetRefCount() == 0 {
		if conId != refId {
			conf.Log.Infof("action=close_connection connId=%s type=%s connectionKey=%s rule=%s op=%s reason=zero_ref", conId, meta.Typ, conId, ctx.GetRuleId(), ctx.GetOpId())
		}
		e.state = entryRemoving
		e.removed = make(chan struct{})
		return meta, meta.stop, nil
	}
	return nil, nil, nil
}

func dropConnectionStore(plugin, id string) error {
	err := conf.DropCfgKeyFromStorage("connections", plugin, id)
	failpoint.Inject("dropConnectionStoreErr", func() {
		err = errors.New("dropConnectionStoreErr")
	})
	return err
}
