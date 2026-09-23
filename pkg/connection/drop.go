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

// dropNameConnection validates and drops one ready key under a single
// Manager critical section. Caller holds m's lock and keeps using m
// afterwards. Validation, the single-key KV delete and the
// ready→removing flip are atomic: on KV failure no pool state changes,
// so Fetch waiters blocked on the Manager lock during the delete
// subsequently observe the original ready entry. The stop itself still
// runs outside the lock via finishStop.
func dropNameConnection(m *Manager, ctx api.StreamContext, selId string) (meta *Meta, stop func(api.StreamContext), err error) {
	e, ok := m.connectionPool[selId]
	if !ok {
		return nil, nil, nil
	}
	if e.state != entryReady || e.meta == nil {
		return nil, nil, ErrConnectionRemoving
	}
	meta = e.meta
	isInternal, err := isInternalConnection(m, selId)
	if err != nil {
		return nil, nil, err
	}
	if isInternal {
		return nil, nil, fmt.Errorf("internal connection %v can't be edit", selId)
	}
	if meta.GetRefCount() > 0 {
		return nil, nil, fmt.Errorf("connection %s can't be dropped due to rule references %v", selId, meta.GetRefNames())
	}
	if err := dropConnectionStore(meta.Typ, selId); err != nil {
		return nil, nil, fmt.Errorf("drop connection %s failed, err:%v", selId, err)
	}
	e.state = entryRemoving
	e.removed = make(chan struct{})
	return meta, meta.stop, nil
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
// inside planDrop/planUpdateDrop (defer-unlocked). Execution (waiting,
// stopping) always happens outside the lock.
//   - stop != nil: caller must finishStop outside the lock, then done.
//   - wait != nil: a creation round owns the key; caller waits outside
//     the lock and retries (bounded: Provision + persist only, no Dial).
//   - err != nil: terminal error.
//   - otherwise (stop == nil, wait == nil, err == nil): key is
//     missing/removing; Drop reports success, Update reports its own error.
type dropPlan struct {
	stop func(api.StreamContext)
	wait <-chan struct{}
	err  error
}

// planDrop decides one Drop step under a single critical section.
func (m *Manager) planDrop(ctx api.StreamContext, selId string) dropPlan {
	m.Lock()
	defer m.Unlock()
	_, stop, err := dropNameConnection(m, ctx, selId)
	if err == nil {
		return dropPlan{stop: stop}
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

// planUpdateDrop decides the validate + drop handoff for Update under one
// critical section, so no state change can slip between validation and the
// drop. Creating yields wait (retry whole Update); removing yields err.
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
	// Drop holds the lock only for validation and stop handoff; the
	// previous generation is fully stopped and removed before creating
	// the replacement, preserving drop-then-create ordering. Creation
	// re-locks internally.
	_, stop, err := dropNameConnection(m, ctx, id)
	if err != nil {
		if errors.Is(err, ErrConnectionRemoving) {
			if e, ok := m.connectionPool[id]; ok && e.state == entryCreating {
				return dropPlan{wait: e.ready}
			}
		}
		return dropPlan{err: err}
	}
	return dropPlan{stop: stop}
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
		if plan.stop != nil {
			finishStop(m, selId, plan.stop, ctx)
		}
		return nil
	}
}

func UpdateConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	// Validation + drop run under one hold of a single captured manager;
	// creation re-locks internally. A concurrent creation round is
	// waited out and retried, consistent with DropNameConnection; a key
	// already owned by teardown stays a hard error.
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
		if plan.stop != nil {
			finishStop(m, id, plan.stop, ctx)
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
