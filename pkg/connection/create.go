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
	"maps"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// runCreation executes one creation round for a reserved entry: heavy
// static work (provider lookup, Provision, optional named persist) runs
// outside the Manager lock, then construct/publish/attach happen
// atomically under one Manager critical section.
//
//   - persist, when non-nil, runs between Provision and publish (named
//     creation only). A persist failure publishes only an error: no Meta,
//     no worker — but the provisioned candidate is Closed first, since a
//     nil return from Provision transfers ownership to the Pool.
//   - attachRef, when non-empty, attaches the creator ref as part of the
//     same critical section, so a successfully published anonymous Meta
//     is born referenced. Named creation passes "" — a zero-ref named
//     Meta is a legitimate idle resource, never an orphan.
//
// Note the worker goroutine is spawned inside the critical section via
// newConnWrapper, i.e. before ready closes. Its first Dial may therefore
// run while the entry is still unpublished; that is harmless because
// every fallible static step already completed and the entry is not
// visible to anyone until ready closes. There is no orphan-worker window.
//
// Failure is atomic: no published Meta, no worker-started-without-Meta,
// no lifecycle. The reservation is removed so a later call may start a
// fresh round, while current waiters still observe this round's error
// through the entry.
func (m *Manager) runCreation(e *poolEntry, key, typ string, props map[string]any, named bool, persist func() error, attachRef string, sc api.StatusChangeHandler) (*Meta, error) {
	createCtx := serverStreamContext(m.ctx)
	conn, err := provisionConnection(createCtx, key, typ, props)
	if err == nil && persist != nil {
		err = persist()
	}
	// Provider Close must never run under the Manager lock. The publish
	// decision runs locked (with defer); a failed candidate is released
	// outside it. After the registered-flag fix (WS/SSE) a
	// provisioned-but-never-dialed Close is a no-op for endpoint
	// providers and a local dispose for the rest, so a fresh round
	// starting right after publishing the failure cannot be disturbed
	// by the cleanup still in flight.
	meta, toClose, perr := m.publishCreation(e, key, typ, props, named, conn, err, attachRef, sc)
	if toClose != nil {
		// The candidate belongs to the Pool from a nil Provision
		// return on; a failed later step must release it instead
		// of dropping the reference.
		toClose.Close(serverStreamContext(m.ctx))
	}
	if perr == nil && attachRef != "" {
		// Creator delivery runs outside the Manager lock (lock
		// invariant): publishCreation only registered the ref.
		meta.deliverInitial(attachRef, sc)
	}
	return meta, perr
}

// publishCreation publishes one creation round outcome atomically under a
// single Manager critical section (defer-unlocked). It never runs provider
// code: a candidate needing release is returned as toClose for the caller
// to dispose outside the lock.
func (m *Manager) publishCreation(e *poolEntry, key, typ string, props map[string]any, named bool, conn modules.Connection, perr error, attachRef string, sc api.StatusChangeHandler) (meta *Meta, toClose modules.Connection, err error) {
	m.Lock()
	defer m.Unlock()
	// Verify our reservation survived. Init/reset is contractually
	// serialized with mutations, so the only way out here is a stale
	// entry from a misuse path — never silently publish over it.
	cur, ok := m.connectionPool[key]
	if !ok || cur != e || cur.state != entryCreating {
		return nil, conn, ErrConnectionClosed
	}
	if perr != nil {
		e.err = perr
		close(e.ready)
		delete(m.connectionPool, key)
		return nil, conn, perr
	}
	meta = newMeta(m, key, typ, props, named)
	meta.pendingConn = conn
	meta.cw = newConnWrapper(meta)
	if attachRef != "" {
		meta.AddRef(attachRef, sc)
	}
	e.meta = meta
	e.state = entryReady
	close(e.ready)
	return meta, nil, nil
}

// attachToMeta stores one consumer reference and returns the stable
// handle. Callers must hold the Manager lock so the attach is atomic
// with zero-ref teardown decisions. Registration is structural only
// (no callback); the caller delivers the initial snapshot via
// deliverInitial after unlocking. The lifecycle guard fails the
// attach instead of parking a ref on a dying Meta.
func attachToMeta(meta *Meta, refId string, sc api.StatusChangeHandler) (*connWrapper, error) {
	select {
	case <-meta.lifecycleCtx.Done():
		return nil, ErrConnectionClosed
	default:
	}
	meta.AddRef(refId, sc)
	return meta.cw, nil
}

// checkCompatible rejects sharing one logical connection between
// incompatible definitions. The comparison is strictly local: identity
// material already normalized by the caller, no plugin calls, no I/O.
func checkCompatible(meta *Meta, opts FetchOptions) error {
	if !strings.EqualFold(meta.Typ, opts.Type) {
		return fmt.Errorf("connection %s type conflict: pooled %q vs requested %q", meta.ID, meta.Typ, opts.Type)
	}
	return nil
}

// ReloadNamedConnection is called when server starts. It initializes all stored named connections
func ReloadNamedConnection() error {
	// Persistent read happens once, outside the Manager lock.
	cfgs, err := conf.GetCfgFromKVStorage("connections", "", "")
	if err != nil {
		return err
	}
	for key, props := range cfgs {
		names := strings.Split(key, ".")
		if len(names) != 3 {
			continue
		}
		typ := names[1]
		id := names[2]
		m := globalConnectionManager.Load()
		e, reserved := m.reserveCreating(id)
		if !reserved {
			continue
		}
		cloned := maps.Clone(props)
		// Reload semantics differ from Create: the KV record is already
		// committed, so a static failure must NOT hide the resource.
		// Provision/runtime failure publishes a manageable Meta in
		// disconnected state (GET/PUT/DELETE keep working); only the
		// worker is skipped. No second persist happens here.
		m.reloadOne(e, id, typ, cloned)
	}
	return nil
}

// reloadOne provisions one persisted record outside the Manager lock,
// then publishes under it. Success yields a normal dialing Meta;
// static failure yields a manageable Meta preset to disconnected with
// the error, no worker, and an already-closed done channel so stop()
// is a no-op for the missing worker. Either way exactly one outcome
// is published and waiters (none normally, Reload runs at bootstrap)
// are woken.
func (m *Manager) reloadOne(e *poolEntry, id, typ string, props map[string]any) {
	conn, err := provisionConnection(serverStreamContext(m.ctx), id, typ, props)
	m.Lock()
	cur, ok := m.connectionPool[id]
	if !ok || cur != e || cur.state != entryCreating {
		// Stale reservation under the serialized-Init contract can
		// only mean caller misuse. Release the lock before closing:
		// Close is provider code and never runs under the Manager
		// lock, even on this unreachable path.
		m.Unlock()
		if conn != nil {
			conn.Close(serverStreamContext(m.ctx))
		}
		return
	}
	meta := newMeta(m, id, typ, props, true)
	if err != nil {
		// Keep the persisted resource manageable: preset failure
		// state instead of deleting the reservation. No worker is
		// started; done is pre-closed so stop() returns immediately
		// and Close(nil) is skipped.
		meta.NotifyStatus(api.ConnectionDisconnected, err.Error())
		meta.cw = &connWrapper{
			ID:          id,
			initialized: true,
			err:         err,
			readCh:      make(chan struct{}),
			meta:        meta,
		}
		close(meta.cw.readCh)
		close(meta.done)
		conf.Log.Warnf("reload named connection %s provision failed, kept manageable: %v", id, err)
	} else {
		meta.pendingConn = conn
		meta.cw = newConnWrapper(meta)
	}
	e.meta = meta
	e.state = entryReady
	close(e.ready)
	m.Unlock()
}

// Connection API handlers

// CreateNamedConnection creates a named connection without attaching a
// consumer reference: the returned Lease holds no ref, so Release on it
// is a no-op. It exists so API handlers and tests can observe the
// shared handle (Wait/Status) without owning a reference.
func CreateNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnectionLease, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	// createNamedConnection owns its locking (reservation, wait, publish).
	return createNamedConnection(ctx, id, typ, props)
}

type namedCreateKind int

const (
	namedCreate namedCreateKind = iota
	namedWait
	namedFailed
)

type namedCreatePlan struct {
	kind namedCreateKind
	// namedCreate: reserved entry plus the immutable props clone.
	entry *poolEntry
	props map[string]any
	// namedWait: creation entry to wait on via waitNamedCreation.
	waitEntry *poolEntry
	wait      <-chan struct{}
	err       error
}

// planNamedCreate decides one named creation attempt under a single
// critical section.
func (m *Manager) planNamedCreate(id string, props map[string]any) namedCreatePlan {
	m.Lock()
	defer m.Unlock()
	if e, ok := m.connectionPool[id]; ok {
		switch e.state {
		case entryReady:
			return namedCreatePlan{kind: namedFailed, err: fmt.Errorf("connection %v already been created", id)}
		case entryCreating:
			return namedCreatePlan{kind: namedWait, waitEntry: e, wait: e.ready}
		default: // entryRemoving
			return namedCreatePlan{kind: namedFailed, err: ErrConnectionRemoving}
		}
	}
	e := &poolEntry{state: entryCreating, ready: make(chan struct{})}
	m.connectionPool[id] = e
	return namedCreatePlan{kind: namedCreate, entry: e, props: maps.Clone(props)}
}

func createNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnectionLease, error) {
	m := globalConnectionManager.Load()
	plan := m.planNamedCreate(id, props)
	switch plan.kind {
	case namedFailed:
		return nil, plan.err
	case namedWait:
		return nil, waitNamedCreation(ctx, m, plan.waitEntry, plan.wait, id)
	default:
		// Static ordering: Provision, then persist, then publish, then worker.
		// A persist failure publishes only an error: no Meta, no worker.
		// Named creation carries no creator ref: zero-ref named is idle.
		meta, err := m.runCreation(plan.entry, id, typ, plan.props, true, func() error {
			return storeConnectionMeta(typ, id, plan.props)
		}, "", nil)
		if err != nil {
			return nil, err
		}
		return newLease(meta.cw, id, ""), nil
	}
}

// waitNamedCreation shares a concurrent creation round without promoting
// the waiter to creator: on success the key already exists, so the waiter
// gets already-exists; on failure it gets the same creation error and may
// retry with a fresh Create call.
func waitNamedCreation(ctx api.StreamContext, m *Manager, e *poolEntry, ch <-chan struct{}, id string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
	}
	if e.err != nil {
		return e.err
	}
	if e.meta == nil || globalConnectionManager.Load() != m {
		return ErrConnectionClosed
	}
	return fmt.Errorf("connection %v already been created", id)
}

func storeConnectionMeta(plugin, id string, props map[string]interface{}) error {
	err := conf.WriteCfgIntoKVStorage("connections", plugin, id, props)
	failpoint.Inject("storeConnectionErr", func() {
		err = errors.New("storeConnectionErr")
	})
	return err
}
