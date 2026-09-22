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
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

// Connection pool manages all connections in the system. There are two kinds of connections:
// 1. Named connection: Long running connection. Users can create it standalone through dedicated API without rules.
// The connection will run through all the eKuiper server lifecycle. When restarting, it will be loaded and run as server init.
// 2. Anonymous connection: It is a subsidiary of rules. The rule source/sink defines connection and the connection will
// be fetched when rules start. If no rule has accessed it, it will be closed and dropped.

type Manager struct {
	syncx.RWMutex
	// connectionPool maps a connectionKey to its coordination entry.
	// Entries exist for creating, ready, and (from the A1b stop work)
	// removing Metas alike, so concurrent Fetch/Create always observe
	// one authoritative record per key.
	connectionPool map[string]*poolEntry
	// ctx is the server-scoped lifecycle root. Every Meta lifecycle
	// derives from it; it is canceled on re-init/shutdown only, never
	// by any rule, request, or first fetcher.
	ctx context.Context
	// cancel terminates the whole manager scope. Nil only before the
	// first InitConnectionManager call.
	cancel context.CancelFunc
}

// entryState is the lifecycle stage of a pooled key.
type entryState int

const (
	// entryCreating reserves the key while the heavy creation
	// transaction (provider/Provision/persist/Meta build) runs outside
	// the Manager lock. Exactly one creator owns it.
	entryCreating entryState = iota
	// entryReady holds a published Meta ready for attach.
	entryReady
	// entryRemoving marks a key owned by teardown. Callers apply
	// operation-specific policy: anonymous fetches wait for cleanup and
	// retry, RequireExisting fetches and named creates fail fast with
	// ErrConnectionRemoving.
	entryRemoving
)

type poolEntry struct {
	state entryState
	// ready is closed exactly once when this creation round concludes
	// (success or failure). It never signals connection readiness;
	// use ConnWrapper.Wait / Meta readiness for that.
	ready chan struct{}
	// meta is set on successful creation before ready is closed.
	meta *Meta
	// err carries the creation failure to all waiters. Mutually
	// exclusive with meta: exactly one of them is set at close.
	err error
	// removed is created when the entry flips to removing and closed
	// after cleanup deletes the entry. Fetch waiters use it to wait
	// out a teardown instead of failing, mirroring the old behavior
	// where they blocked on the Manager lock until Close finished.
	removed chan struct{}
}

// globalConnectionManager is swapped wholesale on Init/reset. The
// pointer itself is synchronized so concurrent readers (e.g. a rule
// teardown racing a test reset) never trip the memory model; every
// mutation path still goes through the captured instance's own
// lock. This does NOT make concurrent Init safe: Init/reset stays
// contractually serialized with all mutations, it just fails
// observably instead of racing.
var globalConnectionManager atomic.Pointer[Manager]

func init() {
	ctx, cancel := context.WithCancel(context.Background())
	globalConnectionManager.Store(&Manager{
		connectionPool: make(map[string]*poolEntry),
		ctx:            ctx,
		cancel:         cancel,
	})
}

func InitConnectionManager4Test() error {
	conf.IsTesting = true
	InitConnectionManager(context.Background())
	return nil
}

func InitConnectionManager(ctx context.Context) {
	// Bootstrap/reset only: must not race Fetch/Create/Update/Drop/
	// Detach/Reload. The previous generation, if any, is retired
	// serially first (scope canceled, every published runtime
	// connection stopped and closed), and only then replaced.
	// Persistent named records are left intact; the next bootstrap
	// reloads them via ReloadNamedConnection. This is generation
	// replacement, not process shutdown: server exit keeps relying on
	// the existing rule teardown path and never calls into here.
	if ctx == nil {
		ctx = context.Background()
	}
	if prev := globalConnectionManager.Load(); prev != nil {
		prev.stopAllRuntime()
	}
	mctx, cancel := context.WithCancel(ctx)
	globalConnectionManager.Store(&Manager{
		connectionPool: make(map[string]*poolEntry),
		ctx:            mctx,
		cancel:         cancel,
	})
	if conf.IsTesting {
		return
	}
	go PatrolConnectionStatusJob(ctx)
}

// stopAllRuntime synchronously retires every published runtime
// connection of this manager: cancel the scope, then Meta.stop each
// (which waits worker exit and Closes exactly once via stopOnce).
// Named and anonymous Metas are both stopped; no KV record is touched.
// Init/reset is contractually serialized with mutations, so no new
// Fetch/Create/Detach/Drop can interleave here and no waiter can be
// parked on these entries — the runtime removing/waiter protocol is
// deliberately not simulated on this path.
func (m *Manager) stopAllRuntime() {
	if m.cancel != nil {
		m.cancel()
	}
	m.Lock()
	var metas []*Meta
	for _, e := range m.connectionPool {
		if e.state == entryReady && e.meta != nil {
			metas = append(metas, e.meta)
		}
	}
	// Reset owns the whole map: drop every record now instead of
	// transitioning through removing.
	m.connectionPool = make(map[string]*poolEntry)
	m.Unlock()
	cleanupCtx := serverStreamContext(context.Background())
	for _, meta := range metas {
		meta.stop(cleanupCtx)
	}
}

const (
	DefaultInitialInterval = 100 * time.Millisecond
	DefaultMaxInterval     = 10 * time.Second
)

func PatrolConnectionStatusJob(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			patrolConnectionStatus()
		}
	}
}

func patrolConnectionStatus() {
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	for connName, e := range m.connectionPool {
		// For now, we only patrol named connection. Creating entries
		// have no Meta yet and report nothing.
		if e.state != entryReady || e.meta == nil || !e.meta.Named {
			continue
		}
		status, _ := e.meta.GetStatus()
		switch status {
		case api.ConnectionConnected:
			ConnStatusGauge.WithLabelValues(connName).Set(1)
		case api.ConnectionDisconnected:
			ConnStatusGauge.WithLabelValues(connName).Set(-1)
		case api.ConnectionConnecting:
			ConnStatusGauge.WithLabelValues(connName).Set(0)
		}
	}
}

func NewExponentialBackOffWithMaxElapsedTime(maxElapsedTime time.Duration) *backoff.ExponentialBackOff {
	return newExponentialBackOff(maxElapsedTime)
}

func newExponentialBackOff(maxElapsedTime time.Duration) *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultInitialInterval),
		backoff.WithMaxInterval(DefaultMaxInterval),
		backoff.WithMaxElapsedTime(maxElapsedTime),
	)
}

// FetchOptions carries the explicit connection identity for the new fetch
// path (DESIGN §8, A1a). ConnectionKey selects which logical connection to
// share; RefID identifies which consumer holds it. The two must never be
// mixed: Pool never derives one from the other.
type FetchOptions struct {
	// ConnectionKey is the opaque logical-connection identity. For named
	// connections it is the connectionSelector; for anonymous ones it is
	// the connector-provided canonical key (e.g. resolved SQL DB URL).
	ConnectionKey string
	// RefID identifies the holding consumer. Source/Sink use the
	// rule+op+instance identity; lookup tables use the framework-injected
	// lookup resource identity. It must be saved by the consumer at
	// Connect time and passed back verbatim at detach time.
	RefID string
	// RequireExisting marks a fetch that may only attach to an already
	// existing logical connection: a missing ConnectionKey is an error
	// and anonymous creation is never performed. Used for
	// connectionSelector/named references. False allows creating an
	// anonymous Meta when the key is absent. This describes the fetch
	// mode, not the Meta lifecycle type (Meta.Named).
	RequireExisting bool
	Type            string
	Props           map[string]any
	// StatusHandler receives connection status changes for this ref.
	StatusHandler api.StatusChangeHandler
}

// ConsumerRefID derives the stable Source/Sink consumer identity
// (ruleID + opID + instanceID) for the current context. Connect must save
// the returned value and pass it back verbatim to DetachConnectionByRef.
func ConsumerRefID(ctx api.StreamContext) string {
	return extractRefId(ctx)
}

// FetchConnectionWithOptions is the explicit-identity fetch path. Callers
// must canonicalize props/key and compute RefID before calling; Pool treats
// ConnectionKey as an opaque identity.
func FetchConnectionWithOptions(ctx api.StreamContext, opts FetchOptions) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if opts.ConnectionKey == "" {
		return nil, fmt.Errorf("connection key should be defined")
	}
	if opts.RefID == "" {
		return nil, fmt.Errorf("connection ref id should be defined")
	}
	if opts.Type == "" {
		return nil, fmt.Errorf("connection type should be defined")
	}
	// No Manager lock held here: fetchInternal manages lock/unlock
	// internally (it must release the lock while waiting on or running
	// creation).
	return fetchInternal(ctx, opts)
}

// FetchConnection is the legacy compatibility shim. It keeps the historical
// connectionKey derivation (connectionSelector-or-refId) so unmigrated
// connectors keep resolving the same logical connection. The consumer ref,
// however, is normalized to ConsumerRefID(ctx): legacy DetachConnection
// derives exactly that value, and the historical habit of passing
// connection-identity material (endpoint/topic/URL) as refId never
// identified the consumer. Without this normalization every legacy Close
// would miss its ref and leak the reference. New code must use
// FetchConnectionWithOptions instead.
func FetchConnection(ctx api.StreamContext, refId, typ string, props map[string]interface{}, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if refId == "" {
		return nil, fmt.Errorf("connection ref id should be defined")
	}
	conId := extractSelID(props, refId)
	opts := FetchOptions{
		ConnectionKey:   conId,
		RefID:           ConsumerRefID(ctx),
		RequireExisting: conId != refId,
		Type:            typ,
		Props:           props,
		StatusHandler:   sc,
	}
	// Same as above: fetchInternal owns its locking.
	return fetchInternal(ctx, opts)
}

// fetchInternal implements lookup-or-create plus attach. Resolution and
// attach share one Manager critical section per attempt, so a ready
// attach and a last-detach teardown are mutually exclusive: either the
// fetch lands first (the later detach sees refs and stands down) or the
// teardown wins (the fetch observes removing, never the dying Meta).
//
//   - Ready entry: compatibility-check, then attach, all under the lock.
//     AddRef still reaches the preexisting GetStatus path (network I/O
//     under the lock is A2 debt, stated here so nobody mistakes this for
//     the final lock discipline).
//   - Creating entry: wait for the round, then re-resolve by key (the
//     entry pointer may have turned over while waiting).
//   - Removing entry: anonymous fetches wait for cleanup and retry the
//     whole lookup; RequireExisting fetches fail fast.
//   - Missing key: RequireExisting fails; otherwise this caller becomes
//     the single-flight creator.
//
// The initiating caller ctx only bounds waiting. Shared creation runs on
// the manager scope, so one caller going away never cancels creation for
// the others.
func fetchInternal(ctx api.StreamContext, opts FetchOptions) (*ConnWrapper, error) {
	for {
		m := globalConnectionManager.Load()
		m.Lock()
		e, ok := m.connectionPool[opts.ConnectionKey]
		if !ok {
			if opts.RequireExisting {
				m.Unlock()
				return nil, fmt.Errorf("connection %s not existed", opts.ConnectionKey)
			}
			e = &poolEntry{state: entryCreating, ready: make(chan struct{})}
			m.connectionPool[opts.ConnectionKey] = e
			// Shallow-copy the caller map: Meta.Props is immutable once
			// inside the Pool. maps.Clone(nil) is nil, so no nil guard
			// needed. Connectors requiring deep-copy semantics must
			// normalize before fetching.
			props := maps.Clone(opts.Props)
			m.Unlock()
			meta, err := m.runCreation(e, opts.ConnectionKey, opts.Type, props, false, nil, opts.RefID, opts.StatusHandler)
			if err != nil {
				return nil, err
			}
			conf.Log.Infof("FetchConnection return new conn %s", meta.ID)
			return meta.cw, nil
		}
		switch e.state {
		case entryReady:
			if err := checkCompatible(e.meta, opts); err != nil {
				m.Unlock()
				return nil, err
			}
			conf.Log.Infof("FetchConnection return existed conn %s", e.meta.ID)
			if e.meta.ID != opts.RefID {
				conf.Log.Infof("action=reuse_connection connId=%s type=%s connectionKey=%s rule=%s op=%s refId=%s", e.meta.ID, opts.Type, e.meta.ID, ctx.GetRuleId(), ctx.GetOpId(), opts.RefID)
			}
			cw, err := attachToMeta(e.meta, opts.RefID, opts.StatusHandler)
			m.Unlock()
			if err != nil {
				return nil, err
			}
			return cw, nil
		case entryCreating:
			ch := e.ready
			m.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ch:
			}
			// e is immutable past ready-close, so reading err here
			// is safe even if the key has since turned over.
			if e.err != nil {
				return nil, e.err
			}
			continue
		default: // entryRemoving
			if opts.RequireExisting {
				m.Unlock()
				return nil, ErrConnectionRemoving
			}
			// A teardown owns this key. Wait it out (or caller
			// cancel) and re-resolve: after cleanup the key is
			// absent and this fetch creates fresh, exactly as if
			// it had blocked on the old global-lock Close path.
			removed := e.removed
			m.Unlock()
			if removed == nil {
				return nil, ErrConnectionRemoving
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-removed:
				continue
			}
		}
	}
}

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
	m.Lock()
	defer m.Unlock()
	// Verify our reservation survived. Init/reset is contractually
	// serialized with mutations, so the only way out here is a stale
	// entry from a misuse path — never silently publish over it.
	cur, ok := m.connectionPool[key]
	if !ok || cur != e || cur.state != entryCreating {
		if conn != nil {
			conn.Close(serverStreamContext(m.ctx))
		}
		return nil, ErrConnectionClosed
	}
	if err != nil {
		if conn != nil {
			// The candidate belongs to the Pool from a nil Provision
			// return on; a failed later step must release it instead
			// of dropping the reference.
			conn.Close(serverStreamContext(m.ctx))
		}
		e.err = err
		close(e.ready)
		delete(m.connectionPool, key)
		return nil, err
	}
	meta := newMeta(m, key, typ, props, named)
	meta.pendingConn = conn
	meta.cw = newConnWrapper(meta)
	if attachRef != "" {
		meta.AddRef(attachRef, sc)
	}
	e.meta = meta
	e.state = entryReady
	close(e.ready)
	return meta, nil
}

// attachToMeta stores one consumer reference and returns the stable
// handle. Callers must hold the Manager lock so the attach is atomic
// with zero-ref teardown decisions. The lifecycle guard fails the
// attach instead of parking a ref on a dying Meta.
func attachToMeta(meta *Meta, refId string, sc api.StatusChangeHandler) (*ConnWrapper, error) {
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
		m.Lock()
		if _, ok := m.connectionPool[id]; ok {
			m.Unlock()
			continue
		}
		e := &poolEntry{state: entryCreating, ready: make(chan struct{})}
		m.connectionPool[id] = e
		cloned := maps.Clone(props)
		m.Unlock()
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
		meta.cw = &ConnWrapper{
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

func CreateNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	// createNamedConnection owns its locking (reservation, wait, publish).
	return createNamedConnection(ctx, id, typ, props)
}

func createNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	m := globalConnectionManager.Load()
	m.Lock()
	if e, ok := m.connectionPool[id]; ok {
		switch e.state {
		case entryReady:
			m.Unlock()
			return nil, fmt.Errorf("connection %v already been created", id)
		case entryCreating:
			ch := e.ready
			m.Unlock()
			return waitNamedCreation(ctx, m, e, ch, id)
		default: // entryRemoving
			m.Unlock()
			return nil, ErrConnectionRemoving
		}
	}
	e := &poolEntry{state: entryCreating, ready: make(chan struct{})}
	m.connectionPool[id] = e
	cloned := maps.Clone(props)
	m.Unlock()
	// Static ordering: Provision, then persist, then publish, then worker.
	// A persist failure publishes only an error: no Meta, no worker.
	// Named creation carries no creator ref: zero-ref named is idle.
	meta, err := m.runCreation(e, id, typ, cloned, true, func() error {
		return storeConnectionMeta(typ, id, cloned)
	}, "", nil)
	if err != nil {
		return nil, err
	}
	return meta.cw, nil
}

// waitNamedCreation shares a concurrent creation round without promoting
// the waiter to creator: on success the key already exists, so the waiter
// gets already-exists; on failure it gets the same creation error and may
// retry with a fresh Create call.
func waitNamedCreation(ctx api.StreamContext, m *Manager, e *poolEntry, ch <-chan struct{}, id string) (*ConnWrapper, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ch:
	}
	if e.err != nil {
		return nil, e.err
	}
	if e.meta == nil || globalConnectionManager.Load() != m {
		return nil, ErrConnectionClosed
	}
	return nil, fmt.Errorf("connection %v already been created", id)
}

func GetAllConnectionsMeta(forceAll bool) []*Meta {
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	metaList := make([]*Meta, 0)
	for _, e := range m.connectionPool {
		if e.state != entryReady || e.meta == nil {
			continue
		}
		if !e.meta.Named && !forceAll {
			continue
		}
		metaList = append(metaList, e.meta)
	}
	return metaList
}

func GetConnectionDetail(_ api.StreamContext, id string) (*Meta, error) {
	if id == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	e, ok := m.connectionPool[id]
	if !ok || e.state != entryReady || e.meta == nil {
		return nil, fmt.Errorf("connection %s not existed", id)
	}
	return e.meta, nil
}

func dropNameConnection(m *Manager, ctx api.StreamContext, selId string) (meta *Meta, stop func(api.StreamContext), err error) {
	// Caller holds m's lock and keeps using m afterwards.
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

func DropNameConnection(ctx api.StreamContext, selId string) error {
	if selId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	m := globalConnectionManager.Load()
	m.Lock()
	_, stop, err := dropNameConnection(m, ctx, selId)
	m.Unlock()
	if err != nil {
		// DELETE stays idempotent: a teardown already owning the key
		// converges to the same end state, so a concurrent dropper
		// reports success. Internal strict callers (Update) use
		// dropNameConnection directly and still see the error.
		if errors.Is(err, ErrConnectionRemoving) {
			return nil
		}
		return err
	}
	if stop != nil {
		finishStop(m, selId, stop, ctx)
	}
	return nil
}

func UpdateConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	// Validation + drop run under one hold of a single captured manager;
	// creation re-locks internally, so the lock must be released in
	// between (no defer across the call).
	m := globalConnectionManager.Load()
	m.Lock()
	isInternal, err := isInternalConnection(m, id)
	if err != nil {
		m.Unlock()
		return nil, err
	}
	if isInternal {
		m.Unlock()
		return nil, fmt.Errorf("internal connection %v can't be edit", id)
	}
	// Drop holds the lock only for validation and stop handoff; the
	// previous generation is fully stopped and removed before creating
	// the replacement, preserving drop-then-create ordering. Creation
	// re-locks internally.
	_, stop, err := dropNameConnection(m, ctx, id)
	m.Unlock()
	if err != nil {
		return nil, err
	}
	if stop != nil {
		finishStop(m, id, stop, ctx)
	}
	return createNamedConnection(ctx, id, typ, props)
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

func storeConnectionMeta(plugin, id string, props map[string]interface{}) error {
	err := conf.WriteCfgIntoKVStorage("connections", plugin, id, props)
	failpoint.Inject("storeConnectionErr", func() {
		err = errors.New("storeConnectionErr")
	})
	return err
}

func dropConnectionStore(plugin, id string) error {
	err := conf.DropCfgKeyFromStorage("connections", plugin, id)
	failpoint.Inject("dropConnectionStoreErr", func() {
		err = errors.New("dropConnectionStoreErr")
	})
	return err
}

func attachConnection(conId string, refId string, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	if conId == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	// Test/compat helper: same atomic attach as the fast path, just
	// resolved by key instead of by entry.
	m := globalConnectionManager.Load()
	m.Lock()
	defer m.Unlock()
	meta, err := readyMeta(m, conId)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, fmt.Errorf("connection %s not existed", conId)
	}
	meta.AddRef(refId, sc)
	if conId != refId {
		conf.Log.Infof("action=attach_connection_ref connId=%s type=%s connectionKey=%s refId=%s refCount=%d", conId, meta.Typ, conId, refId, meta.GetRefCount())
	}
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

// provisionConnection runs provider lookup plus the synchronous static
// Provision phase. It performs no Dial and no retry: static errors fail
// here, before any Meta is published or worker started.
//
// Provision is static by contract (local validation only): it must not
// acquire live resources, so a provisioned object dropped on a later
// failure step is discarded undisposed rather than closed. Resource
// ownership starts at Dial.
func provisionConnection(ctx api.StreamContext, key, typ string, props map[string]any) (modules.Connection, error) {
	connRegister, ok := modules.GetConnectionProvider(strings.ToLower(typ))
	if !ok {
		return nil, fmt.Errorf("unknown connection type")
	}
	conn := connRegister(ctx)
	if err := conn.Provision(ctx, key, props); err != nil {
		return nil, err
	}
	return conn, nil
}

// dialInitial runs the unbounded initial Dial retry loop for an already
// provisioned connection. The loop only ends on success, on a permanent
// (non-IO) Dial error, or on context death. Context death is reported as
// a permanent error carrying ctx.Err(): lifecycle termination must never
// look like a successful Dial. Callers map it further (the Pool worker
// translates its own lifecycle death to ErrConnectionClosed).
func dialInitial(connCtx api.StreamContext, meta *Meta, conn modules.Connection) (modules.Connection, error) {
	var err error
	sc, isStateful := conn.(modules.StatefulDialer)
	if isStateful {
		sc.SetStatusChangeHandler(connCtx, meta.NotifyStatus)
	}
	err = backoff.Retry(func() error {
		select {
		case <-connCtx.Done():
			return backoff.Permanent(connCtx.Err())
		default:
		}
		meta.NotifyStatus(api.ConnectionConnecting, "")
		connCtx.GetLogger().Debugf("connection retry: %s", meta.ID)
		err = conn.Dial(connCtx)
		if err == nil {
			if !isStateful {
				meta.NotifyStatus(api.ConnectionConnected, "")
			}
			return nil
		}
		connCtx.GetLogger().Debugf("connection failed: %s, %v", meta.ID, err)
		meta.NotifyStatus(api.ConnectionDisconnected, err.Error())
		if errorx.IsIOError(err) {
			return err
		}
		return backoff.Permanent(err)
		// No max elapsed time: pooled connections keep retrying until their
		// lifecycle context ends. Consumers decide whether and when to wait
		// for the ConnWrapper to become ready.
	}, NewExponentialBackOffWithMaxElapsedTime(0))
	return conn, err
}

// Return the unique connection id and whether it is set explicitly
func extractSelID(props map[string]interface{}, anomId string) string {
	if len(props) < 1 {
		return anomId
	}
	v, ok := props["connectionSelector"]
	if !ok {
		return anomId
	}
	id, ok := v.(string)
	if !ok {
		return anomId
	}
	return id
}

func extractRefId(ctx api.StreamContext) string {
	return fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}
