// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type ConnWrapper struct {
	ID          string
	initialized bool
	conn        modules.Connection
	err         error
	l           syncx.RWMutex
	readCh      chan struct{}
	// meta back-pointer for lifecycle-aware Wait. Set once at
	// construction; the Meta outlives every handle derived from it
	// (handles never extend a lifecycle, the Pool owns it).
	meta *Meta
}

func (cw *ConnWrapper) setConn(conn modules.Connection, err error) {
	cw.l.Lock()
	defer cw.l.Unlock()
	cw.initialized = true
	cw.conn, cw.err = conn, err
}

// Wait blocks until the logical connection is first usable. It never
// returns (nil, nil). Precedence is fixed: a canceled caller always
// observes ctx.Err() first, lifecycle termination yields
// ErrConnectionClosed otherwise.
func (cw *ConnWrapper) Wait(connectorCtx api.StreamContext) (modules.Connection, error) {
	// Fixed precedence before the racing select below: when both scopes
	// are already done, the caller sees its own cancellation.
	if connectorCtx.Err() != nil {
		return nil, connectorCtx.Err()
	}
	// The select only wakes us up; it never decides the result. When
	// several cases are ready Go may pick any of them, so every path
	// below re-applies the same order: caller first, lifecycle second,
	// published result last.
	select {
	case <-connectorCtx.Done():
	case <-cw.meta.lifecycleCtx.Done():
	case <-cw.readCh:
	}
	if connectorCtx.Err() != nil {
		return nil, connectorCtx.Err()
	}
	select {
	case <-cw.meta.lifecycleCtx.Done():
		return nil, ErrConnectionClosed
	default:
	}
	cw.l.RLock()
	conn, err := cw.conn, cw.err
	cw.l.RUnlock()
	// A connection object is never handed out alongside an error: on
	// termination stop() owns closing it, callers only see the error.
	if err != nil {
		return nil, err
	}
	if conn != nil {
		// Final recheck with the same precedence: caller cancellation
		// wins even if readiness and cancellation became ready together,
		// and a dead lifecycle never hands out its connection.
		if connectorCtx.Err() != nil {
			return nil, connectorCtx.Err()
		}
		select {
		case <-cw.meta.lifecycleCtx.Done():
			return nil, ErrConnectionClosed
		default:
			return conn, nil
		}
	}
	// No usable result and no error: the worker stopped before
	// publishing (or a legacy path). Same fixed precedence: caller
	// cancellation first, lifecycle termination otherwise.
	if connectorCtx.Err() != nil {
		return nil, connectorCtx.Err()
	}
	select {
	case <-cw.meta.lifecycleCtx.Done():
		return nil, ErrConnectionClosed
	default:
	}
	return nil, ErrConnectionClosed
}

func (cw *ConnWrapper) IsInitialized() bool {
	cw.l.RLock()
	defer cw.l.RUnlock()
	return cw.initialized
}

// Status reports the last-known connection state, same pure-read
// semantics as Meta.GetStatus: it never probes the provider.
func (cw *ConnWrapper) Status() (string, string) {
	return cw.meta.GetStatus()
}

// WaitReady blocks until the pooled connection is connected. Unlike
// Wait (first-use readiness), it tracks the current lifecycle across
// disconnect/reconnect cycles: a waiter parked in a disconnected
// generation stays parked until some generation connects. It never
// returns nil error without connected status. Precedence is fixed: a
// canceled caller always observes ctx.Err() first, lifecycle
// termination yields ErrConnectionClosed otherwise. Waking from a
// generation channel always rechecks; a wake is never success.
func (cw *ConnWrapper) WaitReady(ctx api.StreamContext) error {
	// Fixed precedence before the loop: when the caller is already
	// done, it sees its own cancellation.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for {
		status, ch, closed := cw.meta.snapshotReady()
		if closed {
			return ErrConnectionClosed
		}
		if status == api.ConnectionConnected {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cw.meta.lifecycleCtx.Done():
			return ErrConnectionClosed
		case <-ch:
			// Generation ended: recheck, never assume success.
		}
	}
}

func newConnWrapper(meta *Meta) *ConnWrapper {
	cw := &ConnWrapper{
		ID:     meta.ID,
		readCh: make(chan struct{}),
		meta:   meta,
	}
	go func() {
		defer close(meta.done)
		// The worker is owned by the Meta lifecycle, never by the
		// fetching caller: first-fetcher rule stop must not kill a
		// shared connection. The connection object was provisioned
		// during creation; the worker only dials it.
		//
		// The result is always published, even when the lifecycle died
		// mid-Dial: stop() unconditionally closes whatever is here, so
		// a connection dialed past its scope is still released instead
		// of leaked. Wait() below never hands the object out alongside
		// an error; only stop() touches it after termination.
		pending := meta.pendingConn
		meta.pendingConn = nil
		conn, err := dialInitial(serverStreamContext(meta.lifecycleCtx), meta, pending)
		if meta.lifecycleCtx.Err() != nil {
			err = ErrConnectionClosed
		}
		cw.setConn(conn, err)
		close(cw.readCh)
	}()
	return cw
}

// serverStreamContext adapts a server/Manager/lifecycle context.Context to
// the api.StreamContext required by the Connection API. RuleId/OpId stay
// empty and the logger falls back to the connection default: the result
// never represents a rule lifetime.
func serverStreamContext(parent context.Context) api.StreamContext {
	return topoContext.WithContext(parent)
}

// ConnectionRecovering is the runtime-reconnect state: a previously
// connected logical connection lost its transport and (for
// self-recovering clients) is re-establishing it, or (for
// pool-recovered clients) is waiting for the Pool recovery worker.
// It shares the disconnected generation: waiters stay parked, they
// are not woken and re-parked. Defined here until it is promoted to
// the contract api on the next contract release; the wire value is
// fixed as "recovering".
const ConnectionRecovering = "recovering"

// newMeta builds a Meta whose lifecycle derives from the owning Manager.
// Caller ctx only decides whether the current API call keeps waiting;
// it never parents the Meta worker. The state domain starts as
// connecting with an open generation-0 readiness channel.
func newMeta(manager *Manager, id, typ string, props map[string]any, named bool) *Meta {
	parent := context.Background()
	if manager != nil && manager.ctx != nil {
		parent = manager.ctx
	}
	lifecycleCtx, cancel := context.WithCancel(parent)
	return &Meta{
		ID:              id,
		Typ:             typ,
		Props:           props,
		Named:           named,
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: cancel,
		done:            make(chan struct{}),
		status:          api.ConnectionConnecting,
		readyCh:         make(chan struct{}),
	}
}

type Meta struct {
	ID    string         `json:"id"`
	Typ   string         `json:"typ"`
	Props map[string]any `json:"props"`
	// named means connection is created manually
	Named bool `json:"named"`

	// refs is the single source of truth for consumer references.
	// RefCount is always len(refs). Guarded by refMu.
	refMu sync.Mutex
	refs  map[string]api.StatusChangeHandler `json:"-"`
	// cbMu serializes consumer-callback invocation per Meta: the
	// NotifyStatus broadcast and initial deliveries never run a
	// handler concurrently, and every delivery observes a snapshot
	// at least as new as any broadcast that preceded it. Lock
	// discipline: stateMu, refMu and cbMu are never nested — each is
	// released before the next is taken.
	cbMu sync.Mutex   `json:"-"`
	cw   *ConnWrapper `json:"-"`
	// lifecycleCtx parents the Meta worker. Derived from the Manager
	// server ctx at creation; canceled on zero-ref/Drop/Update/shutdown
	// or Manager re-init. Never a rule/request/first-fetcher ctx.
	lifecycleCtx context.Context `json:"-"`
	// lifecycleCancel terminates the Meta scope. Invoked exactly once
	// via stopOnce on the stop path.
	lifecycleCancel context.CancelFunc `json:"-"`
	// done is closed by the Meta worker on exit. Observers (Manager
	// re-init, stop paths) wait on it instead of polling.
	done chan struct{} `json:"-"`
	// pendingConn is the provisioned-but-undialed logical connection,
	// installed by the creation heavy phase and consumed exactly once by
	// the worker at start. Never touched after worker start.
	pendingConn modules.Connection `json:"-"`
	// stopOnce makes the stop path idempotent: concurrent stoppers
	// converge on the first execution.
	stopOnce sync.Once `json:"-"`
	// The first connection status
	// If connection is stateful, the status will update all the way
	// For stateless connection, the status needs to ping
	//
	// stateMu is the single synchronization domain for connection
	// state. status, lastError and the readiness generation (readyCh)
	// always transition together under this lock, so every snapshot
	// is consistent and WaitReady can never miss a wakeup. Reading
	// state never performs I/O (pure read); health detection is
	// produced asynchronously by provider callbacks and the health
	// probe, never by a status read.
	stateMu sync.RWMutex `json:"-"`
	// status is one of connecting/connected/disconnected/recovering.
	// connecting is only for the initial dial; runtime reconnects
	// report recovering, never connecting.
	status string `json:"-"`
	// lastError belongs to the generation that produced it: entering
	// connected clears it, entering disconnected replaces it. A stale
	// error from an older generation is never reported.
	lastError string `json:"-"`
	// readyCh is the current readiness generation: open while the
	// connection is not connected, closed on every transition into
	// connected. Leaving connected always opens a new channel, so
	// parked WaitReady waiters stay parked across
	// disconnected<->recovering without spurious wakeups.
	readyCh    chan struct{} `json:"-"`
	generation uint64        `json:"-"`
}

// closeReadyLocked ends the current generation: every waiter parked
// on readyCh wakes and rechecks. prev is the pre-transition status;
// only a transition into connected from another state closes the
// channel, duplicate connected reports are no-ops (closing an
// already-closed channel would panic). Caller holds stateMu (write).
func (meta *Meta) closeReadyLocked(prev string) {
	if prev != api.ConnectionConnected {
		close(meta.readyCh)
		meta.generation++
	}
}

// newGenerationLocked opens a fresh parked generation. Caller holds
// stateMu (write); only call when leaving connected, i.e. the
// current channel is already closed.
func (meta *Meta) newGenerationLocked() {
	meta.readyCh = make(chan struct{})
	meta.generation++
}

func (meta *Meta) NotifyStatus(status string, s string) {
	meta.stateMu.Lock()
	switch status {
	case api.ConnectionConnected:
		// A new generation ends here: clear the previous
		// generation's error even when the producer sends none.
		prev := meta.status
		meta.status = api.ConnectionConnected
		meta.lastError = ""
		meta.closeReadyLocked(prev)
	case api.ConnectionDisconnected:
		// Leaving connected parks waiters on a new generation;
		// repeated disconnects within one generation only refresh
		// the error.
		if meta.status == api.ConnectionConnected {
			meta.newGenerationLocked()
		}
		meta.status = api.ConnectionDisconnected
		meta.lastError = s
	case ConnectionRecovering:
		// Runtime reconnect shares the disconnected generation:
		// no new channel, no wakeup. Direct connected->recovering
		// still opens a generation first (leaving connected always
		// parks).
		if meta.status == api.ConnectionConnected {
			meta.newGenerationLocked()
		}
		meta.status = ConnectionRecovering
		if s != "" {
			meta.lastError = s
		}
	case api.ConnectionConnecting:
		// Initial dial attempts re-report connecting; that is a
		// no-op, not a new generation. Any other regression into
		// connecting re-opens a parked generation defensively.
		if meta.status != api.ConnectionConnecting {
			if meta.status == api.ConnectionConnected {
				meta.newGenerationLocked()
			}
			meta.status = api.ConnectionConnecting
		}
	default:
		conf.Log.Warnf("conn %s ignoring unknown status %q", meta.ID, status)
		meta.stateMu.Unlock()
		return
	}
	effStatus, effErr := meta.status, meta.lastError
	meta.stateMu.Unlock()
	// Snapshot handlers before invoking so the invocation does not run
	// while holding the refs lock. Delivery itself is serialized per
	// Meta on cbMu (see Meta.cbMu): a slow consumer delays later
	// deliveries, never the Manager lock and never concurrently with
	// another delivery to the same Meta. Handlers observe the
	// effective post-transition state, never the raw producer
	// arguments.
	meta.refMu.Lock()
	handlers := make([]api.StatusChangeHandler, 0, len(meta.refs))
	for _, sc := range meta.refs {
		handlers = append(handlers, sc)
	}
	meta.refMu.Unlock()
	meta.cbMu.Lock()
	defer meta.cbMu.Unlock()
	for _, sch := range handlers {
		if sch != nil {
			sch(effStatus, effErr)
		}
	}
}

// AddRef registers one consumer reference. It is structural only:
// no status read, no callback, no I/O — safe under the Manager lock.
// The initial state delivery is a separate step (deliverInitial) that
// runs after the Manager lock is released, so a slow consumer can
// never stall the Pool. Registration precedes delivery; combined with
// per-Meta serialized invocation (cbMu) every handler observes a
// monotonic state sequence, never a concurrent or inverted one.
func (meta *Meta) AddRef(refId string, sc api.StatusChangeHandler) {
	meta.refMu.Lock()
	if meta.refs == nil {
		meta.refs = make(map[string]api.StatusChangeHandler)
	}
	_, dup := meta.refs[refId]
	meta.refs[refId] = sc
	count := len(meta.refs)
	meta.refMu.Unlock()
	if dup {
		conf.Log.Infof("conn %s re-attach existing reference %s, refs stay %d", meta.ID, refId, count)
		return
	}
	conf.Log.Infof("conn %s add reference %s to %d refs", meta.ID, refId, count)
}

// deliverInitial delivers the current state snapshot to a freshly
// attached consumer. Call only after releasing the Manager lock. The
// snapshot is taken after registration, so any broadcast that reached
// the handler concurrently carries a state no newer than this one:
// the handler always sees old-then-new. A ref detached before this
// check is skipped; a detach racing the invocation may still observe
// one benign delivery (status sets are idempotent). Locks are never
// nested: refMu is released before cbMu is taken.
func (meta *Meta) deliverInitial(refId string, sc api.StatusChangeHandler) {
	if sc == nil {
		return
	}
	meta.refMu.Lock()
	_, ok := meta.refs[refId]
	meta.refMu.Unlock()
	if !ok {
		return
	}
	s, e := meta.GetStatus()
	meta.cbMu.Lock()
	defer meta.cbMu.Unlock()
	sc(s, e)
}

func (meta *Meta) DeRef(refId string) bool {
	meta.refMu.Lock()
	defer meta.refMu.Unlock()
	if _, ok := meta.refs[refId]; !ok {
		conf.Log.Warnf("conn %s dereference missing %s, refs stay %d", meta.ID, refId, len(meta.refs))
		return false
	}
	delete(meta.refs, refId)
	count := len(meta.refs)
	conf.Log.Infof("conn %s dereference %s to %d refs", meta.ID, refId, count)
	return true
}

// stop terminates the Meta exactly once: cancel the lifecycle, wait for
// the worker to exit, then close the logical connection. It must run
// outside the Manager lock. Concurrent stoppers converge on the first
// caller's execution; latecomers return once it completes.
func (meta *Meta) stop(ctx api.StreamContext) {
	meta.stopOnce.Do(func() {
		meta.lifecycleCancel()
		<-meta.done
		// Safe without the cw lock: the worker wrote conn before
		// closing readCh and done in the same goroutine, so the
		// receive above happens after that write. The object is
		// always non-nil here unless the worker panicked before
		// publishing, in which case there is nothing to close.
		if conn := meta.cw.conn; conn != nil {
			_ = conn.Close(ctx)
		}
	})
}

func (meta *Meta) GetRefCount() int {
	meta.refMu.Lock()
	defer meta.refMu.Unlock()
	return len(meta.refs)
}

func (meta *Meta) GetRefNames() (result []string) {
	meta.refMu.Lock()
	defer meta.refMu.Unlock()
	for key := range meta.refs {
		result = append(result, key)
	}
	return
}

// GetStatus reports the last-known connection state maintained by the
// Pool. It is a pure read: it never dials, pings or waits. Freshness
// comes from asynchronous producers (provider status callbacks, the
// initial dial loop, the health probe), never from the read itself.
// Callers must not interpret connected as "probed just now".
func (meta *Meta) GetStatus() (s string, e string) {
	meta.stateMu.RLock()
	defer meta.stateMu.RUnlock()
	return meta.status, meta.lastError
}

// snapshotReady captures one consistent readiness observation for
// WaitReady: the status, its generation channel, and whether the
// lifecycle already ended. Channel close and status change happen
// atomically under stateMu, so a waiter parked on the returned
// channel can never miss the transition that ends its generation;
// it wakes and rechecks.
func (meta *Meta) snapshotReady() (status string, ch <-chan struct{}, closed bool) {
	select {
	case <-meta.lifecycleCtx.Done():
		return "", nil, true
	default:
	}
	meta.stateMu.RLock()
	defer meta.stateMu.RUnlock()
	return meta.status, meta.readyCh, false
}
