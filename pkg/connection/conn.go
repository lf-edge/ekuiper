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

// peekConn returns the published logical connection, or nil when the
// worker has not published yet or published a failure. Pure read for
// the health probe: it never waits for readiness.
func (cw *ConnWrapper) peekConn() modules.Connection {
	cw.l.RLock()
	defer cw.l.RUnlock()
	if !cw.initialized || cw.err != nil {
		return nil
	}
	return cw.conn
}

// ReportSuspectedFailure reports one failed business I/O against the
// pooled connection. The first suspect of an episode closes the
// internal readiness gate (public status stays connected) so
// subsequent WaitReady callers park; verification and recovery belong
// to the Pool worker. Non-blocking and coalesced: a storm of reports
// collapses into one worker wakeup. Safe to call from any consumer;
// it never performs I/O and never blocks.
func (cw *ConnWrapper) ReportSuspectedFailure() {
	cw.meta.reportSuspect()
}

// reportSuspect records one consumer-observed failure and nudges the
// recovery worker. The gate change is the signal; suspectCh is only
// the wakeup. StateMu only, never eventMu: closing the gate must not
// wait behind a slow status callback.
func (meta *Meta) reportSuspect() {
	meta.stateMu.Lock()
	switch meta.status {
	case api.ConnectionConnected:
		if meta.ready {
			// First suspect of this episode: close the gate,
			// open a fresh parked generation, mark verification
			// pending. Public status stays connected.
			meta.ready = false
			meta.verifying = true
			meta.readyCh = make(chan struct{})
			meta.generation++
		}
		// Already verifying: coalesce, nudge below.
	case api.ConnectionDisconnected:
		// Fault already recorded; nudge only.
	default:
		// connecting/recovering: the worker (or the initial dial)
		// owns the episode, nothing to record.
		meta.stateMu.Unlock()
		return
	}
	meta.stateMu.Unlock()
	select {
	case meta.suspectCh <- struct{}{}:
	default:
	}
}

// Status reports the last-known connection state, same pure-read
// semantics as Meta.GetStatus: it never probes the provider.
func (cw *ConnWrapper) Status() (string, string) {
	return cw.meta.GetStatus()
}

// WaitReady blocks until the pooled connection is internally ready.
// Unlike Wait (first-use readiness), it tracks the current lifecycle
// across disconnect/reconnect cycles: a waiter parked in a
// not-ready generation stays parked until some generation opens the
// gate. Readiness is the internal gate, not the public status: a
// connected status under verification still parks. It never returns
// nil error without internal readiness. Precedence is fixed: a
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
		ready, ch, closed := cw.meta.snapshotReady()
		if closed {
			return ErrConnectionClosed
		}
		if ready {
			// Final recheck with Wait() precedence: a caller that
			// canceled between the snapshot and this return must
			// observe its own cancellation, and a lifecycle that
			// ended in between must surface as termination — never
			// a success. This narrows the race to recheck-vs-return
			// (the same standard as Wait), it does not claim to
			// catch a cancel landing after the return instruction.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			select {
			case <-cw.meta.lifecycleCtx.Done():
				return ErrConnectionClosed
			default:
				return nil
			}
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
		suspectCh:       make(chan struct{}, 1),
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
	// eventMu serializes the observable status event stream per Meta:
	// one complete event (transition + handler snapshot + delivery)
	// at a time, in transition order. Every NotifyStatus transition
	// is delivered exactly once, in order; initial deliveries share
	// the same serialization, so they can duplicate the latest state
	// but never invert it (never new-then-old) and never swallow a
	// real transition. Lock discipline: eventMu is outermost;
	// stateMu and refMu are only ever taken while holding eventMu
	// (briefly, never across a callback) or on their own — never
	// the reverse. Callbacks run holding eventMu but neither
	// stateMu, refMu nor the Manager lock.
	//
	// Handler contract (the premise this serialization rests on):
	// StatusChangeHandler must return promptly, be memory-only
	// (no I/O, no blocking/waiting), and must not synchronously
	// re-enter Connection Pool operations that can produce another
	// status delivery on the same Meta (e.g. Fetch/attach of the
	// same Meta inside the callback): the callback already holds
	// eventMu, so such re-entry self-deadlocks.
	eventMu sync.Mutex   `json:"-"`
	cw      *ConnWrapper `json:"-"`
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
	// state. status, lastError, the internal readiness gate
	// (ready/verifying) and the readiness generation (readyCh) always
	// transition together under this lock, so every snapshot is
	// consistent and WaitReady can never miss a wakeup. Reading state
	// never performs I/O (pure read); health detection is produced
	// asynchronously by provider callbacks, the health probe and the
	// recovery worker, never by a status read.
	stateMu sync.RWMutex `json:"-"`
	// status is one of connecting/connected/disconnected/recovering.
	// connecting is only for the initial dial; runtime reconnects
	// report recovering, never connecting.
	status string `json:"-"`
	// lastError belongs to the generation that produced it: entering
	// connected clears it, entering disconnected replaces it. A stale
	// error from an older generation is never reported.
	lastError string `json:"-"`
	// ready is the internal readiness gate judged by WaitReady. It is
	// true exactly when the current generation channel is closed.
	// Public status may stay connected while ready is false (an
	// episode under verification): subsequent operations park even
	// though the status still reads connected.
	ready bool `json:"-"`
	// verifying marks a connected episode with a failure under
	// investigation: the first suspect closed the gate and the worker
	// has not concluded yet. Cleared on every status transition out
	// of the episode (connected/disconnected/recovering/connecting).
	verifying bool `json:"-"`
	// readyCh is the current readiness generation: open while the
	// gate is closed, closed when the gate opens. Closing the gate
	// always opens a new channel, so parked WaitReady waiters stay
	// parked across disconnected<->recovering without spurious
	// wakeups.
	readyCh    chan struct{} `json:"-"`
	generation uint64        `json:"-"`
	// suspectCh wakes the recovery worker (c3): one buffered slot,
	// non-blocking send, coalesced by construction. It carries no
	// truth — the worker always re-reads Meta state on wakeup, so a
	// stale wakeup against an open gate is a no-op. Created with the
	// Meta; the worker (when started) drains it until the lifecycle
	// ends.
	suspectCh chan struct{} `json:"-"`
}

// setReadyLocked opens the internal readiness gate and ends the
// current generation: every waiter parked on readyCh wakes and
// rechecks. Idempotent: an already-open gate changes nothing (closing
// an already-closed channel would panic). Caller holds stateMu
// (write).
func (meta *Meta) setReadyLocked() {
	if !meta.ready {
		close(meta.readyCh)
		meta.generation++
	}
	meta.ready = true
	meta.verifying = false
}

// setNotReadyLocked closes the internal readiness gate and opens a
// fresh parked generation. Idempotent: an already-closed gate changes
// nothing, so a verifying episode followed by connected->disconnected
// never opens two generations for one outage. Caller holds stateMu
// (write).
func (meta *Meta) setNotReadyLocked() {
	if meta.ready {
		meta.ready = false
		meta.readyCh = make(chan struct{})
		meta.generation++
	}
}

func (meta *Meta) NotifyStatus(status string, s string) {
	// eventMu serializes the whole observable event: the transition,
	// the exact event snapshot and the handler snapshot are one
	// atomic unit in delivery order. The readiness generation still
	// closes inside the transition (before any callback), so
	// WaitReady waiters wake promptly; only the *next* transition's
	// state write waits for a slow callback — bounded by the handler
	// contract above, never by the Manager lock.
	meta.eventMu.Lock()
	defer meta.eventMu.Unlock()
	meta.stateMu.Lock()
	switch status {
	case api.ConnectionConnected:
		// A new generation ends here: clear the previous
		// generation's error even when the producer sends none,
		// open the internal gate.
		meta.status = api.ConnectionConnected
		meta.lastError = ""
		meta.setReadyLocked()
	case api.ConnectionDisconnected:
		// The gate closes at most once per outage: a verifying
		// episode already parked waiters, so a subsequent
		// connected->disconnected only records the fault. A
		// probe-first disconnect (gate still open) parks here.
		// Repeated disconnects within one episode only refresh
		// the error.
		meta.status = api.ConnectionDisconnected
		meta.lastError = s
		meta.verifying = false
		meta.setNotReadyLocked()
	case ConnectionRecovering:
		// Runtime reconnect shares the episode: entering
		// recovering concludes verification and parks the gate,
		// never opening a second generation for one outage.
		meta.status = ConnectionRecovering
		if s != "" {
			meta.lastError = s
		}
		meta.verifying = false
		meta.setNotReadyLocked()
	case api.ConnectionConnecting:
		// Initial dial attempts re-report connecting; that is a
		// no-op, not a new generation. Any other regression into
		// connecting parks the gate defensively.
		if meta.status != api.ConnectionConnecting {
			meta.status = api.ConnectionConnecting
			meta.verifying = false
			meta.setNotReadyLocked()
		}
	default:
		conf.Log.Warnf("conn %s ignoring unknown status %q", meta.ID, status)
		meta.stateMu.Unlock()
		return
	}
	effStatus, effErr := meta.status, meta.lastError
	meta.stateMu.Unlock()
	// Snapshot handlers while still holding eventMu so no concurrent
	// deliverInitial can interleave a snapshot between our transition
	// and our delivery. The invocation itself runs holding eventMu
	// but neither the refs lock nor the state lock: a slow consumer
	// delays later deliveries on this Meta, never the Manager lock
	// and never concurrently with another delivery to the same Meta.
	// Handlers observe the exact post-transition state of this event,
	// never the raw producer arguments and never a coalesced latest.
	meta.refMu.Lock()
	handlers := make([]api.StatusChangeHandler, 0, len(meta.refs))
	for _, sc := range meta.refs {
		handlers = append(handlers, sc)
	}
	meta.refMu.Unlock()
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
// per-Meta event serialization (eventMu) every handler observes each
// transition exactly once and in order, never concurrently and never
// inverted. A racing initial delivery may duplicate the latest state
// (connected, connected) but never reports new-then-old and never
// swallows a real transition.
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
// attached consumer. Call only after releasing the Manager lock, and
// only once per attach. It joins the same eventMu serialization as
// NotifyStatus: the ref check, the state snapshot and the invocation
// are one atomic unit in delivery order. A ref detached before this
// unit runs is skipped; a detach racing the invocation may still
// observe one benign same-state duplicate (status sets are
// idempotent). A duplicate is always the latest state repeated —
// real NotifyStatus transitions are never coalesced or swallowed.
func (meta *Meta) deliverInitial(refId string, sc api.StatusChangeHandler) {
	if sc == nil {
		return
	}
	meta.eventMu.Lock()
	defer meta.eventMu.Unlock()
	meta.refMu.Lock()
	_, ok := meta.refs[refId]
	meta.refMu.Unlock()
	if !ok {
		return
	}
	meta.stateMu.RLock()
	s, e := meta.status, meta.lastError
	meta.stateMu.RUnlock()
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
// WaitReady: the internal gate, its generation channel, and whether
// the lifecycle already ended. Gate flip and channel swap happen
// atomically under stateMu, so a waiter parked on the returned
// channel can never miss the transition that ends its generation;
// it wakes and rechecks.
func (meta *Meta) snapshotReady() (ready bool, ch <-chan struct{}, closed bool) {
	select {
	case <-meta.lifecycleCtx.Done():
		return false, nil, true
	default:
	}
	meta.stateMu.RLock()
	defer meta.stateMu.RUnlock()
	return meta.ready, meta.readyCh, false
}
