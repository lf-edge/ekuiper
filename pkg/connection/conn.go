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

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type connWrapper struct {
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

func (cw *connWrapper) setConn(conn modules.Connection, err error) {
	cw.l.Lock()
	defer cw.l.Unlock()
	cw.initialized = true
	cw.conn, cw.err = conn, err
}

// Wait blocks until the logical connection is first usable. It never
// returns (nil, nil). Precedence is fixed: a canceled caller always
// observes ctx.Err() first, lifecycle termination yields
// ErrConnectionClosed otherwise.
func (cw *connWrapper) Wait(connectorCtx api.StreamContext) (modules.Connection, error) {
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

func (cw *connWrapper) IsInitialized() bool {
	cw.l.RLock()
	defer cw.l.RUnlock()
	return cw.initialized
}

// peekConn returns the published logical connection, or nil when the
// worker has not published yet or published a failure. Pure read for
// the health probe: it never waits for readiness.
func (cw *connWrapper) peekConn() modules.Connection {
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
func (cw *connWrapper) ReportSuspectedFailure() {
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
func (cw *connWrapper) Status() (string, string) {
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
func (cw *connWrapper) WaitReady(ctx api.StreamContext) error {
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

func newConnWrapper(meta *Meta) *connWrapper {
	cw := &connWrapper{
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
		if err == nil {
			// The initial episode concluded with a usable handle:
			// arm the runtime recovery worker before publishing,
			// so even a zero-ref named connection recovers. The
			// worker is Meta-lifecycle-owned (stop() joins it)
			// and parks until the first wakeup. No-op unless the
			// provider is pool-recoverable.
			meta.startRecoveryWorker(conn)
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

type Meta struct {
	ID    string         `json:"id"`
	Typ   string         `json:"typ"`
	Props map[string]any `json:"props"`
	// named means connection is created manually
	Named bool `json:"named"`

	// refs is the single source of truth for consumer references.
	// RefCount is always len(refs). Guarded by refMu. refTokens holds
	// the attachment token minted by each AddRef (guarded by the same
	// refMu, updated atomically with refs): Release and initial
	// delivery only honor the currently recorded token, so a stale
	// attachment can never act on a later one.
	refMu     sync.RWMutex
	refs      map[string]api.StatusChangeHandler `json:"-"`
	refTokens map[string]uint64                  `json:"-"`
	// eventMu orders the observable status event stream per Meta.
	// Producers (NotifyStatus, tryProbeDisconnect, deliverInitial)
	// hold it only to transition state, freeze the handler snapshot
	// and append one event to the FIFO below — never across a handler
	// invocation. The dispatcher then delivers FIFO, each event
	// exactly once, so every handler observes each transition in
	// order, never concurrently and never inverted. A racing initial
	// delivery may duplicate the latest state (connected, connected)
	// but never reports new-then-old and never swallows a real
	// transition. Lock discipline: eventMu is outermost; stateMu and
	// refMu are only ever taken while holding eventMu (briefly, never
	// across a callback) or on their own — never the reverse. The
	// dispatcher holds no lock across an invocation except refMu
	// briefly for initial-event membership.
	//
	// Handler contract: handlers run serially on the dispatcher, so a
	// slow consumer delays later deliveries on the same Meta — but,
	// unlike the pre-dispatcher design, never the next transition,
	// never the recovery worker, and never the Manager lock.
	// Handlers may synchronously re-enter Pool operations (enqueue
	// paths only take eventMu briefly and never wait for delivery);
	// they still must not perform I/O or block indefinitely, since
	// that stalls their own Meta's event stream and, at teardown,
	// the stop-path drain (see stop).
	eventMu sync.Mutex `json:"-"`
	// eventQueue is the FIFO of frozen status events, guarded by
	// eventMu. Append-only for producers; the dispatcher pops from
	// the front. Unbounded by design: a bounded queue would turn
	// consumer backpressure into producer backpressure, which is
	// exactly what the dispatcher exists to remove. In practice it
	// holds a handful of transitions — producers only enqueue on
	// real state changes, and the dispatcher drains continuously.
	eventQueue []statusDispatch `json:"-"`
	// eventWake wakes the dispatcher; capacity 1, coalesced. It
	// carries no truth — the dispatcher always re-reads the queue
	// under eventMu after waking, so a pending wake always means
	// "re-check the queue".
	eventWake chan struct{} `json:"-"`
	// dispatcherDone is closed by the dispatcher on exit; stop()
	// joins it after the second drain barrier.
	dispatcherDone chan struct{} `json:"-"`
	// dispatcherStopping is set under eventMu by stop() before the
	// final join. A producer callback arriving after Close (e.g. a
	// stateful client's teardown event) still applies its state
	// transition but drops the event instead of enqueueing onto a
	// stopped dispatcher. There is no close-the-queue window: the
	// queue is never closed, so a late enqueue can never panic —
	// it is simply skipped once stopping is set.
	dispatcherStopping bool         `json:"-"`
	cw                 *connWrapper `json:"-"`
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
	// suspectCh wakes the recovery worker: one buffered slot,
	// non-blocking send, coalesced by construction. It carries no
	// truth — the worker always re-reads Meta state on wakeup, and a
	// wakeup against settled state is a no-op. Sequence numbers are
	// deliberately absent: absorbing "everything seen so far" after
	// an attempt can swallow a newer episode reported during a slow
	// status delivery, parking the gate forever. Created with the
	// Meta; the worker (when started) drains it until the lifecycle
	// ends.
	suspectCh chan struct{} `json:"-"`
	// recoveryDone is closed by the recovery worker on exit. Written
	// by the initial worker before it closes done (ordered by that
	// close), read by stop() after waiting done — no extra mutex.
	// Nil when the provider is not pool-recoverable: no worker was
	// ever started, nothing to join.
	recoveryDone chan struct{} `json:"-"`
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

// snapshotProbe captures the probe's precondition atomically: public
// status, internal gate, and generation. The probe only Pings a
// connected+ready Meta, and only lands its verdict if none of the
// three moved while the Ping was in flight.
func (meta *Meta) snapshotProbe() (status string, ready bool, generation uint64) {
	meta.stateMu.RLock()
	defer meta.stateMu.RUnlock()
	return meta.status, meta.ready, meta.generation
}

// tryProbeDisconnect lands a failed probe Ping as disconnected only
// if the Meta still belongs to the probed episode: same generation,
// still connected, gate still open. A recovery (or any newer episode)
// that moved the generation while the Ping was in flight makes this
// stale verdict a no-op — it must not overwrite fresh connected
// state, nor trigger a redundant recovery. Returns true only when
// the flip was applied; only then may the caller nudge the worker.
func (meta *Meta) tryProbeDisconnect(generation uint64, errMsg string) bool {
	meta.eventMu.Lock()
	defer meta.eventMu.Unlock()
	meta.stateMu.Lock()
	if meta.generation != generation || meta.status != api.ConnectionConnected || !meta.ready {
		meta.stateMu.Unlock()
		return false
	}
	meta.status = api.ConnectionDisconnected
	meta.lastError = errMsg
	meta.verifying = false
	meta.setNotReadyLocked()
	effStatus, effErr := meta.status, meta.lastError
	meta.stateMu.Unlock()
	meta.refMu.Lock()
	handlers := make([]api.StatusChangeHandler, 0, len(meta.refs))
	for _, sc := range meta.refs {
		handlers = append(handlers, sc)
	}
	meta.refMu.Unlock()
	meta.enqueueLocked(statusDispatch{status: effStatus, errMsg: effErr, handlers: handlers})
	return true
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
