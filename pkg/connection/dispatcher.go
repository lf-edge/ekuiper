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
	"sync/atomic"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

// attachTokenSeq mints process-unique attachment tokens: every AddRef
// consumes one, never reused, so a token can never collide with a
// later attachment even across Meta recreation. Zero means no token.
// Never reset: uniqueness across resets is what makes stale-lease
// checks sound — a reset that recycled tokens would reintroduce ABA.
var attachTokenSeq atomic.Uint64

// statusDispatch is one frozen status event in the per-Meta FIFO.
// Transition events carry the handler membership frozen at enqueue
// time: a ref attached after the enqueue never receives the earlier
// transition, so a late attacher can never observe attach-before
// history. Initial-delivery events carry exactly one handler plus
// its refId; the dispatcher additionally verifies the ref is still
// registered at dispatch time, so a ref detached between enqueue and
// dispatch is skipped. A barrier event carries no status: the
// dispatcher closes its channel once every event enqueued before it
// is delivered, which is what makes the stop-path drain exact.
type statusDispatch struct {
	status, errMsg string
	handlers       []api.StatusChangeHandler
	refId          string
	// token binds an initial-delivery event to the attachment that
	// enqueued it; transition events leave it zero and skip the check.
	token   uint64
	barrier chan struct{}
}

func (meta *Meta) NotifyStatus(status string, s string) {
	// eventMu orders producers: the transition, the exact event
	// snapshot and the frozen handler membership are one atomic unit
	// in enqueue order. The readiness generation still closes inside
	// the transition (before any delivery), so WaitReady waiters wake
	// promptly; delivery itself never blocks the next producer —
	// that decoupling is what keeps a slow consumer from stalling
	// the recovery worker.
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
	// Freeze the handler membership now, not at dispatch: the queue
	// may hold a backlog, and a ref attached afterwards must not
	// receive history from before its attach.
	meta.refMu.Lock()
	handlers := make([]api.StatusChangeHandler, 0, len(meta.refs))
	for _, sc := range meta.refs {
		handlers = append(handlers, sc)
	}
	meta.refMu.Unlock()
	meta.enqueueLocked(statusDispatch{status: effStatus, errMsg: effErr, handlers: handlers})
}

// enqueueLocked appends one event to the FIFO and wakes the
// dispatcher. Caller holds eventMu. After stop() sets
// dispatcherStopping the event is dropped: state (already
// transitioned by the caller) stays truthful, delivery is moot on a
// dying Meta.
func (meta *Meta) enqueueLocked(ev statusDispatch) {
	if meta.dispatcherStopping {
		return
	}
	meta.eventQueue = append(meta.eventQueue, ev)
	select {
	case meta.eventWake <- struct{}{}:
	default:
	}
}

// dispatchLoop delivers frozen events FIFO on a single goroutine,
// which is the only delivery path: exactly once, in enqueue order,
// never concurrent on one Meta. A wakeup against an empty queue is a
// no-op. It holds no lock across a handler invocation (refMu briefly
// for initial-event membership), so a blocked consumer stalls only
// its own Meta's stream. Exits only after stop() sets
// dispatcherStopping and the queue drains.
func (meta *Meta) dispatchLoop() {
	defer close(meta.dispatcherDone)
	for {
		meta.eventMu.Lock()
		for len(meta.eventQueue) == 0 {
			if meta.dispatcherStopping {
				meta.eventMu.Unlock()
				return
			}
			meta.eventMu.Unlock()
			<-meta.eventWake
			meta.eventMu.Lock()
		}
		ev := meta.eventQueue[0]
		// Clear the popped slot so handler references do not linger
		// in the backing array.
		meta.eventQueue[0] = statusDispatch{}
		meta.eventQueue = meta.eventQueue[1:]
		meta.eventMu.Unlock()
		if ev.barrier != nil {
			close(ev.barrier)
			continue
		}
		if ev.refId != "" {
			meta.refMu.RLock()
			cur, ok := meta.refTokens[ev.refId]
			meta.refMu.RUnlock()
			if !ok || cur != ev.token {
				continue
			}
		}
		for _, h := range ev.handlers {
			if h != nil {
				h(ev.status, ev.errMsg)
			}
		}
	}
}

// drainEvents blocks until every event enqueued so far is delivered.
// The barrier rides the same FIFO, so its completion means exactly
// that. Only the stop path uses it (two phases, see stop).
func (meta *Meta) drainEvents() {
	done := make(chan struct{})
	meta.eventMu.Lock()
	meta.eventQueue = append(meta.eventQueue, statusDispatch{barrier: done})
	select {
	case meta.eventWake <- struct{}{}:
	default:
	}
	meta.eventMu.Unlock()
	<-done
}

// AddRef registers one consumer reference and mints its attachment
// token. It is structural only: no status read, no callback, no I/O —
// safe under the Manager lock. Every call mints a fresh token, even a
// same-ref reattach that replaces the handler without growing the
// count: the token identifies this attachment, not just the refID, so
// a stale Lease from an earlier attachment can never release or
// observe a later one. The initial state delivery is a separate step
// (deliverInitial) that runs after the Manager lock is released, so a
// slow consumer can never stall the Pool. Registration precedes
// delivery; combined with per-Meta FIFO event order (eventMu) every
// handler observes each transition exactly once and in order, never
// concurrently and never inverted. A racing initial delivery may
// duplicate the latest state (connected, connected) but never reports
// new-then-old and never swallows a real transition.
func (meta *Meta) AddRef(refId string, sc api.StatusChangeHandler) uint64 {
	token := attachTokenSeq.Add(1)
	meta.refMu.Lock()
	if meta.refs == nil {
		meta.refs = make(map[string]api.StatusChangeHandler)
	}
	if meta.refTokens == nil {
		meta.refTokens = make(map[string]uint64)
	}
	_, dup := meta.refs[refId]
	meta.refs[refId] = sc
	meta.refTokens[refId] = token
	count := len(meta.refs)
	meta.refMu.Unlock()
	if dup {
		conf.Log.Infof("conn %s re-attach existing reference %s, refs stay %d", meta.ID, refId, count)
		return token
	}
	conf.Log.Infof("conn %s add reference %s to %d refs", meta.ID, refId, count)
	return token
}

// refToken reports the current attachment token for refId. Same-package
// escape for paths that must name a token without holding a Lease.
func (meta *Meta) refToken(refId string) (uint64, bool) {
	meta.refMu.RLock()
	defer meta.refMu.RUnlock()
	token, ok := meta.refTokens[refId]
	return token, ok
}

// deliverInitial enqueues the current state snapshot for a freshly
// attached consumer. Call only after releasing the Manager lock, and
// only once per attach, passing the token minted by that attach's
// AddRef. It joins the same eventMu enqueue order as NotifyStatus, so
// a concurrent transition and this initial observe a total order:
// initial-first sees the old state then the transition,
// transition-first yields the transition then a same-state initial
// duplicate. Never new-then-old, never swallowed. A ref detached (or
// reattached, which mints a new token) before dispatch is skipped, so
// a stale attachment never observes a later one.
func (meta *Meta) deliverInitial(refId string, sc api.StatusChangeHandler, token uint64) {
	if sc == nil {
		return
	}
	meta.eventMu.Lock()
	defer meta.eventMu.Unlock()
	meta.refMu.Lock()
	cur, ok := meta.refTokens[refId]
	meta.refMu.Unlock()
	if !ok || cur != token {
		return
	}
	meta.stateMu.RLock()
	s, e := meta.status, meta.lastError
	meta.stateMu.RUnlock()
	meta.enqueueLocked(statusDispatch{status: s, errMsg: e, handlers: []api.StatusChangeHandler{sc}, refId: refId, token: token})
}

func (meta *Meta) DeRef(refId string) bool {
	meta.refMu.Lock()
	defer meta.refMu.Unlock()
	if _, ok := meta.refs[refId]; !ok {
		conf.Log.Warnf("conn %s dereference missing %s, refs stay %d", meta.ID, refId, len(meta.refs))
		return false
	}
	delete(meta.refs, refId)
	delete(meta.refTokens, refId)
	count := len(meta.refs)
	conf.Log.Infof("conn %s dereference %s to %d refs", meta.ID, refId, count)
	return true
}
