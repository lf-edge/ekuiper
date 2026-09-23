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

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

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
	m := &Meta{
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
		eventWake:       make(chan struct{}, 1),
		dispatcherDone:  make(chan struct{}),
	}
	// The dispatcher owns all handler invocation from birth: every
	// producer below only enqueues. It exits on the stop path after
	// the second drain barrier (see stop).
	go m.dispatchLoop()
	return m
}

// stop terminates the Meta exactly once and joins every worker in
// dependency order. It must run outside the Manager lock. Concurrent
// stoppers converge on the first caller's execution; latecomers
// return once it completes.
//
// Order: cancel the lifecycle, wait for the initial worker, join the
// recovery worker (a Recover can never run concurrently with the
// final Close below), drain the pre-Close backlog so no observed
// state is lost, Close the provider, then drain again for finals
// produced by provider teardown itself (a stateful client's teardown
// event lands here). Only then is the dispatcher stopped and joined:
// the queue is never closed, so a provider callback racing the end
// drops its event instead of panicking — state stays truthful,
// delivery is moot on a dying Meta.
//
// Teardown caveat: the drains wait for queued handlers, so a handler
// blocked past teardown (downstream never unblocks, rule scope never
// dies) stalls stop here. That matches the pre-existing rule-teardown
// assumption (teardown unblocks blocked Broadcasts); the dispatcher
// only narrows the blast radius from "recovery stalls forever" to
// "stop waits for teardown to keep its promise".
func (meta *Meta) stop(ctx api.StreamContext) {
	meta.stopOnce.Do(func() {
		meta.lifecycleCancel()
		<-meta.done
		// Hard invariant 2: the recovery worker belongs to this
		// lifecycle. It closes recoveryDone on exit, so joining it
		// here means a Recover can never run concurrently with the
		// final Close below. Nil when no worker was started.
		if meta.recoveryDone != nil {
			<-meta.recoveryDone
		}
		meta.drainEvents()
		// Safe without the cw lock: the worker wrote conn before
		// closing readCh and done in the same goroutine, so the
		// receive above happens after that write. The object is
		// always non-nil here unless the worker panicked before
		// publishing, in which case there is nothing to close.
		if conn := meta.cw.conn; conn != nil {
			_ = conn.Close(ctx)
		}
		meta.drainEvents()
		meta.eventMu.Lock()
		meta.dispatcherStopping = true
		meta.eventMu.Unlock()
		select {
		case meta.eventWake <- struct{}{}:
		default:
		}
		<-meta.dispatcherDone
	})
}
