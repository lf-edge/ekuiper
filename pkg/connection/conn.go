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
	"sync/atomic"

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
	select {
	case <-connectorCtx.Done():
		return nil, connectorCtx.Err()
	case <-cw.meta.lifecycleCtx.Done():
		return nil, ErrConnectionClosed
	case <-cw.readCh:
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
		// Recheck: the lifecycle may have ended between ready and
		// return; never hand out a connection of a dead scope.
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

// newMeta builds a Meta whose lifecycle derives from the owning Manager.
// Caller ctx only decides whether the current API call keeps waiting;
// it never parents the Meta worker.
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
	cw    *ConnWrapper                       `json:"-"`
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
	status    atomic.Value `json:"-"`
	lastError atomic.Value `json:"-"`
}

func (meta *Meta) NotifyStatus(status string, s string) {
	meta.status.Store(status)
	if s != "" {
		meta.lastError.Store(s)
	}
	// Snapshot handlers before invoking so the invocation does not run
	// while holding the refs lock. Invocation stays synchronous: a slow
	// consumer callback can still delay the connection worker; isolating
	// that (dispatcher/queue) is A2 work, not attempted here.
	meta.refMu.Lock()
	handlers := make([]api.StatusChangeHandler, 0, len(meta.refs))
	for _, sc := range meta.refs {
		handlers = append(handlers, sc)
	}
	meta.refMu.Unlock()
	for _, sch := range handlers {
		if sch != nil {
			sch(status, s)
		}
	}
}

func (meta *Meta) AddRef(refId string, sc api.StatusChangeHandler) {
	// Baseline ordering, kept deliberately: snapshot and deliver the
	// initial status BEFORE registering the handler. Register-then-deliver
	// would let a concurrent broadcast reach the handler first and then be
	// overwritten by the older snapshot (new-then-old inversion).
	// Fully lock-free ordered delivery needs a serialized subscription
	// mechanism (A2); until then the initial callback runs here, under
	// the Manager lock held by the fetch path.
	s, e := meta.GetStatus()
	if sc != nil {
		sc(s, e)
	}
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

func (meta *Meta) GetStatus() (s string, e string) {
	ee := meta.lastError.Load()
	if ee != nil {
		e = ee.(string)
	}
	ss := meta.status.Load()
	if ss != nil {
		s = ss.(string)
		if s == api.ConnectionConnected {
			if meta.cw.IsInitialized() {
				conn, err := meta.cw.Wait(topoContext.Background())
				if err != nil || conn == nil {
					return
				}
				e = ""
				// if connected, cw, cw.conn should exist
				if _, isStateful := conn.(modules.StatefulDialer); !isStateful {
					err := conn.Ping(topoContext.Background())
					if err != nil {
						s = api.ConnectionDisconnected
						e = err.Error()
					}
				}
			}
		}
		return
	} else {
		s = api.ConnectionConnecting
		return
	}
}
