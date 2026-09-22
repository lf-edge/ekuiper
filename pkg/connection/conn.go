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
	"sync"
	"sync/atomic"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
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
	detachCh    chan struct{}
}

func (cw *ConnWrapper) setConn(conn modules.Connection, err error) {
	cw.l.Lock()
	defer cw.l.Unlock()
	cw.initialized = true
	cw.conn, cw.err = conn, err
}

// Wait will wait for connection connected or the caller interrupts (like rule exit)
func (cw *ConnWrapper) Wait(connectorCtx api.StreamContext) (modules.Connection, error) {
	select {
	case <-connectorCtx.Done():
		connectorCtx.GetLogger().Infof("stop waiting connection")
	case <-cw.readCh:
	case <-cw.detachCh:
	}
	cw.l.RLock()
	defer cw.l.RUnlock()
	return cw.conn, cw.err
}

func (cw *ConnWrapper) IsInitialized() bool {
	cw.l.RLock()
	defer cw.l.RUnlock()
	return cw.initialized
}

func newConnWrapper(ctx api.StreamContext, meta *Meta) *ConnWrapper {
	cw := &ConnWrapper{
		ID:       meta.ID,
		readCh:   make(chan struct{}),
		detachCh: make(chan struct{}),
	}
	go func() {
		conn, err := createConnection(ctx, meta)
		cw.setConn(conn, err)
		close(cw.readCh)
	}()
	return cw
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

func (meta *Meta) DeRef(refId string) {
	meta.refMu.Lock()
	if _, ok := meta.refs[refId]; !ok {
		count := len(meta.refs)
		meta.refMu.Unlock()
		conf.Log.Warnf("conn %s dereference missing %s, refs stay %d", meta.ID, refId, count)
		return
	}
	delete(meta.refs, refId)
	count := len(meta.refs)
	meta.refMu.Unlock()
	conf.Log.Infof("conn %s dereference %s to %d refs", meta.ID, refId, count)
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
				conn, err := meta.cw.Wait(context.Background())
				if err != nil || conn == nil {
					return
				}
				e = ""
				// if connected, cw, cw.conn should exist
				if _, isStateful := conn.(modules.StatefulDialer); !isStateful {
					err := conn.Ping(context.Background())
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
