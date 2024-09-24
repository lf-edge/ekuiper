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
	"sync"
	"sync/atomic"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type ConnWrapper struct {
	ID          string
	initialized bool
	conn        modules.Connection
	err         error
	cond        *sync.Cond
}

func (cw *ConnWrapper) SetConn(conn modules.Connection, err error) {
	cw.cond.L.Lock()
	defer cw.cond.L.Unlock()
	cw.initialized = true
	cw.conn, cw.err = conn, err
}

func (cw *ConnWrapper) Wait() (modules.Connection, error) {
	cw.cond.L.Lock()
	defer cw.cond.L.Unlock()
	for !cw.initialized {
		cw.cond.Wait()
	}
	return cw.conn, cw.err
}

func (cw *ConnWrapper) IsInitialized() bool {
	cw.cond.L.Lock()
	defer cw.cond.L.Unlock()
	return cw.initialized
}

func newConnWrapper(ctx api.StreamContext, meta *Meta) *ConnWrapper {
	cw := &ConnWrapper{
		ID:   meta.ID,
		cond: sync.NewCond(&sync.Mutex{}),
	}
	go func() {
		conn, err := createConnection(ctx, meta)
		cw.SetConn(conn, err)
		cw.cond.Broadcast()
	}()
	return cw
}

type Meta struct {
	ID    string         `json:"id"`
	Typ   string         `json:"typ"`
	Props map[string]any `json:"props"`

	Named    bool         `json:"-"`
	refCount atomic.Int32 `json:"-"`
	ref      sync.Map     `json:"-"`
	cw       *ConnWrapper `json:"-"`
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
	meta.ref.Range(func(refId, sc any) bool {
		sch := sc.(api.StatusChangeHandler)
		if sch != nil {
			sch(status, s)
		}
		return true
	})
}

func (meta *Meta) GetRefCount() int {
	return int(meta.refCount.Load())
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
			e = ""
			// if connected, cw, cw.conn should exist
			if _, isStateful := meta.cw.conn.(modules.StatefulDialer); !isStateful {
				err := meta.cw.conn.Ping(context.Background())
				if err != nil {
					s = api.ConnectionDisconnected
					e = err.Error()
				}
			}
		}
		return
	} else {
		s = api.ConnectionConnecting
		return
	}
}
