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
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type RegisterConnection func(ctx api.StreamContext, id string, props map[string]any) (Connection, error)

var ConnectionRegister map[string]RegisterConnection

func init() {
	ConnectionRegister = map[string]RegisterConnection{}
	ConnectionRegister["mock"] = createMockConnection
}

var isTest bool

type Connection interface {
	Ping(ctx api.StreamContext) error
	Close(ctx api.StreamContext)
	Attach(ctx api.StreamContext)
	DetachSub(ctx api.StreamContext, props map[string]any)
	DetachPub(ctx api.StreamContext, props map[string]any)
	Ref(ctx api.StreamContext) int
}

func GetAllConnectionsID() []string {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	ids := make([]string, 0)
	for key := range globalConnectionManager.connectionPool {
		ids = append(ids, key)
	}
	return ids
}

func PingConnection(ctx api.StreamContext, id string) error {
	conn, err := GetNameConnection(id)
	if err != nil {
		return err
	}
	return conn.Ping(ctx)
}

func GetNameConnection(selId string) (Connection, error) {
	if selId == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	meta, ok := globalConnectionManager.connectionPool[selId]
	if !ok {
		return nil, fmt.Errorf("connection %s not existed", selId)
	}
	return meta.conn, nil
}

func CreateNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (Connection, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	_, ok := globalConnectionManager.connectionPool[id]
	if ok {
		return nil, fmt.Errorf("connection %v already been created", id)
	}
	meta := ConnectionMeta{
		ID:    id,
		Typ:   typ,
		Props: props,
	}
	if !isTest {
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}
		if err := globalConnectionManager.store.Set(id, string(b)); err != nil {
			return nil, err
		}
	}
	conn, err := createNamedConnection(ctx, meta)
	if err != nil {
		return nil, err
	}
	meta.conn = conn
	globalConnectionManager.connectionPool[id] = meta
	return conn, nil
}

func CreateNonStoredConnection(ctx api.StreamContext, id, typ string, props map[string]any) (Connection, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	_, ok := globalConnectionManager.connectionPool[id]
	if ok {
		return nil, fmt.Errorf("connection %v already been created", id)
	}
	meta := ConnectionMeta{
		ID:    id,
		Typ:   typ,
		Props: props,
	}
	conn, err := createNamedConnection(ctx, meta)
	if err != nil {
		return nil, err
	}
	meta.conn = conn
	globalConnectionManager.connectionPool[id] = meta
	return conn, nil
}

func createNamedConnection(ctx api.StreamContext, meta ConnectionMeta) (Connection, error) {
	var conn Connection
	var err error
	connRegister, ok := ConnectionRegister[strings.ToLower(meta.Typ)]
	if !ok {
		return nil, fmt.Errorf("unknown connection type")
	}
	conn, err = connRegister(ctx, meta.ID, meta.Props)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func DropNameConnection(ctx api.StreamContext, selId string) error {
	if selId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	meta, ok := globalConnectionManager.connectionPool[selId]
	if !ok {
		return nil
	}
	conn := meta.conn
	if conn.Ref(ctx) > 0 {
		return fmt.Errorf("connection %s can't be dropped due to reference", selId)
	}
	if !isTest {
		err := globalConnectionManager.store.Delete(selId)
		if err != nil {
			return fmt.Errorf("drop connection %s failed, err:%v", selId, err)
		}
	}
	conn.Close(ctx)
	delete(globalConnectionManager.connectionPool, selId)
	return nil
}

var globalConnectionManager *ConnectionManager

func InitConnectionManagerInTest() {
	isTest = true
	InitConnectionManager()
}

func InitConnectionManager() error {
	globalConnectionManager = &ConnectionManager{
		connectionPool: make(map[string]ConnectionMeta),
	}
	if !isTest {
		globalConnectionManager.store, _ = store.GetKV("connectionMeta")
		kvs, _ := globalConnectionManager.store.All()
		for connectionID, raw := range kvs {
			meta := ConnectionMeta{}
			err := json.Unmarshal([]byte(raw), &meta)
			if err != nil {
				return fmt.Errorf("initialize connection:%v failed, err:%v", connectionID, err)
			}
			conn, err := createNamedConnection(context.Background(), meta)
			if err != nil {
				return fmt.Errorf("initialize connection:%v failed, err:%v", connectionID, err)
			}
			meta.conn = conn
			globalConnectionManager.connectionPool[connectionID] = meta
		}
	}
	return nil
}

type ConnectionManager struct {
	sync.RWMutex
	store          kv.KeyValue
	connectionPool map[string]ConnectionMeta
}

type ConnectionMeta struct {
	ID    string         `json:"id"`
	Typ   string         `json:"typ"`
	Props map[string]any `json:"props"`
	conn  Connection     `json:"-"`
}

type mockConnection struct {
	id  string
	ref int
}

func (m *mockConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (m *mockConnection) Close(ctx api.StreamContext) {
	return
}

func (m *mockConnection) Attach(ctx api.StreamContext) {
	m.ref++
	return
}

func (m *mockConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	m.ref--
	return
}

func (m *mockConnection) DetachPub(ctx api.StreamContext, props map[string]any) {
	m.ref--
	return
}

func (m *mockConnection) Ref(ctx api.StreamContext) int {
	return m.ref
}

func createMockConnection(ctx api.StreamContext, id string, props map[string]any) (Connection, error) {
	m := &mockConnection{id: id, ref: 0}
	return m, nil
}
