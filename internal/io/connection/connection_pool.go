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
	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type Connection interface {
	Subscribe(ctx api.StreamContext, props map[string]any, ingest api.BytesIngest, ingestError api.ErrorIngest) error
	Publish(payload any, props map[string]any) error
	Ping() error
	Close()
	Attach()
	DetachSub(props map[string]any)
	DetachPub(props map[string]any)
	Ref() int
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

func CreateNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) error {
	if id == "" || typ == "" {
		return fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	_, ok := globalConnectionManager.connectionPool[id]
	if ok {
		return fmt.Errorf("connection %v already been created", id)
	}
	meta := ConnectionMeta{
		ID:    id,
		Typ:   typ,
		Props: props,
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := globalConnectionManager.store.Set(id, string(b)); err != nil {
		return err
	}
	conn, err := createNamedConnection(ctx, meta)
	if err != nil {
		return err
	}
	meta.conn = conn
	globalConnectionManager.connectionPool[id] = meta
	return nil
}

func createNamedConnection(ctx api.StreamContext, meta ConnectionMeta) (Connection, error) {
	var conn Connection
	var err error
	switch strings.ToLower(meta.Typ) {
	case "mqtt":
		conn, err = client.CreateClient(ctx, meta.ID, meta.Props)
	default:
		err = fmt.Errorf("unknown connection type")
	}
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func DropNameConnection(selId string) error {
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
	if conn.Ref() > 0 {
		return fmt.Errorf("connection %s can't be dropped due to reference", selId)
	}
	err := globalConnectionManager.store.Delete(selId)
	if err != nil {
		return fmt.Errorf("drop connection %s failed, err:%v", selId, err)
	}
	conn.Close()
	delete(globalConnectionManager.connectionPool, selId)
	return nil
}

var globalConnectionManager *ConnectionManager

func InitConnectionManager() error {
	globalConnectionManager = &ConnectionManager{
		connectionPool: make(map[string]ConnectionMeta),
	}
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
