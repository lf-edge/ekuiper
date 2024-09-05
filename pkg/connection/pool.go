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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// Connection pool manages all connections in the system. There are two kinds of connections:
// 1. Named connection: Long running connection. Users can create it standalone through dedicated API without rules.
// The connection will run through all the eKuiper server lifecycle. When restarting, it will be loaded and run as server init.
// 2. Anonymous connection: It is a subsidiary of rules. The rule source/sink defines connection and the connection will
// be fetched when rules start. If no rule has accessed it, it will be closed and dropped.

type Manager struct {
	sync.RWMutex
	// key is selId(explicitly specified or anonymous)
	connectionPool map[string]*Meta
}

var (
	globalConnectionManager *Manager
	mockErr                 = true
)

func init() {
	globalConnectionManager = &Manager{
		connectionPool: make(map[string]*Meta),
	}
}

func InitConnectionManager4Test() error {
	InitMockTest()
	InitConnectionManager()
	return nil
}

func InitConnectionManager() {
	globalConnectionManager = &Manager{
		connectionPool: make(map[string]*Meta),
	}
	if conf.IsTesting {
		return
	}
	DefaultBackoffMaxElapsedDuration = time.Duration(conf.Config.Connection.BackoffMaxElapsedDuration)
}

const (
	DefaultInitialInterval = 100 * time.Millisecond
	DefaultMaxInterval     = 1 * time.Second
)

var DefaultBackoffMaxElapsedDuration = 100 * time.Minute

func NewExponentialBackOff() *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultInitialInterval),
		backoff.WithMaxInterval(DefaultMaxInterval),
		backoff.WithMaxElapsedTime(DefaultBackoffMaxElapsedDuration),
	)
}

// FetchConnection is called by source/sink to get or create an anonymous connection instance in the pool
func FetchConnection(ctx api.StreamContext, refId, typ string, props map[string]interface{}, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if refId == "" {
		return nil, fmt.Errorf("connection ref id should be defined")
	}
	selID, _ := extractSelID(props, refId)
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	if _, ok := globalConnectionManager.connectionPool[selID]; ok {
		conf.Log.Infof("FetchConnection return existed conn %s", selID)
	} else {
		meta := &Meta{
			ID:    selID,
			Typ:   typ,
			Props: props,
		}
		meta.cw = newConnWrapper(ctx, meta)
		globalConnectionManager.connectionPool[meta.ID] = meta
		conf.Log.Infof("FetchConnection return new conn %s", selID)
	}
	return attachConnection(selID, refId, sc)
}

// ReloadNamedConnection is called when server starts. It initializes all stored named connections
func ReloadNamedConnection() error {
	cfgs, err := conf.GetCfgFromKVStorage("connections", "", "")
	if err != nil {
		return err
	}
	for key, props := range cfgs {
		names := strings.Split(key, ".")
		if len(names) != 3 {
			continue
		}
		typ := names[1]
		id := names[2]
		meta := &Meta{
			ID:    id,
			Typ:   typ,
			Props: props,
		}
		meta.cw = newConnWrapper(topoContext.WithContext(context.Background()), meta)
		globalConnectionManager.connectionPool[id] = meta
	}
	return nil
}

// Connection API handlers

func CreateNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	if _, ok := globalConnectionManager.connectionPool[id]; ok {
		return nil, fmt.Errorf("connection %v already been created", id)
	}
	meta := &Meta{
		ID:    id,
		Typ:   typ,
		Props: props,
	}
	meta.cw = newConnWrapper(ctx, meta)
	if err := storeConnectionMeta(typ, id, props); err != nil {
		return nil, err
	}
	globalConnectionManager.connectionPool[id] = meta
	return meta.cw, nil
}

func GetAllConnectionStatus(ctx api.StreamContext) map[string]modules.ConnectionStatus {
	cws := make(map[string]*Meta)
	globalConnectionManager.RLock()
	for id, meta := range globalConnectionManager.connectionPool {
		cws[id] = meta
	}
	globalConnectionManager.RUnlock()
	s := make(map[string]modules.ConnectionStatus)
	for id, meta := range cws {
		switch mm := meta.cw.conn.(type) {
		case modules.StatefulDialer:
			s[id] = mm.Status(ctx)
		default:
			if meta.cw.IsInitialized() {
				conn, err := meta.cw.Wait()
				if err != nil {
					s[id] = modules.ConnectionStatus{
						Status: api.ConnectionDisconnected,
						ErrMsg: err.Error(),
					}
					continue
				}
				err = conn.Ping(ctx)
				if err != nil {
					s[id] = modules.ConnectionStatus{
						Status: api.ConnectionDisconnected,
						ErrMsg: err.Error(),
					}
					continue
				}
				s[id] = modules.ConnectionStatus{
					Status: api.ConnectionConnected,
				}
			} else {
				s[id] = modules.ConnectionStatus{
					Status: api.ConnectionConnecting,
				}
			}
		}
	}
	return s
}

func GetAllConnectionsMeta() []*Meta {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	metaList := make([]*Meta, 0)
	for _, meta := range globalConnectionManager.connectionPool {
		metaList = append(metaList, meta)
	}
	return metaList
}

func GetConnectionDetail(_ api.StreamContext, id string) (*Meta, error) {
	if id == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return nil, fmt.Errorf("connection %s not existed", id)
	}
	return meta, nil
}

func PingConnection(ctx api.StreamContext, id string) error {
	if id == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return fmt.Errorf("connection %s not existed", id)
	}
	conn, err := meta.cw.Wait()
	if err != nil {
		return err
	}
	return conn.Ping(ctx)
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
	if meta.refCount.Load() > 0 {
		return fmt.Errorf("connection %s can't be dropped due to reference", selId)
	}
	err := dropConnectionStore(meta.Typ, selId)
	if err != nil {
		return fmt.Errorf("drop connection %s failed, err:%v", selId, err)
	}
	conn, err := meta.cw.Wait()
	if conn != nil && err == nil {
		conn.Close(ctx)
	}
	delete(globalConnectionManager.connectionPool, selId)
	return nil
}

func DetachConnection(ctx api.StreamContext, id string, props map[string]interface{}) error {
	if id == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	selID, defined := extractSelID(props, id)
	return detachConnection(ctx, selID, !defined)
}

func getConnectionRef(id string) int {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return 0
	}
	return meta.GetRefCount()
}

func storeConnectionMeta(plugin, id string, props map[string]interface{}) error {
	err := conf.WriteCfgIntoKVStorage("connections", plugin, id, props)
	failpoint.Inject("storeConnectionErr", func() {
		err = errors.New("storeConnectionErr")
	})
	return err
}

func dropConnectionStore(plugin, id string) error {
	err := conf.DropCfgKeyFromStorage("connections", plugin, id)
	failpoint.Inject("dropConnectionStoreErr", func() {
		err = errors.New("dropConnectionStoreErr")
	})
	return err
}

func attachConnection(id string, refId string, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	if id == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return nil, fmt.Errorf("connection %s not existed", id)
	}
	meta.ref.Store(refId, sc)
	meta.refCount.Add(1)
	return meta.cw, nil
}

func detachConnection(ctx api.StreamContext, id string, remove bool) error {
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return nil
	}
	refId := extractRefId(ctx)
	meta.ref.Delete(refId)
	meta.refCount.Add(-1)
	globalConnectionManager.connectionPool[id] = meta
	conf.Log.Infof("detachConnection remove conn:%v,ref:%v", id, refId)
	if remove && (meta.refCount.Load() == 0) {
		conn, err := meta.cw.Wait()
		if conn != nil && err == nil {
			conn.Close(ctx)
		}
		delete(globalConnectionManager.connectionPool, id)
		return nil
	}
	return nil
}

func createConnection(ctx api.StreamContext, meta *Meta) (modules.Connection, error) {
	var conn modules.Connection
	var err error
	connRegister, ok := modules.ConnectionRegister[strings.ToLower(meta.Typ)]
	if !ok {
		return nil, fmt.Errorf("unknown connection type")
	}
	conn = connRegister(ctx)
	sc, isStateful := conn.(modules.StatefulDialer)
	err = conn.Provision(ctx, meta.Props)
	if err != nil {
		return nil, err
	}
	if isStateful {
		sc.SetStatusChangeHandler(ctx, meta.NotifyStatus)
	}
	err = backoff.Retry(func() error {
		select {
		case <-ctx.Done():
			return backoff.Permanent(errors.New("connection fail: exit retrying because rule/server stops"))
		default:
		}
		if !isStateful {
			meta.NotifyStatus(api.ConnectionConnecting, "")
		}
		err = conn.Dial(ctx)
		failpoint.Inject("createConnectionErr", func() {
			if mockErr {
				err = errorx.NewIOErr("createConnectionErr")
				mockErr = false
			}
		})
		if err == nil {
			if !isStateful {
				meta.NotifyStatus(api.ConnectionConnected, "")
			}
			return nil
		}
		if !isStateful {
			meta.NotifyStatus(api.ConnectionDisconnected, err.Error())
		}
		if errorx.IsIOError(err) {
			return err
		}
		return backoff.Permanent(err)
	}, NewExponentialBackOff())
	return conn, err
}

// Return the unique connection id and whether it is set explicitly
func extractSelID(props map[string]interface{}, anomId string) (string, bool) {
	if len(props) < 1 {
		return anomId, false
	}
	v, ok := props["connectionSelector"]
	if !ok {
		return anomId, false
	}
	id, ok := v.(string)
	if !ok {
		return anomId, false
	}
	return id, true
}

func extractRefId(ctx api.StreamContext) string {
	return fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}
