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
	InitConnectionManager(context.Background())
	return nil
}

func InitConnectionManager(ctx context.Context) {
	globalConnectionManager = &Manager{
		connectionPool: make(map[string]*Meta),
	}
	if conf.IsTesting {
		return
	}
	go PatrolConnectionStatusJob(ctx)
}

const (
	DefaultInitialInterval = 100 * time.Millisecond
	DefaultMaxInterval     = 10 * time.Second
)

func PatrolConnectionStatusJob(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			patrolConnectionStatus()
		}
	}
}

func patrolConnectionStatus() {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	for connName, conn := range globalConnectionManager.connectionPool {
		// For now, we only patrol named connection
		if !conn.Named {
			continue
		}
		status, _ := conn.GetStatus()
		switch status {
		case api.ConnectionConnected:
			ConnStatusGauge.WithLabelValues(connName).Set(1)
		case api.ConnectionDisconnected:
			ConnStatusGauge.WithLabelValues(connName).Set(-1)
		case api.ConnectionConnecting:
			ConnStatusGauge.WithLabelValues(connName).Set(0)
		}
	}
}

func NewExponentialBackOff() *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultInitialInterval),
		backoff.WithMaxInterval(DefaultMaxInterval),
		backoff.WithMaxElapsedTime(0),
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
	conId := extractSelID(props, refId)
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	if _, ok := globalConnectionManager.connectionPool[conId]; ok {
		conf.Log.Infof("FetchConnection return existed conn %s", conId)
	} else {
		if conId != refId {
			return nil, fmt.Errorf("connection %s not existed", conId)
		}
		meta := &Meta{
			ID:    conId,
			Typ:   typ,
			Props: props,
			Named: false,
		}
		meta.cw = newConnWrapper(ctx, meta)
		globalConnectionManager.connectionPool[meta.ID] = meta
		conf.Log.Infof("FetchConnection return new conn %s", conId)
	}
	return attachConnection(conId, refId, sc)
}

// ReloadNamedConnection is called when server starts. It initializes all stored named connections
func ReloadNamedConnection() error {
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
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
		if _, ok := globalConnectionManager.connectionPool[id]; ok {
			continue
		}
		meta := &Meta{
			ID:    id,
			Typ:   typ,
			Props: props,
			Named: true,
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
	return createNamedConnection(ctx, id, typ, props)
}

func createNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if _, ok := globalConnectionManager.connectionPool[id]; ok {
		return nil, fmt.Errorf("connection %v already been created", id)
	}
	meta := &Meta{
		ID:    id,
		Typ:   typ,
		Props: props,
		Named: true,
	}
	meta.cw = newConnWrapper(ctx, meta)
	if err := storeConnectionMeta(typ, id, props); err != nil {
		return nil, err
	}
	globalConnectionManager.connectionPool[id] = meta
	return meta.cw, nil
}

func GetAllConnectionsMeta(forceAll bool) []*Meta {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	metaList := make([]*Meta, 0)
	for _, meta := range globalConnectionManager.connectionPool {
		if !meta.Named && !forceAll {
			continue
		}
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

func DropNameConnection(ctx api.StreamContext, selId string) error {
	if selId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	return dropNameConnection(ctx, selId)
}

func dropNameConnection(ctx api.StreamContext, selId string) error {
	meta, ok := globalConnectionManager.connectionPool[selId]
	if !ok {
		return nil
	}
	isInternal, err := isInternalConnection(selId)
	if err != nil {
		return err
	}
	if isInternal {
		return fmt.Errorf("internal connection %v can't be edit", selId)
	}
	if meta.GetRefCount() > 0 {
		return fmt.Errorf("connection %s can't be dropped due to rule references %v", selId, meta.GetRefNames())
	}
	err = dropConnectionStore(meta.Typ, selId)
	if err != nil {
		return fmt.Errorf("drop connection %s failed, err:%v", selId, err)
	}
	if meta.cw.IsInitialized() {
		conn, err := meta.cw.Wait(ctx)
		if conn != nil && err == nil {
			conn.Close(ctx)
		}
	}
	delete(globalConnectionManager.connectionPool, selId)
	return nil
}

func UpdateConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	isInternal, err := isInternalConnection(id)
	if err != nil {
		return nil, err
	}
	if isInternal {
		return nil, fmt.Errorf("internal connection %v can't be edit", id)
	}
	if err := dropNameConnection(ctx, id); err != nil {
		return nil, err
	}
	return createNamedConnection(ctx, id, typ, props)
}

func isInternalConnection(id string) (bool, error) {
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return false, fmt.Errorf("connection %s not existed", id)
	}
	return !meta.Named, nil
}

func DetachConnection(ctx api.StreamContext, conId string) error {
	if conId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	return detachConnection(ctx, conId)
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

func attachConnection(conId string, refId string, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	if conId == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	meta, ok := globalConnectionManager.connectionPool[conId]
	if !ok {
		return nil, fmt.Errorf("connection %s not existed", conId)
	}
	meta.AddRef(refId, sc)
	return meta.cw, nil
}

func detachConnection(ctx api.StreamContext, conId string) error {
	meta, ok := globalConnectionManager.connectionPool[conId]
	if !ok {
		conf.Log.Infof("detachConnection not found:%v", conId)
		return nil
	}
	refId := extractRefId(ctx)
	meta.DeRef(refId)
	globalConnectionManager.connectionPool[conId] = meta
	conf.Log.Infof("detachConnection remove conn:%v,ref:%v", conId, refId)
	if !meta.Named && meta.GetRefCount() == 0 {
		close(meta.cw.detachCh)
		conn, err := meta.cw.Wait(ctx)
		if conn != nil && err == nil {
			conn.Close(ctx)
		}
		delete(globalConnectionManager.connectionPool, conId)
		return nil
	}
	return nil
}

func createConnection(connCtx api.StreamContext, meta *Meta) (modules.Connection, error) {
	var conn modules.Connection
	var err error
	connRegister, ok := modules.ConnectionRegister[strings.ToLower(meta.Typ)]
	if !ok {
		return nil, fmt.Errorf("unknown connection type")
	}
	conn = connRegister(connCtx)
	sc, isStateful := conn.(modules.StatefulDialer)
	err = conn.Provision(connCtx, meta.ID, meta.Props)
	if err != nil {
		return nil, err
	}
	if isStateful {
		sc.SetStatusChangeHandler(connCtx, meta.NotifyStatus)
	}
	err = backoff.Retry(func() error {
		select {
		case <-connCtx.Done():
			return nil
		default:
		}
		meta.NotifyStatus(api.ConnectionConnecting, "")
		connCtx.GetLogger().Debugf("connection retry: %s", meta.ID)
		err = conn.Dial(connCtx)
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
		connCtx.GetLogger().Debugf("connection failed: %s, %v", meta.ID, err)
		meta.NotifyStatus(api.ConnectionDisconnected, err.Error())
		if errorx.IsIOError(err) {
			return err
		}
		return backoff.Permanent(err)
	}, NewExponentialBackOff())
	return conn, err
}

// Return the unique connection id and whether it is set explicitly
func extractSelID(props map[string]interface{}, anomId string) string {
	if len(props) < 1 {
		return anomId
	}
	v, ok := props["connectionSelector"]
	if !ok {
		return anomId
	}
	id, ok := v.(string)
	if !ok {
		return anomId
	}
	return id
}

func extractRefId(ctx api.StreamContext) string {
	return fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}
