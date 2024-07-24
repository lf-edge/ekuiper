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
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

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

func IsConnectionExists(id string) bool {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	_, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return false
	}
	return true
}

func GetConnectionRef(id string) int {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return 0
	}
	return meta.refCount
}

func GetAllConnectionStatus(ctx api.StreamContext) map[string]ConnectionStatus {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	s := make(map[string]ConnectionStatus)
	for id, meta := range globalConnectionManager.connectionPool {
		status := ConnectionStatus{
			Status: ConnectionRunning,
		}
		conn, err := meta.cw.Wait()
		if err != nil {
			status.Status = ConnectionFail
			status.ErrMsg = err.Error()
		} else {
			err = conn.Ping(ctx)
			if err != nil {
				status.Status = ConnectionFail
				status.ErrMsg = err.Error()
			}
		}
		s[id] = status
	}
	return s
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

func FetchConnection(ctx api.StreamContext, id, typ string, props map[string]interface{}) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if id == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	selID := extractSelID(props)
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	if len(selID) < 1 {
		cw := getConnectionWrapper(id)
		if cw != nil {
			conf.Log.Infof("FetchConnection return existed conn %s", id)
			return cw, nil
		}
		meta := &ConnectionMeta{
			ID:       id,
			Typ:      typ,
			Props:    props,
			refCount: 1,
		}
		meta.cw = newConnWrapper(ctx, meta)
		globalConnectionManager.connectionPool[meta.ID] = meta
		conf.Log.Infof("FetchConnection return new conn %s", id)
		return meta.cw, nil
	}
	return attachConnection(selID)
}

func attachConnection(id string) (*ConnWrapper, error) {
	if id == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return nil, fmt.Errorf("connection %s not existed", id)
	}
	meta.refCount++
	return meta.cw, nil
}

func DetachConnection(ctx api.StreamContext, id string, props map[string]interface{}) error {
	if id == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	selID := extractSelID(props)
	if len(selID) < 1 {
		return detachConnection(ctx, id, true)
	}
	return detachConnection(ctx, selID, false)
}

func detachConnection(ctx api.StreamContext, id string, remove bool) error {
	meta, ok := globalConnectionManager.connectionPool[id]
	if !ok {
		return nil
	}
	meta.refCount--
	globalConnectionManager.connectionPool[id] = meta
	conf.Log.Infof("detachConnection remove conn:%v,ref:%v", id, meta.refCount)
	if remove && meta.refCount < 1 {
		conn, err := meta.cw.Wait()
		if conn != nil && err == nil {
			conn.Close(ctx)
		}
		delete(globalConnectionManager.connectionPool, id)
		return nil
	}
	return nil
}

func CreateNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (*ConnWrapper, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	exists := checkConn(id)
	if exists {
		return nil, fmt.Errorf("connection %v already been created", id)
	}
	meta := &ConnectionMeta{
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

func getConnectionWrapper(id string) *ConnWrapper {
	oldConn, ok := globalConnectionManager.connectionPool[id]
	if ok {
		oldConn.refCount++
		globalConnectionManager.connectionPool[id] = oldConn
		return oldConn.cw
	}
	return nil
}

func CheckConn(id string) bool {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	return checkConn(id)
}

func checkConn(id string) bool {
	_, ok := globalConnectionManager.connectionPool[id]
	return ok
}

var mockErr = true

func createNamedConnection(ctx api.StreamContext, meta *ConnectionMeta) (modules.Connection, error) {
	var conn modules.Connection
	var err error
	connRegister, ok := modules.ConnectionRegister[strings.ToLower(meta.Typ)]
	if !ok {
		return nil, fmt.Errorf("unknown connection type")
	}
	err = backoff.Retry(func() error {
		select {
		case <-ctx.Done():
			return backoff.Permanent(errors.New("timeout"))
		default:
		}
		conn, err = connRegister(ctx, meta.Props)
		failpoint.Inject("createConnectionErr", func() {
			if mockErr {
				err = errorx.NewIOErr("createConnectionErr")
				mockErr = false
			}
		})
		if err == nil {
			return nil
		}
		if errorx.IsIOError(err) {
			return err
		}
		return backoff.Permanent(err)
	}, NewExponentialBackOff())
	return conn, err
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
	if meta.refCount > 0 {
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

var globalConnectionManager *ConnectionManager

func init() {
	globalConnectionManager = &ConnectionManager{
		connectionPool: make(map[string]*ConnectionMeta),
	}
}

func InitConnectionManager4Test() error {
	InitMockTest()
	InitConnectionManager()
	return nil
}

func InitConnectionManager() {
	globalConnectionManager = &ConnectionManager{
		connectionPool: make(map[string]*ConnectionMeta),
	}
	if conf.IsTesting {
		return
	}
	DefaultBackoffMaxElapsedDuration = time.Duration(conf.Config.Connection.BackoffMaxElapsedDuration)
}

func ReloadConnection() error {
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
		meta := &ConnectionMeta{
			ID:    id,
			Typ:   typ,
			Props: props,
		}
		meta.cw = newConnWrapper(topoContext.WithContext(context.Background()), meta)
		globalConnectionManager.connectionPool[id] = meta
	}
	return nil
}

type ConnectionManager struct {
	sync.RWMutex
	connectionPool map[string]*ConnectionMeta
}

type ConnectionMeta struct {
	ID       string         `json:"id"`
	Typ      string         `json:"typ"`
	Props    map[string]any `json:"props"`
	refCount int            `json:"-"`
	cw       *ConnWrapper   `json:"-"`
}

func NewExponentialBackOff() *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultInitialInterval),
		backoff.WithMaxInterval(DefaultMaxInterval),
		backoff.WithMaxElapsedTime(DefaultBackoffMaxElapsedDuration),
	)
}

const (
	DefaultInitialInterval = 100 * time.Millisecond
	DefaultMaxInterval     = 1 * time.Second
)

var DefaultBackoffMaxElapsedDuration = 3 * time.Minute

const (
	ConnectionRunning = "running"
	ConnectionFail    = "fail"
)

type ConnectionStatus struct {
	Status string
	ErrMsg string
}

func extractSelID(props map[string]interface{}) string {
	if len(props) < 1 {
		return ""
	}
	v, ok := props["connectionSelector"]
	if !ok {
		return ""
	}
	id, ok := v.(string)
	if !ok {
		return ""
	}
	return id
}

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

func newConnWrapper(ctx api.StreamContext, meta *ConnectionMeta) *ConnWrapper {
	cw := &ConnWrapper{
		ID:   meta.ID,
		cond: sync.NewCond(&sync.Mutex{}),
	}
	go func() {
		conn, err := createNamedConnection(ctx, meta)
		cw.SetConn(conn, err)
		cw.cond.Broadcast()
	}()
	return cw
}
