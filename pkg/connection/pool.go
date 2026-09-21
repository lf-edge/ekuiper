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
	"maps"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

// Connection pool manages all connections in the system. There are two kinds of connections:
// 1. Named connection: Long running connection. Users can create it standalone through dedicated API without rules.
// The connection will run through all the eKuiper server lifecycle. When restarting, it will be loaded and run as server init.
// 2. Anonymous connection: It is a subsidiary of rules. The rule source/sink defines connection and the connection will
// be fetched when rules start. If no rule has accessed it, it will be closed and dropped.

type Manager struct {
	syncx.RWMutex
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
	conf.IsTesting = true
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

func NewExponentialBackOffWithMaxElapsedTime(maxElapsedTime time.Duration) *backoff.ExponentialBackOff {
	return newExponentialBackOff(maxElapsedTime)
}

func newExponentialBackOff(maxElapsedTime time.Duration) *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultInitialInterval),
		backoff.WithMaxInterval(DefaultMaxInterval),
		backoff.WithMaxElapsedTime(maxElapsedTime),
	)
}

// FetchOptions carries the explicit connection identity for the new fetch
// path (DESIGN §8, A1a). ConnectionKey selects which logical connection to
// share; RefID identifies which consumer holds it. The two must never be
// mixed: Pool never derives one from the other.
type FetchOptions struct {
	// ConnectionKey is the opaque logical-connection identity. For named
	// connections it is the connectionSelector; for anonymous ones it is
	// the connector-provided canonical key (e.g. resolved SQL DB URL).
	ConnectionKey string
	// RefID identifies the holding consumer. Source/Sink use the
	// rule+op+instance identity; lookup tables use the framework-injected
	// lookup resource identity. It must be saved by the consumer at
	// Connect time and passed back verbatim at detach time.
	RefID string
	// RequireExisting marks a fetch that may only attach to an already
	// existing logical connection: a missing ConnectionKey is an error
	// and anonymous creation is never performed. Used for
	// connectionSelector/named references. False allows creating an
	// anonymous Meta when the key is absent. This describes the fetch
	// mode, not the Meta lifecycle type (Meta.Named).
	RequireExisting bool
	Type            string
	Props           map[string]any
	// StatusHandler receives connection status changes for this ref.
	StatusHandler api.StatusChangeHandler
}

// ConsumerRefID derives the stable Source/Sink consumer identity
// (ruleID + opID + instanceID) for the current context. Connect must save
// the returned value and pass it back verbatim to DetachConnectionByRef.
func ConsumerRefID(ctx api.StreamContext) string {
	return extractRefId(ctx)
}

// FetchConnectionWithOptions is the explicit-identity fetch path. Callers
// must canonicalize props/key and compute RefID before calling; Pool treats
// ConnectionKey as an opaque identity.
func FetchConnectionWithOptions(ctx api.StreamContext, opts FetchOptions) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if opts.ConnectionKey == "" {
		return nil, fmt.Errorf("connection key should be defined")
	}
	if opts.RefID == "" {
		return nil, fmt.Errorf("connection ref id should be defined")
	}
	if opts.Type == "" {
		return nil, fmt.Errorf("connection type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	return fetchInternal(ctx, opts)
}

// FetchConnection is the legacy compatibility shim. It keeps the historical
// connectionKey derivation (connectionSelector-or-refId) so unmigrated
// connectors keep resolving the same logical connection. The consumer ref,
// however, is normalized to ConsumerRefID(ctx): legacy DetachConnection
// derives exactly that value, and the historical habit of passing
// connection-identity material (endpoint/topic/URL) as refId never
// identified the consumer. Without this normalization every legacy Close
// would miss its ref and leak the reference. New code must use
// FetchConnectionWithOptions instead.
func FetchConnection(ctx api.StreamContext, refId, typ string, props map[string]interface{}, sc api.StatusChangeHandler) (*ConnWrapper, error) {
	failpoint.Inject("FetchConnectionErr", func() {
		failpoint.Return(nil, fmt.Errorf("FetchConnectionErr"))
	})
	if refId == "" {
		return nil, fmt.Errorf("connection ref id should be defined")
	}
	conId := extractSelID(props, refId)
	opts := FetchOptions{
		ConnectionKey:   conId,
		RefID:           ConsumerRefID(ctx),
		RequireExisting: conId != refId,
		Type:            typ,
		Props:           props,
		StatusHandler:   sc,
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	return fetchInternal(ctx, opts)
}

// fetchInternal implements lookup-or-create plus attach. Callers must hold
// the Manager lock. A1a introduces no new plugin/network work here, but the
// attach path still reaches the preexisting Meta.GetStatus behavior (which
// may Ping for stateless connections); that is normalized in A2. The only
// local addition is the synchronous compatibility comparison.
func fetchInternal(ctx api.StreamContext, opts FetchOptions) (*ConnWrapper, error) {
	conId := opts.ConnectionKey
	if meta, ok := globalConnectionManager.connectionPool[conId]; ok {
		if err := checkCompatible(meta, opts); err != nil {
			return nil, err
		}
		conf.Log.Infof("FetchConnection return existed conn %s", conId)
		if conId != opts.RefID {
			conf.Log.Infof("action=reuse_connection connId=%s type=%s connectionKey=%s rule=%s op=%s refId=%s", conId, opts.Type, conId, ctx.GetRuleId(), ctx.GetOpId(), opts.RefID)
		}
		return attachConnection(conId, opts.RefID, opts.StatusHandler)
	}
	if opts.RequireExisting {
		return nil, fmt.Errorf("connection %s not existed", conId)
	}
	meta := &Meta{
		ID:  conId,
		Typ: opts.Type,
		// Shallow-copy the caller map: Meta.Props is immutable once
		// inside the Pool. maps.Clone(nil) is nil, so no nil guard
		// needed. Connectors requiring deep-copy semantics must
		// normalize before fetching.
		Props: maps.Clone(opts.Props),
		Named: false,
	}
	meta.cw = newConnWrapper(ctx, meta)
	globalConnectionManager.connectionPool[meta.ID] = meta
	conf.Log.Infof("FetchConnection return new conn %s", conId)
	return attachConnection(conId, opts.RefID, opts.StatusHandler)
}

// checkCompatible rejects sharing one logical connection between
// incompatible definitions. The comparison is strictly local: identity
// material already normalized by the caller, no plugin calls, no I/O.
func checkCompatible(meta *Meta, opts FetchOptions) error {
	if !strings.EqualFold(meta.Typ, opts.Type) {
		return fmt.Errorf("connection %s type conflict: pooled %q vs requested %q", meta.ID, meta.Typ, opts.Type)
	}
	return nil
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
	return DetachConnectionByRef(ctx, conId, extractRefId(ctx))
}

// DetachConnectionByRef detaches a connection using the reference ID supplied
// to FetchConnection.
func DetachConnectionByRef(ctx api.StreamContext, conId, refId string) error {
	if conId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	if refId == "" {
		return fmt.Errorf("connection reference id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	return detachConnection(ctx, conId, refId)
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
	if conId != refId {
		conf.Log.Infof("action=attach_connection_ref connId=%s type=%s connectionKey=%s refId=%s refCount=%d", conId, meta.Typ, conId, refId, meta.GetRefCount())
	}
	return meta.cw, nil
}

func detachConnection(ctx api.StreamContext, conId, refId string) error {
	meta, ok := globalConnectionManager.connectionPool[conId]
	if !ok {
		conf.Log.Infof("detachConnection not found:%v", conId)
		return nil
	}
	meta.DeRef(refId)
	globalConnectionManager.connectionPool[conId] = meta
	conf.Log.Infof("detachConnection remove conn:%v,ref:%v", conId, refId)
	if conId != refId {
		conf.Log.Infof("action=detach_connection_ref connId=%s type=%s connectionKey=%s rule=%s op=%s refId=%s refCount=%d", conId, meta.Typ, conId, ctx.GetRuleId(), ctx.GetOpId(), refId, meta.GetRefCount())
	}
	if !meta.Named && meta.GetRefCount() == 0 {
		if conId != refId {
			conf.Log.Infof("action=close_connection connId=%s type=%s connectionKey=%s rule=%s op=%s reason=zero_ref", conId, meta.Typ, conId, ctx.GetRuleId(), ctx.GetOpId())
		}
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
	connRegister, ok := modules.GetConnectionProvider(strings.ToLower(meta.Typ))
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
		// No max elapsed time: pooled connections keep retrying until their
		// lifecycle context ends. Consumers decide whether and when to wait
		// for the ConnWrapper to become ready.
	}, NewExponentialBackOffWithMaxElapsedTime(0))
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
