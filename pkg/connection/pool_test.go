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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestConnection(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := context.Background()
	cw, err := CreateNamedConnection(ctx, "id1", "mock", nil)
	require.NoError(t, err)
	conn, err := cw.Wait(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.NoError(t, conn.Ping(ctx))
	require.Equal(t, 0, getConnectionRef("id1"))
	_, err = CreateNamedConnection(ctx, "id1", "mock", nil)
	require.Error(t, err)
	_, err = attachConnection("id1", "ref1", nil)
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("id1"))
	_, err = attachConnection("id1", "ref2", nil)
	require.NoError(t, err)
	require.Equal(t, 2, getConnectionRef("id1"))
	err = DetachConnectionByRef(ctx, "id1", "ref1")
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("id1"))
	err = DropNameConnection(ctx, "id1")
	require.Error(t, err)
	err = DetachConnectionByRef(ctx, "id1", "ref2")
	require.NoError(t, err)
	require.Equal(t, 0, getConnectionRef("id1"))
	err = DropNameConnection(ctx, "id1")
	require.NoError(t, err)
	err = DropNameConnection(ctx, "id1")
	require.NoError(t, err)
	conn3, err := attachConnection("id1", "ref3", nil)
	require.Error(t, err)
	require.Nil(t, conn3)

	cw, err = CreateNamedConnection(ctx, "id2", "mock", nil)
	require.NoError(t, err)
	require.NotNil(t, cw)

	cw, err = FetchConnection(ctx, "2222", "mock", map[string]interface{}{"connectionSelector": "id2"}, nil)
	require.NoError(t, err)
	require.NotNil(t, cw)

	require.Equal(t, 1, getConnectionRef("id2"))
}

func TestConnectionErr(t *testing.T) {
	var err error
	require.NoError(t, InitConnectionManager4Test())
	ctx := context.Background()

	_, err = CreateNamedConnection(ctx, "", "mock", nil)
	require.Error(t, err)
	err = DropNameConnection(ctx, "")
	require.Error(t, err)
	// Unknown connection types are static creation errors: no Meta is
	// published and no worker starts, so the error surfaces here rather
	// than from a later Wait on a doomed handle.
	_, err = CreateNamedConnection(ctx, "12", "unknown", nil)
	require.ErrorContains(t, err, "unknown connection type")
	_, err = attachConnection("", "ref1", nil)
	require.Error(t, err)
	err = DetachConnection(ctx, "")
	require.Error(t, err)
	err = DetachConnection(ctx, "nonexists")
	require.NoError(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/pkg/connection/storeConnectionErr", "return(true)")
	_, err = CreateNamedConnection(ctx, "qwe", "mock", nil)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/pkg/connection/storeConnectionErr")

	_, err = CreateNamedConnection(ctx, "qwe", "mock", nil)
	require.NoError(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/pkg/connection/dropConnectionStoreErr", "return(true)")
	err = DropNameConnection(ctx, "qwe")
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/pkg/connection/dropConnectionStoreErr")
}

func TestUpdateConn(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("id", "2")
	_, err := UpdateConnection(ctx, "", "mock", map[string]any{})
	require.Error(t, err)
	_, err = UpdateConnection(ctx, "1", "mock", map[string]any{})
	require.Error(t, err)
	_, err = UpdateConnection(ctx, "1", "mockmock", map[string]any{})
	require.Error(t, err)
	_, err = FetchConnection(ctx, "id1", "mock", nil, nil)
	require.NoError(t, err)
	_, err = UpdateConnection(ctx, "id1", "mockmock", map[string]any{})
	require.Error(t, err)
}

func TestNonStoredConnection(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("id", "2")
	_, err := FetchConnection(ctx, "id1", "mock", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("id1"))
	// Same consumer re-attaching the same key does not grow the count.
	_, err = FetchConnection(ctx, "id1", "mock", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("id1"))
	// The legacy shim normalizes the stored ref to ConsumerRefID(ctx),
	// which is exactly what legacy DetachConnection derives: the
	// round-trip releases and drops the Meta instead of leaking it.
	require.NoError(t, DetachConnection(ctx, "id1"))
	require.Equal(t, 0, getConnectionRef("id1"))
	_, ok := globalConnectionManager.Load().connectionPool["id1"]
	require.False(t, ok)
}

func TestFetchWithOptionsExplicitIdentity(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("rule1", "op1")
	newOpts := func(key, ref string) FetchOptions {
		return FetchOptions{
			ConnectionKey: key,
			RefID:         ref,
			Type:          "mock",
			Props:         map[string]any{"k": "v"},
		}
	}
	_, err := FetchConnectionWithOptions(ctx, newOpts("dbA", "rule1_op1_0"))
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("dbA"))
	// Same ref re-attaches without growing the count.
	_, err = FetchConnectionWithOptions(ctx, newOpts("dbA", "rule1_op1_0"))
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("dbA"))
	// A different consumer of the same key adds a second ref.
	_, err = FetchConnectionWithOptions(ctx, newOpts("dbA", "rule2_op1_0"))
	require.NoError(t, err)
	require.Equal(t, 2, getConnectionRef("dbA"))
	// Missing DeRef is a no-op and never drives the count negative.
	require.NoError(t, DetachConnectionByRef(ctx, "dbA", "no-such-ref"))
	require.Equal(t, 2, getConnectionRef("dbA"))
	require.NoError(t, DetachConnectionByRef(ctx, "dbA", "rule1_op1_0"))
	require.Equal(t, 1, getConnectionRef("dbA"))
	require.NoError(t, DetachConnectionByRef(ctx, "dbA", "rule1_op1_0"))
	require.Equal(t, 1, getConnectionRef("dbA"))
	require.NoError(t, DetachConnectionByRef(ctx, "dbA", "rule2_op1_0"))
	_, ok := globalConnectionManager.Load().connectionPool["dbA"]
	require.False(t, ok)
}

func TestFetchWithOptionsValidation(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("rule1", "2")
	_, err := FetchConnectionWithOptions(ctx, FetchOptions{RefID: "r", Type: "mock"})
	require.Error(t, err)
	_, err = FetchConnectionWithOptions(ctx, FetchOptions{ConnectionKey: "k", Type: "mock"})
	require.Error(t, err)
	_, err = FetchConnectionWithOptions(ctx, FetchOptions{ConnectionKey: "k", RefID: "r"})
	require.Error(t, err)
}

func TestFetchWithOptionsTypeConflict(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("rule1", "2")
	_, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "shared", RefID: "r1", Type: "mock",
	})
	require.NoError(t, err)
	// A rejected fetch must not invoke the handler: no ref was stored.
	called := false
	_, err = FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "shared", RefID: "r2", Type: "other",
		StatusHandler: func(string, string) { called = true },
	})
	require.ErrorContains(t, err, "type conflict")
	require.False(t, called)
	require.Equal(t, 1, getConnectionRef("shared"))
}

func TestFetchWithOptionsNamedMissing(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("rule1", "2")
	_, err := FetchConnectionWithOptions(ctx, FetchOptions{
		ConnectionKey: "no-such-named", RefID: "r1", RequireExisting: true, Type: "mock",
	})
	require.ErrorContains(t, err, "not existed")
}

func init() {
	modules.RegisterConnection("mock", CreateMockConnection)
	modules.RegisterConnection("faildial", CreateFailDialConnection)
	modules.RegisterConnection("countprov", CreateCountProvConnection)
	modules.RegisterConnection("failprov", CreateFailProvConnection)
	modules.RegisterConnection("countclose", CreateCountCloseConnection)
}

// failDialConnection never dials successfully: every attempt fails with an
// IO error, so the Pool worker retries until the Meta lifecycle ends. It
// models an unreachable database for lifecycle ownership tests.
type failDialConnection struct {
	id string
}

func (f *failDialConnection) GetId(ctx api.StreamContext) string {
	return f.id
}

func (f *failDialConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	f.id = conId
	return nil
}

func (f *failDialConnection) Dial(ctx api.StreamContext) error {
	return errorx.NewIOErr("faildial: database unreachable")
}

func (f *failDialConnection) Ping(ctx api.StreamContext) error {
	return errorx.NewIOErr("faildial: database unreachable")
}

func (f *failDialConnection) Close(ctx api.StreamContext) error {
	return nil
}

func CreateFailDialConnection(ctx api.StreamContext) modules.Connection {
	return &failDialConnection{}
}

// countProvConnection counts static Provisions to prove same-key
// single-flight. Provision blocks on countProvRelease so concurrent
// fetchers reliably pile up as waiters instead of serializing.
var (
	countProvRelease = make(chan struct{}, 1)
	countProvCalls   atomic.Int32
)

type countProvConnection struct {
	id string
}

func (c *countProvConnection) GetId(ctx api.StreamContext) string {
	return c.id
}

func (c *countProvConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	countProvCalls.Add(1)
	<-countProvRelease
	return nil
}

func (c *countProvConnection) Dial(ctx api.StreamContext) error {
	return nil
}

func (c *countProvConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (c *countProvConnection) Close(ctx api.StreamContext) error {
	return nil
}

func CreateCountProvConnection(ctx api.StreamContext) modules.Connection {
	return &countProvConnection{}
}

// failProvConnection fails its static Provision, but only after the test
// releases it: concurrent fetchers are guaranteed to pile up on one
// creating round instead of running sequential rounds with the same
// error text.
var (
	failProvRelease = make(chan struct{}, 1)
	failProvCalls   atomic.Int32
)

type failProvConnection struct{}

func (f *failProvConnection) GetId(ctx api.StreamContext) string { return "failprov" }

func (f *failProvConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	failProvCalls.Add(1)
	<-failProvRelease
	return fmt.Errorf("failprov: static provision failure")
}

func (f *failProvConnection) Dial(ctx api.StreamContext) error { return nil }

func (f *failProvConnection) Ping(ctx api.StreamContext) error { return nil }

func (f *failProvConnection) Close(ctx api.StreamContext) error { return nil }

func CreateFailProvConnection(ctx api.StreamContext) modules.Connection {
	return &failProvConnection{}
}

func checkConn(id string) bool {
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	_, ok := m.connectionPool[id]
	return ok
}

// getReadyTestMeta resolves the published Meta for tests. It returns nil
// for missing keys and for keys still mid-transition.
func getReadyTestMeta(key string) *Meta {
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	if e, ok := m.connectionPool[key]; ok && e.state == entryReady {
		return e.meta
	}
	return nil
}

func TestFetchConnectionNotExist(t *testing.T) {
	ctx := context.Background()
	_, err := FetchConnection(ctx, "2222", "mock", map[string]interface{}{"connectionSelector": "id2"}, nil)
	require.Error(t, err)
}

// TestReloadFailedProvisionStaysManageable is the KV-ghost regression
// test: a persisted named record whose provider is gone (unknown type)
// must still reload into a manageable Meta — GET sees it as
// disconnected, and DELETE removes both the Meta and the KV record
// instead of reporting success while the ghost resurrects on restart.
func TestReloadFailedProvisionStaysManageable(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("reload", "op1")
	// Defensive cleanup from any prior failed run.
	_ = dropConnectionStore("nosuchprovider", "ghost1")

	require.NoError(t, storeConnectionMeta("nosuchprovider", "ghost1", map[string]any{"a": 1}))
	require.NoError(t, ReloadNamedConnection())

	meta, err := GetConnectionDetail(ctx, "ghost1")
	require.NoError(t, err)
	require.True(t, meta.Named)
	status, msg := meta.GetStatus()
	require.Equal(t, api.ConnectionDisconnected, status)
	require.NotEmpty(t, msg)

	// Wait on the failed handle surfaces the provision error, not nil.
	_, err = meta.cw.Wait(ctx)
	require.Error(t, err)

	// DELETE works and really removes the KV record.
	require.NoError(t, DropNameConnection(ctx, "ghost1"))
	_, err = GetConnectionDetail(ctx, "ghost1")
	require.Error(t, err)
	cfgs, err := conf.GetCfgFromKVStorage("connections", "nosuchprovider", "ghost1")
	require.NoError(t, err)
	require.Empty(t, cfgs)
}
