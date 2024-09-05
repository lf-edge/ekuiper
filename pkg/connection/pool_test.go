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
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestConnection(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := context.Background()
	cw, err := CreateNamedConnection(ctx, "id1", "mock", nil)
	require.NoError(t, err)
	conn, err := cw.Wait()
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
	err = detachConnection(ctx, "id1")
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("id1"))
	err = DropNameConnection(ctx, "id1")
	require.Error(t, err)
	err = detachConnection(ctx, "id1")
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
	cw, err := CreateNamedConnection(ctx, "12", "unknown", nil)
	require.NoError(t, err)
	_, err = cw.Wait()
	require.Error(t, err)
	_, err = attachConnection("", "ref1", nil)
	require.Error(t, err)
	err = PingConnection(ctx, "")
	require.Error(t, err)
	err = DetachConnection(ctx, "")
	require.Error(t, err)
	err = DetachConnection(ctx, "nonexists")
	require.NoError(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/pkg/createConnectionErr", "return(true)")
	conn, err := createConnection(ctx, &Meta{
		ID:    "1",
		Typ:   "mock",
		Props: nil,
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/pkg/connection/createConnectionErr")

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

func TestConnectionStatus(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	conf.WriteCfgIntoKVStorage("connections", "mockErr", "a1", map[string]interface{}{})
	conf.WriteCfgIntoKVStorage("connections", "mock", "a2", map[string]interface{}{})
	require.NoError(t, ReloadNamedConnection())
	time.Sleep(100 * time.Millisecond)
	ctx := context.Background()
	allStatus := GetAllConnectionStatus(ctx)
	s, ok := allStatus["a1"]
	require.True(t, ok)
	require.Equal(t, modules.ConnectionStatus{
		Status: api.ConnectionDisconnected,
		ErrMsg: "mockErr",
	}, s)
	s, ok = allStatus["a2"]
	require.True(t, ok)
	require.Equal(t, modules.ConnectionStatus{
		Status: api.ConnectionConnected,
	}, s)
}

func TestGetAllConnectionStatus(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("id", "2")
	cw1, err := FetchConnection(ctx, "id1", "mock", nil, nil)
	require.NoError(t, err)
	cw2, err := FetchConnection(ctx, "id2", "mockErr", nil, nil)
	require.NoError(t, err)
	cw3, err := FetchConnection(ctx, "id3", "blockconn", nil, nil)
	require.NoError(t, err)
	cw1.Wait()
	cw2.Wait()
	allStatus := GetAllConnectionStatus(ctx)
	s, ok := allStatus["id2"]
	require.True(t, ok)
	require.Equal(t, modules.ConnectionStatus{
		Status: api.ConnectionDisconnected,
		ErrMsg: "mockErr",
	}, s)
	s, ok = allStatus["id1"]
	require.True(t, ok)
	require.Equal(t, modules.ConnectionStatus{
		Status: api.ConnectionConnected,
	}, s)
	s, ok = allStatus["id3"]
	require.True(t, ok)
	require.Equal(t, modules.ConnectionStatus{
		Status: api.ConnectionConnecting,
	}, s)
	blockCh <- struct{}{}
	cw3.Wait()
	allStatus = GetAllConnectionStatus(ctx)
	s, ok = allStatus["id3"]
	require.True(t, ok)
	require.Equal(t, modules.ConnectionStatus{
		Status: api.ConnectionConnected,
	}, s)
}

func TestNonStoredConnection(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("id", "2")
	_, err := FetchConnection(ctx, "id1", "mock", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("id1"))
	_, err = FetchConnection(ctx, "id1", "mock", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 2, getConnectionRef("id1"))
	require.NoError(t, DetachConnection(ctx, "id1"))
	require.Equal(t, 1, getConnectionRef("id1"))
	require.NoError(t, DetachConnection(ctx, "id1"))
	require.Equal(t, 0, getConnectionRef("id1"))
	_, ok := globalConnectionManager.connectionPool["id1"]
	require.False(t, ok)
}

func TestConnectionLock(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("id", "2")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err := CreateNamedConnection(ctx, "ccc1", "blockconn", nil)
		require.NoError(t, err)
		wg.Done()
	}()
	require.False(t, checkConn("ccc1"))
	blockCh <- struct{}{}
	wg.Wait()
	require.True(t, checkConn("ccc1"))
}

var blockCh chan any

func init() {
	blockCh = make(chan any, 10)
	modules.RegisterConnection("blockconn", CreateBlockConnection)
}

type blockConnection struct {
	blochCh chan any
	id      string
}

func (b *blockConnection) GetId(ctx api.StreamContext) string {
	return b.id
}

func (b *blockConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	b.id = conId
	return nil
}

func (b *blockConnection) Dial(ctx api.StreamContext) error {
	<-blockCh
	return nil
}

func (b *blockConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (b *blockConnection) Close(ctx api.StreamContext) error {
	return nil
}

func CreateBlockConnection(ctx api.StreamContext) modules.Connection {
	return &blockConnection{}
}

func checkConn(id string) bool {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	_, ok := globalConnectionManager.connectionPool[id]
	return ok
}
