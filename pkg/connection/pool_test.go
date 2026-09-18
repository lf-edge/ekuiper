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
	stdcontext "context"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type retryTestConnection struct {
	attempts chan struct{}
}

func (c *retryTestConnection) GetId(api.StreamContext) string                            { return "retry-test" }
func (c *retryTestConnection) Provision(api.StreamContext, string, map[string]any) error { return nil }
func (c *retryTestConnection) Dial(api.StreamContext) error {
	c.attempts <- struct{}{}
	return errorx.NewIOErr("database unavailable")
}
func (c *retryTestConnection) Ping(api.StreamContext) error  { return nil }
func (c *retryTestConnection) Close(api.StreamContext) error { return nil }

func TestNamedConnectionRetryCancelledOnUpdateAndDrop(t *testing.T) {
	require.NoError(t, InitConnectionManager4Test())
	ctx := context.Background()
	const typ = "retry-test"
	previous, hadPrevious := modules.ConnectionRegister[typ]
	instances := make(chan *retryTestConnection, 3)
	modules.RegisterConnection(typ, func(api.StreamContext) modules.Connection {
		conn := &retryTestConnection{attempts: make(chan struct{}, 100)}
		instances <- conn
		return conn
	})
	t.Cleanup(func() {
		if hadPrevious {
			modules.RegisterConnection(typ, previous)
		} else {
			delete(modules.ConnectionRegister, typ)
		}
	})

	old, err := CreateNamedConnection(ctx, "retry-test-id", typ, nil)
	require.NoError(t, err)
	oldConn := <-instances
	select {
	case <-oldConn.attempts:
	case <-time.After(time.Second):
		t.Fatal("old connection never attempted dial")
	}
	current, err := UpdateConnection(ctx, "retry-test-id", typ, nil)
	require.NoError(t, err)
	newConn := <-instances
	_, err = old.Wait(ctx)
	require.ErrorIs(t, err, stdcontext.Canceled)
	select {
	case <-newConn.attempts:
	case <-time.After(time.Second):
		t.Fatal("replacement connection never attempted dial")
	}
	require.NoError(t, DropNameConnection(ctx, "retry-test-id"))
	_, err = current.Wait(ctx)
	require.ErrorIs(t, err, stdcontext.Canceled)
	for _, ch := range []chan struct{}{oldConn.attempts, newConn.attempts} {
		for len(ch) > 0 {
			<-ch
		}
		select {
		case <-ch:
			t.Fatal("cancelled connection continued retrying")
		case <-time.After(300 * time.Millisecond):
		}
	}

	shutdown, err := CreateNamedConnection(ctx, "retry-test-shutdown", typ, nil)
	require.NoError(t, err)
	shutdownConn := <-instances
	select {
	case <-shutdownConn.attempts:
	case <-time.After(time.Second):
		t.Fatal("shutdown connection never attempted dial")
	}
	globalConnectionManager.stopAll(ctx)
	_, err = shutdown.Wait(ctx)
	require.ErrorIs(t, err, stdcontext.Canceled)
	require.NoError(t, DropNameConnection(ctx, "retry-test-shutdown"))
}

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
	err = detachConnection(ctx, "id1", "ref1")
	require.NoError(t, err)
	require.Equal(t, 1, getConnectionRef("id1"))
	err = DropNameConnection(ctx, "id1")
	require.Error(t, err)
	err = detachConnection(ctx, "id1", "ref2")
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
	_, err = cw.Wait(ctx)
	require.Error(t, err)
	_, err = attachConnection("", "ref1", nil)
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
	blockCh <- struct{}{}
	wg.Wait()
	require.True(t, checkConn("ccc1"))

	wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err := CreateNamedConnection(ctx, "ccc2", "blockconn", nil)
		require.NoError(t, err)
		wg.Done()
	}()
	wg.Wait()
	require.NoError(t, DropNameConnection(ctx, "ccc2"))
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

func TestFetchConnectionNotExist(t *testing.T) {
	ctx := context.Background()
	_, err := FetchConnection(ctx, "2222", "mock", map[string]interface{}{"connectionSelector": "id2"}, nil)
	require.Error(t, err)
}
