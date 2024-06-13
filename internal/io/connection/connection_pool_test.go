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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

func TestConnection(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, InitConnectionManager4Test())
	ctx := context.Background()
	conn, err := CreateNamedConnection(ctx, "id1", "mock", nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.NoError(t, conn.Ping(ctx))
	require.Equal(t, 0, GetConnectionRef("id1"))
	_, err = CreateNamedConnection(ctx, "id1", "mock", nil)
	require.Error(t, err)
	AttachConnection("id1")
	require.Equal(t, 1, GetConnectionRef("id1"))
	AttachConnection("id1")
	require.Equal(t, 2, GetConnectionRef("id1"))
	DetachConnection("id1")
	require.Equal(t, 1, GetConnectionRef("id1"))
	err = DropNameConnection(ctx, "id1")
	require.Error(t, err)
	DetachConnection("id1")
	require.Equal(t, 0, GetConnectionRef("id1"))
	err = DropNameConnection(ctx, "id1")
	require.NoError(t, err)
	err = DropNameConnection(ctx, "id1")
	require.NoError(t, err)
	conn3, err := AttachConnection("id1")
	require.Error(t, err)
	require.Nil(t, conn3)
}

func TestConnectionErr(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, InitConnectionManager4Test())
	ctx := context.Background()

	_, err = CreateNamedConnection(ctx, "", "mock", nil)
	require.Error(t, err)
	err = DropNameConnection(ctx, "")
	require.Error(t, err)
	_, err = CreateNamedConnection(ctx, "12", "unknown", nil)
	require.Error(t, err)
	_, err = AttachConnection("")
	require.Error(t, err)
	err = PingConnection(ctx, "")
	require.Error(t, err)
	_, err = CreateNonStoredConnection(ctx, "", "mock", nil)
	require.Error(t, err)

	conn4, err := CreateNonStoredConnection(ctx, "id2", "mock", nil)
	require.NoError(t, err)
	require.NotNil(t, conn4)
	_, err = CreateNonStoredConnection(ctx, "id2", "mock", nil)
	require.Error(t, err)
	err = DropNonStoredConnection(ctx, "")
	require.Error(t, err)
	err = DropNonStoredConnection(ctx, "nonexists")
	require.NoError(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/connection/createConnectionErr", "return(true)")
	conn, err := createNamedConnection(ctx, &ConnectionMeta{
		ID:    "1",
		Typ:   "mock",
		Props: nil,
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/connection/createConnectionErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/connection/storeConnectionErr", "return(true)")
	_, err = CreateNamedConnection(ctx, "qwe", "mock", nil)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/connection/storeConnectionErr")

	_, err = CreateNamedConnection(ctx, "qwe", "mock", nil)
	require.NoError(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/connection/dropConnectionStoreErr", "return(true)")
	err = DropNameConnection(ctx, "qwe")
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/connection/dropConnectionStoreErr")
}

func TestConnectionStatus(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, InitConnectionManager4Test())

	conf.WriteCfgIntoKVStorage("connections", "mockErr", "a1", map[string]interface{}{})
	conf.WriteCfgIntoKVStorage("connections", "mock", "a2", map[string]interface{}{})
	require.NoError(t, ReloadConnection())
	ctx := context.Background()
	allStatus := GetAllConnectionStatus(ctx)
	s, ok := allStatus["a1"]
	require.True(t, ok)
	require.Equal(t, ConnectionStatus{
		Status: ConnectionFail,
		ErrMsg: "mockErr",
	}, s)
	s, ok = allStatus["a2"]
	require.True(t, ok)
	require.Equal(t, ConnectionStatus{
		Status: ConnectionRunning,
	}, s)
}
