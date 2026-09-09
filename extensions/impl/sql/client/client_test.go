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

//go:build mysql_integration_test

package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

var (
	address = "localhost"
	port    = 33060
)

func TestSQLClient(t *testing.T) {
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	props := map[string]interface{}{
		"dburl": fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
	}
	conn := CreateConnection(ctx)
	require.NoError(t, err)
	err = conn.Provision(ctx, "test", props)
	require.NoError(t, err)
	sconn, ok := conn.(*SQLConnection)
	require.True(t, ok)
	err = conn.Dial(ctx)
	require.NoError(t, err)
	require.NotNil(t, sconn.GetDB())
	require.NoError(t, conn.Ping(ctx))
	conn.Close(ctx)
}

func TestSQLReconnectFailureKeepsDBHandle(t *testing.T) {
	serverPort := 33063
	s, err := testx.SetupEmbeddedMysqlServer(address, serverPort)
	require.NoError(t, err)
	defer s.Close()

	ctx := mockContext.NewMockContext("1", "2")
	conn := CreateConnection(ctx)
	require.NoError(t, conn.Provision(ctx, "test", map[string]any{
		"dburl": fmt.Sprintf("mysql://root:@%v:%v/test", address, serverPort),
	}))
	require.NoError(t, conn.Dial(ctx))
	sconn := conn.(*SQLConnection)

	// Simulate a runtime disconnect and make the replacement endpoint fail.
	// The second consumer shares the same SQLConnection instance.
	_ = sconn.db.Close()
	sconn.url = fmt.Sprintf("mysql://root:@%v:%v/test", address, serverPort+1)

	require.Error(t, sconn.Reconnect())
	sharedDB := sconn.GetDB()
	require.NotNil(t, sharedDB)
	_, err = sharedDB.Query("select 1")
	require.Error(t, err)
}
