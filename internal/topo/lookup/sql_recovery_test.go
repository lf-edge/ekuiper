// Copyright 2026 EMQ Technologies Co., Ltd.
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

package lookup

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	sqlconnector "github.com/lf-edge/ekuiper/v2/extensions/impl/sql"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestRestoredSQLLookupRecoversAfterStalledHandshake(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	modules.RegisterLookupSource("sql", sqlconnector.GetLookupSource)
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)
	const table = "restored_mysql"
	require.NoError(t, RestoreInstance(table, "sql", &ast.Options{
		DATASOURCE: "t",
		EXTRA:      fmt.Sprintf(`{"dburl":"mysql://root:@%s/test"}`, addr),
	}))
	t.Cleanup(func() { require.NoError(t, DropInstance(table)) })

	require.NoError(t, listener.SetDeadline(time.Now().Add(3*time.Second)))
	stalledPeer, err := listener.AcceptTCP()
	require.NoError(t, err)
	defer stalledPeer.Close()

	source, err := Attach(table)
	require.NoError(t, err)
	ctx, cancel := kctx.Background().WithCancel()
	type result struct {
		rows []map[string]any
		err  error
	}
	queried := make(chan result, 1)
	go func() {
		rows, err := source.(api.LookupSource).Lookup(ctx, []string{"b"}, []string{"a"}, []any{1})
		queried <- result{rows, err}
	}()
	defer func() {
		cancel()
		require.NoError(t, Detach(table))
	}()
	select {
	case early := <-queried:
		t.Fatalf("lookup returned during the stalled handshake: %+v", early)
	case <-time.After(50 * time.Millisecond):
	}

	// Leave the first accepted connection open and let MySQL listen on
	// the same address. Only another dial can reach the database.
	require.NoError(t, listener.Close())
	mysql, err := testx.SetupEmbeddedMysqlServer(addr.IP.String(), addr.Port)
	require.NoError(t, err)
	defer mysql.Close()

	select {
	case got := <-queried:
		require.NoError(t, got.err)
		require.Equal(t, []map[string]any{{"b": int64(1)}}, got.rows)
	case <-time.After(15 * time.Second):
		t.Fatal("lookup did not recover after the first dial timed out")
	}
}
