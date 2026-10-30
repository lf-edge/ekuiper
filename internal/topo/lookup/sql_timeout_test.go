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

package lookup

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sqlconnector "github.com/lf-edge/ekuiper/v2/extensions/impl/sql"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestSQLLookupTimeoutReleasesRegistry(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	modules.RegisterLookupSource("sql", sqlconnector.GetLookupSource)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	ctx, cancel := kctx.Background().WithCancel()
	t.Cleanup(cancel)
	dburl := "mysql://root:@" + listener.Addr().String() + "/test"
	cw, err := connection.CreateNamedConnection(ctx, "lookup-timeout", "sql", map[string]any{"dburl": dburl})
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, connection.DropNameConnection(ctx, cw.ID))
	})
	select {
	case c := <-accepted:
		t.Cleanup(func() { _ = c.Close() })
	case <-time.After(2 * time.Second):
		t.Fatal("named connection did not start dialing")
	}
	opts := func(dburl, selector string) *ast.Options {
		return &ast.Options{DATASOURCE: "t", EXTRA: fmt.Sprintf(
			`{"dburl":%q,"connectionSelector":%q}`, dburl, selector)}
	}
	failed := make(chan error, 1)
	go func() { failed <- CreateInstance("failed_sql_lookup", "sql", opts(dburl, cw.ID)) }()
	select {
	case err := <-failed:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(12 * time.Second):
		t.Fatal("lookup creation kept the registry locked")
	}
	meta, err := connection.GetConnectionDetail(ctx, cw.ID)
	require.NoError(t, err)
	require.Zero(t, meta.GetRefCount())
	require.Empty(t, meta.GetRefNames())
	_, err = Attach("failed_sql_lookup")
	require.Error(t, err)

	// The same registry must accept and serve a healthy table after failure.
	goodURL := "sqlite://" + filepath.Join(t.TempDir(), "good.db")
	started := time.Now()
	require.NoError(t, CreateInstance("good_sql_lookup", "sql", &ast.Options{
		DATASOURCE: "t", EXTRA: fmt.Sprintf(`{"dburl":%q}`, goodURL),
	}))
	t.Cleanup(func() { require.NoError(t, DropInstance("good_sql_lookup")) })
	require.Less(t, time.Since(started), 2*time.Second)
	_, err = Attach("good_sql_lookup")
	require.NoError(t, err)
	require.NoError(t, Detach("good_sql_lookup"))
}
