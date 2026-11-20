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
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
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
	// Keep another consumer attached using the same URL as the lookup.
	var notifications atomic.Int32
	_, err = connection.FetchConnection(ctx, dburl, "sql", map[string]any{"connectionSelector": cw.ID},
		func(string, string) { notifications.Add(1) })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.DetachConnectionByRef(ctx, cw.ID, dburl)) })
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
	require.Equal(t, 1, meta.GetRefCount())
	require.Equal(t, []string{dburl}, meta.GetRefNames())
	before := notifications.Load()
	meta.NotifyStatus(api.ConnectionDisconnected, "still waiting")
	require.Equal(t, before+1, notifications.Load())
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

func TestRestoreSQLLookupRetriesAndReleases(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	modules.RegisterLookupSource("sql", sqlconnector.GetLookupSource)
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	const dburl = "mysql://root:@127.0.0.1:1/default"
	url := "mysql://root:@" + listener.Addr().String() + "/test"
	opts := &ast.Options{DATASOURCE: "t", EXTRA: fmt.Sprintf(`{"dburl":%q,"url":%q}`, dburl, url)}
	for _, name := range []string{"restored_sql_one", "restored_sql_two"} {
		require.NoError(t, RestoreInstance(name, "sql", opts))
		t.Cleanup(func() { require.NoError(t, DropInstance(name)) })
	}
	ctx := kctx.Background()
	meta, err := connection.GetConnectionDetail(ctx, dburl)
	require.NoError(t, err)
	require.Equal(t, 2, meta.GetRefCount())

	accept := func(timeout time.Duration) *net.TCPConn {
		t.Helper()
		require.NoError(t, listener.SetDeadline(time.Now().Add(timeout)))
		peer, err := listener.AcceptTCP()
		require.NoError(t, err)
		t.Cleanup(func() { _ = peer.Close() })
		return peer
	}
	first := accept(3 * time.Second)

	source, err := Attach("restored_sql_one")
	require.NoError(t, err)
	waitCtx, cancel := ctx.WithCancel()
	defer cancel()
	waited := make(chan error, 1)
	go func() {
		_, err := source.(api.LookupSource).Lookup(waitCtx, nil, []string{"a"}, []any{1})
		waited <- err
	}()
	select {
	case err := <-waited:
		t.Fatalf("lookup returned before the connection was ready: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waited:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("lookup did not stop waiting on rule cancellation")
	}
	require.NoError(t, Detach("restored_sql_one"))
	require.NoError(t, DropInstance("restored_sql_one"))
	require.Equal(t, 1, meta.GetRefCount())

	// Withhold the first handshake throughout. The remaining reference must
	// keep the pool alive and the deadline must let it start another dial.
	second := accept(15 * time.Second)
	var b [1]byte
	require.NoError(t, first.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = first.Read(b[:])
	require.ErrorIs(t, err, io.EOF)

	require.NoError(t, DropInstance("restored_sql_two"))
	require.Zero(t, meta.GetRefCount())
	_, err = connection.GetConnectionDetail(ctx, dburl)
	require.Error(t, err)
	require.NoError(t, second.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = second.Read(b[:])
	require.ErrorIs(t, err, io.EOF)
}

func TestRestoreSQLLookupRejectsUnregisteredDriver(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	modules.RegisterLookupSource("sql", sqlconnector.GetLookupSource)
	const dburl = "duckdb:///unused"
	err := RestoreInstance("unregistered_sql", "sql", &ast.Options{
		DATASOURCE: "t",
		EXTRA:      fmt.Sprintf(`{"dburl":%q}`, dburl),
	})
	require.ErrorContains(t, err, `sql driver "duckdb" is not registered`)
	_, err = Attach("unregistered_sql")
	require.Error(t, err)
	_, err = connection.GetConnectionDetail(kctx.Background(), dburl)
	require.Error(t, err)
}
