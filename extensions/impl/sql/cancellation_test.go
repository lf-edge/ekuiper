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

package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// Aggregate before returning any rows, so cancellation must interrupt the
// database operation itself rather than just a caller waiting for a result.
const cancellationQuery = `WITH RECURSIVE seq(n) AS (
	VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<10000000
) SELECT sum(n), sum(n) FROM seq`

func TestSQLOperationCancellation(t *testing.T) {
	for _, operation := range []string{"source", "sink"} {
		t.Run(operation, func(t *testing.T) {
			require.NoError(t, connection.InitConnectionManager4Test())
			root := mockContext.NewMockContext(t.Name(), "sql")
			dburl := "sqlite://" + filepath.Join(t.TempDir(), "shared.db")
			sink := &SQLSinkConnector{}
			require.NoError(t, sink.Provision(root, map[string]any{"dburl": dburl, "table": "t"}))
			require.NoError(t, sink.Connect(root, nil))
			t.Cleanup(func() { require.NoError(t, sink.Close(root)) })
			db := sink.conn.GetDB()
			_, err := db.Exec("CREATE TABLE t(a INTEGER, b INTEGER)")
			require.NoError(t, err)
			require.NoError(t, sink.collect(root, map[string]any{"a": 1, "b": 2}))

			sourceRoot := mockContext.NewMockContext(t.Name(), "source")
			source := &SQLSourceConnector{}
			require.NoError(t, source.Provision(sourceRoot, map[string]any{
				"dburl": dburl, "interval": "1s",
				"templateSqlQueryCfg": map[string]any{"templateSql": cancellationQuery},
			}))
			require.NoError(t, source.Connect(sourceRoot, nil))
			t.Cleanup(func() { require.NoError(t, source.Close(sourceRoot)) })
			require.Same(t, sink.conn, source.conn)

			ctx, cancel := sourceRoot.WithCancel()
			defer cancel()
			timer := time.AfterFunc(50*time.Millisecond, cancel)
			defer timer.Stop()
			started := time.Now()
			var operationErr error
			if operation == "source" {
				source.queryData(ctx, started, func(api.StreamContext, any, map[string]any, time.Time) {
					t.Error("canceled source query returned a row")
				}, func(_ api.StreamContext, err error) { operationErr = err })
				require.NoError(t, operationErr, "stopping a source is not a database failure")
			} else {
				operationErr = sink.writeToDB(ctx, "INSERT INTO t "+cancellationQuery)
				require.Error(t, operationErr)
			}
			require.Error(t, ctx.Err())
			require.Less(t, time.Since(started), 2*time.Second)

			// A canceled consumer must leave the physical shared database usable.
			require.NoError(t, sink.collect(root, map[string]any{"a": 3, "b": 4}))
			var count int
			require.NoError(t, db.QueryRow("SELECT count(*) FROM t").Scan(&count))
			require.Equal(t, 2, count, fmt.Sprintf("unexpected rows after canceled %s", operation))
		})
	}
}

func TestSQLDialCancellationPublishesError(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	root := mockContext.NewMockContext(t.Name(), "sql")
	ctx, cancel := root.WithCancel()
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	cw, err := connection.CreateNamedConnection(ctx, "canceled-dial", "sql", map[string]any{
		"dburl": "mysql://root:@" + listener.Addr().String() + "/test?timeout=1s&readTimeout=1s",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.DropNameConnection(root, cw.ID)) })
	select {
	case conn := <-accepted:
		cancel()
		// Release the handshake as well, without depending on a driver timeout.
		require.NoError(t, conn.Close())
	case <-time.After(2 * time.Second):
		t.Fatal("SQL dial did not connect to the test server")
	}
	// A different consumer must observe the creation error, even with a live context.
	waitCtx, stopWaiting := root.WithCancel()
	defer stopWaiting()
	timer := time.AfterFunc(2*time.Second, stopWaiting)
	defer timer.Stop()
	conn, err := cw.Wait(waitCtx)
	require.NoError(t, waitCtx.Err(), "connection creation did not finish")
	require.ErrorIs(t, err, context.Canceled)
	if conn != nil {
		require.Nil(t, conn.(*client.SQLConnection).GetDB())
	}
}

func TestSQLSourceRowsLifecycle(t *testing.T) {
	for _, failure := range []string{"driver_error", "cancellation", "scan_error"} {
		t.Run(failure, func(t *testing.T) {
			require.NoError(t, connection.InitConnectionManager4Test())
			root := mockContext.NewMockContext(t.Name(), "source")
			ctx, cancel := root.WithCancel()
			defer cancel()
			function := fmt.Sprintf("source_rows_%d", time.Now().UnixNano())
			failIteration := true
			require.NoError(t, sqlite.RegisterScalarFunction(function, 1,
				func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
					if args[0] == int64(32) && failIteration {
						if failure == "cancellation" {
							cancel()
						}
						return nil, errors.New("iteration failure")
					}
					return args[0], nil
				}))
			source := &SQLSourceConnector{}
			require.NoError(t, source.Provision(root, map[string]any{
				"dburl": "sqlite://" + filepath.Join(t.TempDir(), "source.db"), "interval": "1s",
				"templateSqlQueryCfg": map[string]any{"templateSql": fmt.Sprintf(`
					WITH RECURSIVE seq(n) AS (
						VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<64
					) SELECT %s(n) AS n FROM seq`, function)},
			}))
			require.NoError(t, source.Connect(root, nil))
			t.Cleanup(func() { require.NoError(t, source.Close(root)) })
			if failure == "scan_error" {
				// A destination mismatch makes Scan return early with rows still open.
				source.columns = []interface{}{new(int64), new(int64)}
			}
			var queryErr error
			count := 0
			ingest := func(api.StreamContext, any, map[string]any, time.Time) { count++ }
			ingestError := func(_ api.StreamContext, err error) { queryErr = err }
			source.queryData(ctx, time.Now(), ingest, ingestError)
			switch failure {
			case "cancellation":
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.NoError(t, queryErr)
			case "driver_error":
				require.ErrorContains(t, queryErr, "iteration failure")
				require.Equal(t, 31, count)
			case "scan_error":
				require.ErrorContains(t, queryErr, "destination arguments")
			}
			require.Zero(t, source.conn.GetDB().Stats().InUse)

			// The next healthy query must retain the shared handle and read all rows.
			failIteration = false
			source.columns = nil
			queryErr, count = nil, 0
			source.queryData(root, time.Now(), ingest, ingestError)
			require.NoError(t, queryErr)
			require.Equal(t, 64, count)
			require.Zero(t, source.conn.GetDB().Stats().InUse)
		})
	}
}

func TestLookupCancellationDuringIteration(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	root := mockContext.NewMockContext(t.Name(), "lookup")
	ctx, cancel := root.WithCancel()
	defer cancel()
	function := fmt.Sprintf("lookup_cancel_%d", time.Now().UnixNano())
	var canceledAtRow int64
	cancelDuringIteration := true
	require.NoError(t, sqlite.RegisterScalarFunction(function, 1,
		func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if args[0] == int64(32) && cancelDuringIteration {
				// Stop SQLite at this row too, so the iteration error is reported
				// by rows.Err() instead of racing with a subsequent Scan call.
				canceledAtRow = 32
				cancel()
				return nil, ctx.Err()
			}
			return args[0], nil
		}))
	source := &SqlLookupSource{}
	require.NoError(t, source.Provision(root, map[string]any{
		"dburl": "sqlite://" + filepath.Join(t.TempDir(), "lookup.db"),
		"templateSqlQueryCfg": map[string]any{"templateSql": fmt.Sprintf(`
			WITH RECURSIVE seq(n) AS (
				VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<64
			) SELECT %s(n) AS n FROM seq`, function)},
	}))
	require.NoError(t, source.Connect(root, nil))
	t.Cleanup(func() { require.NoError(t, source.Close(root)) })

	rows, err := source.Lookup(ctx, nil, nil, nil)
	require.EqualValues(t, 32, canceledAtRow)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Error(t, err, "lookup returned %d partial rows without an error", len(rows))
	require.Nil(t, rows)

	// A healthy request on the same pooled database still returns all rows.
	cancelDuringIteration = false
	rows, err = source.Lookup(root, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, rows, 64)
	require.Equal(t, map[string]any{"n": int64(1)}, rows[0])
	require.Equal(t, map[string]any{"n": int64(64)}, rows[63])
}

func TestSQLConnectRejectsUndialedConnection(t *testing.T) {
	for name, connector := range map[string]api.Source{
		"source": &SQLSourceConnector{},
		"sink":   &SQLSinkConnector{},
	} {
		t.Run(name, func(t *testing.T) { testSQLConnectRejectsUndialedConnection(t, connector) })
	}
}

func testSQLConnectRejectsUndialedConnection(t *testing.T, connector api.Source) {
	t.Helper()
	require.NoError(t, connection.InitConnectionManager4Test())
	root := mockContext.NewMockContext(t.Name(), "sql")
	canceled, cancel := root.WithCancel()
	cancel()
	dburl := "sqlite://" + filepath.Join(t.TempDir(), "undialed.db")
	cw, err := connection.CreateNamedConnection(canceled, "undialed", "sql", map[string]any{"dburl": dburl})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.DropNameConnection(root, cw.ID)) })
	conn, err := cw.Wait(root)
	// The legacy pool can publish a provisioned-only provider after cancellation.
	require.NoError(t, err)
	require.Nil(t, conn.(*client.SQLConnection).GetDB())
	require.NoError(t, connector.Provision(root, map[string]any{
		"dburl": dburl, "connectionSelector": cw.ID, "interval": "1s", "table": "t",
		"templateSqlQueryCfg": map[string]any{"templateSql": "SELECT 1"},
	}))
	require.ErrorContains(t, connector.Connect(root, nil), "sql client not ready")
	require.NoError(t, connector.Close(root))
	meta, err := connection.GetConnectionDetail(root, cw.ID)
	require.NoError(t, err)
	require.Zero(t, meta.GetRefCount())
}
