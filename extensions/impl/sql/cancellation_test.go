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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

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
			} else {
				operationErr = sink.writeToDB(ctx, "INSERT INTO t "+cancellationQuery)
			}
			require.Error(t, operationErr)
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
