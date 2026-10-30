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
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestLookupSharedConnectionLifecycle(t *testing.T) {
	for _, named := range []bool{true, false} {
		t.Run(map[bool]string{true: "named", false: "anonymous"}[named], func(t *testing.T) {
			require.NoError(t, connection.InitConnectionManager4Test())
			ctx := mockContext.NewMockContext(t.Name(), "lookup")
			dburl := "sqlite://" + filepath.Join(t.TempDir(), "shared.db")
			props := map[string]any{"dburl": dburl, "datasource": "t"}
			if named {
				cw, err := connection.CreateNamedConnection(ctx, "lookup-shared", "sql", map[string]any{"dburl": dburl})
				require.NoError(t, err)
				props["connectionSelector"] = cw.ID
				t.Cleanup(func() { require.NoError(t, connection.DropNameConnection(ctx, cw.ID)) })
			}
			var lookups [2]SqlLookupSource
			var notifications [2]atomic.Int32
			for i := range lookups {
				s := &lookups[i]
				require.NoError(t, s.Provision(ctx, props))
				require.NoError(t, s.Connect(ctx, func(string, string) { notifications[i].Add(1) }))
				t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
			}
			require.Same(t, lookups[0].conn, lookups[1].conn)
			db := lookups[0].conn.GetDB()
			require.Same(t, db, lookups[1].conn.GetDB())
			_, err := db.Exec("CREATE TABLE t(a INTEGER); INSERT INTO t VALUES(7)")
			require.NoError(t, err)
			meta, err := connection.GetConnectionDetail(ctx, lookups[0].conId)
			require.NoError(t, err)
			require.Equal(t, named, meta.Named)
			require.Equal(t, 2, meta.GetRefCount())
			require.Len(t, meta.GetRefNames(), 2)
			before := [2]int32{notifications[0].Load(), notifications[1].Load()}
			meta.NotifyStatus(api.ConnectionConnected, "")
			for i := range notifications {
				require.Equal(t, before[i]+1, notifications[i].Load())
			}

			// Notifications can overlap releasing a different lookup instance.
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 0; i < 100; i++ {
					meta.NotifyStatus(api.ConnectionConnected, "")
				}
			}()
			require.NoError(t, lookups[0].Close(ctx))
			<-done
			require.NoError(t, lookups[0].Close(ctx))
			require.Equal(t, 1, meta.GetRefCount())
			require.Equal(t, []string{lookups[1].refId}, meta.GetRefNames())
			before = [2]int32{notifications[0].Load(), notifications[1].Load()}
			meta.NotifyStatus(api.ConnectionConnected, "")
			require.Equal(t, before[0], notifications[0].Load())
			require.Equal(t, before[1]+1, notifications[1].Load())

			// A failed attachment must also leave the surviving lookup intact.
			failed := &SqlLookupSource{}
			require.NoError(t, failed.Provision(ctx, props))
			canceled, cancel := ctx.WithCancel()
			cancel()
			require.ErrorIs(t, failed.Connect(canceled, nil), canceled.Err())
			require.NoError(t, failed.Close(ctx))
			require.Equal(t, 1, meta.GetRefCount())
			require.Equal(t, []string{lookups[1].refId}, meta.GetRefNames())
			rows, err := lookups[1].Lookup(ctx, []string{"a"}, []string{"a"}, []any{7})
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"a": int64(7)}}, rows)
			require.NoError(t, lookups[1].Close(ctx))
			require.Zero(t, meta.GetRefCount())
			require.Empty(t, meta.GetRefNames())
			_, err = connection.GetConnectionDetail(ctx, meta.ID)
			if named {
				require.NoError(t, err)
				require.NoError(t, db.Ping())
			} else {
				require.Error(t, err, "the last anonymous reference must remove the connection")
				require.Error(t, db.Ping(), "the anonymous database handle must be closed")
			}
		})
	}
}

func TestLookupFastFailuresReleaseReference(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext(t.Name(), "lookup")
	dburl := "sqlite://" + filepath.Join(t.TempDir(), "fast.db")
	// The provider fails permanently before any network attempt.
	cw, err := connection.CreateNamedConnection(ctx, "invalid-provider", "unknown-provider", nil)
	require.NoError(t, err)
	_, err = cw.Wait(ctx)
	require.Error(t, err)
	var notifications atomic.Int32
	_, err = connection.FetchConnection(ctx, dburl, "sql", map[string]any{"connectionSelector": cw.ID},
		func(string, string) { notifications.Add(1) })
	require.NoError(t, err)
	for _, selector := range []string{cw.ID, "missing-connection"} {
		s := &SqlLookupSource{}
		require.NoError(t, s.Provision(ctx, map[string]any{
			"dburl": dburl, "connectionSelector": selector, "datasource": "t",
		}))
		started := time.Now()
		require.Error(t, s.Connect(ctx, nil))
		require.Less(t, time.Since(started), time.Second)
		require.NoError(t, s.Close(ctx))
	}
	meta, err := connection.GetConnectionDetail(ctx, cw.ID)
	require.NoError(t, err)
	require.Equal(t, 1, meta.GetRefCount())
	require.Equal(t, []string{dburl}, meta.GetRefNames())
	before := notifications.Load()
	meta.NotifyStatus(api.ConnectionDisconnected, "unavailable")
	require.Equal(t, before+1, notifications.Load())
	require.NoError(t, connection.DetachConnectionByRef(ctx, cw.ID, dburl))
	require.Zero(t, meta.GetRefCount())
	require.Empty(t, meta.GetRefNames())
	require.NoError(t, connection.DropNameConnection(ctx, cw.ID))
}

func TestLookupRejectsUndialedConnection(t *testing.T) {
	testSQLConnectRejectsUndialedConnection(t, &SqlLookupSource{})
}
