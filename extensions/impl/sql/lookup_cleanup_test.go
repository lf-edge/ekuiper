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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestLookupSharedConnectionLifecycle(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext(t.Name(), "lookup")
	dburl := "sqlite://" + filepath.Join(t.TempDir(), "shared.db")
	cw, err := connection.CreateNamedConnection(ctx, "lookup-shared", "sql", map[string]any{"dburl": dburl})
	require.NoError(t, err)
	c, err := cw.Wait(ctx)
	require.NoError(t, err)
	db := c.(*client.SQLConnection).GetDB()
	_, err = db.Exec("CREATE TABLE t(a INTEGER); INSERT INTO t VALUES(7)")
	require.NoError(t, err)
	meta, err := connection.GetConnectionDetail(ctx, cw.ID)
	require.NoError(t, err)
	var lookups [2]SqlLookupSource
	for i := range lookups {
		s := &lookups[i]
		require.NoError(t, s.Provision(ctx, map[string]any{
			"dburl": dburl, "connectionSelector": cw.ID, "datasource": "t",
		}))
		require.NoError(t, s.Connect(ctx, nil))
		t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	}
	require.Equal(t, 2, meta.GetRefCount())
	require.NoError(t, lookups[0].Close(ctx))
	require.NoError(t, lookups[0].Close(ctx))
	require.Equal(t, 1, meta.GetRefCount())
	rows, err := lookups[1].Lookup(ctx, []string{"a"}, []string{"a"}, []any{7})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"a": int64(7)}}, rows)
	require.NoError(t, lookups[1].Close(ctx))
	require.Zero(t, meta.GetRefCount())
	require.NoError(t, db.Ping(), "releasing lookup references must retain a named connection")
	require.NoError(t, connection.DropNameConnection(ctx, cw.ID))
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
	require.Zero(t, meta.GetRefCount())
	require.Empty(t, meta.GetRefNames())
	require.NoError(t, connection.DropNameConnection(ctx, cw.ID))
}
