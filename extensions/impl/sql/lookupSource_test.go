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

package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSQLLookupSourceErr(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := connection.WithLookupRefID(context.Background(), "lookup:mysqltest")
	props := map[string]interface{}{
		"dburl":              fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"datasource":         "t",
		"connectionSelector": "not-existed-connection",
	}
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, props))
	require.Error(t, ls.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
}

func TestSQLLookupSource(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := connection.WithLookupRefID(context.Background(), "lookup:mysqltest")
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	props := map[string]interface{}{
		"dburl":      fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"datasource": "t",
	}
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, props))
	require.NoError(t, ls.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	// The connection is attached lazily, so the very first lookup may
	// report not-ready once; the next one waits for the pooled connection.
	require.Eventually(t, func() bool {
		got, err := ls.Lookup(ctx, []string{"a", "b"}, []string{"a"}, []any{1})
		return err == nil && len(got) == 1
	}, 10*time.Second, 100*time.Millisecond)
	got, err := ls.Lookup(ctx, []string{"a", "b"}, []string{"a", "b"}, []any{1, 1})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"a": int64(1), "b": int64(1)}}, got)
	ls.Close(ctx)

	props = map[string]interface{}{
		"dburl":      fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"datasource": "t",
		"templateSqlQueryCfg": map[string]interface{}{
			"templateSql": "select * from t where b = {{.bid}}",
		},
	}
	ls = &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, props))
	require.NoError(t, ls.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	require.Eventually(t, func() bool {
		got, err := ls.Lookup(ctx, []string{"a", "b"}, []string{"bid"}, []any{1})
		return err == nil && len(got) == 1
	}, 10*time.Second, 100*time.Millisecond)
	ls.Close(ctx)
}

func TestSQLLookupSourceProvisionErr(t *testing.T) {
	props := map[string]interface{}{}
	ls := &SqlLookupSource{}
	ctx := mockContext.NewMockContext("1", "2")
	require.Error(t, ls.Provision(ctx, props))
	props = map[string]interface{}{
		"dburl":      fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"datasource": "t",
	}
	props = map[string]interface{}{
		"dburl":      123,
		"datasource": "t",
	}
	require.Error(t, ls.Provision(ctx, props))
	props = map[string]interface{}{
		"dburl":      "123",
		"datasource": "t",
	}
	require.Error(t, ls.Provision(ctx, props))
}

func TestSQLLookupReconnect(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := connection.WithLookupRefID(context.Background(), "lookup:mysqltest")
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	props := map[string]interface{}{
		"dburl":      fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"datasource": "t",
	}
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Provision(ctx, props))
	require.NoError(t, ls.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	require.Eventually(t, func() bool {
		got, err := ls.Lookup(ctx, []string{"a", "b"}, []string{"a"}, []any{1})
		return err == nil && len(got) == 1
	}, 10*time.Second, 100*time.Millisecond)
	// Simulate a runtime disconnect: break the established sessions and
	// close the server.
	require.NoError(t, testx.KillSessions(s))
	s.Close()
	_, err = ls.Lookup(ctx, []string{"a", "b"}, []string{"a"}, []any{1})
	require.Error(t, err)
	s, err = testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	// The failed lookup hands the episode to the Pool worker, which
	// keeps recovering until the database is back; later lookups park
	// on WaitReady meanwhile and continue afterwards.
	require.Eventually(t, func() bool {
		got, err := ls.Lookup(ctx, []string{"a", "b"}, []string{"a"}, []any{1})
		return err == nil && len(got) == 1
	}, 60*time.Second, 200*time.Millisecond)
	got, err := ls.Lookup(ctx, []string{"a", "b"}, []string{"a"}, []any{1})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"a": int64(1), "b": int64(1)}}, got)
}

func TestLookupPing(t *testing.T) {
	connection.InitConnectionManager4Test()
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	props := map[string]interface{}{
		"dburl":      fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"datasource": "t",
	}
	ls := &SqlLookupSource{}
	require.NoError(t, ls.Ping(context.Background(), props))
	props = map[string]interface{}{
		"dburl":      "",
		"datasource": "t",
	}
	require.Error(t, ls.Ping(context.Background(), props))
}
