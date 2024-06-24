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

package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/sql/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSQLSinkCollect(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	tableName := "t"
	dburl := fmt.Sprintf("mysql://root:@%v:%v/test", address, port)
	testcases := []struct {
		props map[string]any
		data  map[string]any
		a     int
		b     int
	}{
		{
			props: map[string]interface{}{
				"dburl": dburl,
				"table": tableName,
			},
			data: map[string]any{
				"a": 2,
				"b": 2,
			},
			a: 2,
			b: 2,
		},
		{
			props: map[string]interface{}{
				"dburl":  dburl,
				"table":  tableName,
				"fields": []string{"a", "b"},
			},
			data: map[string]any{
				"a": 3,
				"b": 3,
				"c": 3,
			},
			a: 3,
			b: 3,
		},
		{
			props: map[string]interface{}{
				"dburl":        dburl,
				"table":        tableName,
				"fields":       []string{"a", "b"},
				"rowKindField": "action",
				"keyField":     "a",
			},
			data: map[string]any{
				"a":      4,
				"b":      4,
				"c":      4,
				"action": "insert",
			},
			a: 4,
			b: 4,
		},
	}
	for _, tc := range testcases {
		sqlSink := &SQLSinkConnector{}
		require.NoError(t, sqlSink.Provision(ctx, tc.props))
		require.NoError(t, sqlSink.Connect(ctx))
		require.NoError(t, sqlSink.collect(ctx, tc.data))
		rows, err := sqlSink.conn.GetDB().Query(fmt.Sprintf("select a,b from t where a = %v and b = %v", tc.a, tc.b))
		require.NoError(t, err)
		count := 0
		for rows.Next() {
			count++
			var a int
			var b int
			require.NoError(t, rows.Scan(&a, &b))
			require.Equal(t, tc.a, a)
			require.Equal(t, tc.b, b)
		}
		sqlSink.Close(ctx)
		require.Equal(t, 1, count)
	}

	// insert 4,4  5,5
	sqlSink := &SQLSinkConnector{}
	require.NoError(t, sqlSink.Provision(ctx, map[string]interface{}{
		"dburl": dburl,
		"table": tableName,
	}))
	require.NoError(t, sqlSink.Connect(ctx))
	require.NoError(t, sqlSink.collectList(ctx, []map[string]any{
		{
			"a": 5,
			"b": 5,
		},
		{
			"a": 6,
			"b": 6,
		},
	}))

	rows, err := sqlSink.conn.GetDB().Query("select a,b from t where a >=5 and b >=5")
	require.NoError(t, err)
	var got [][]int
	for rows.Next() {
		var a int
		var b int
		require.NoError(t, rows.Scan(&a, &b))
		got = append(got, []int{a, b})
	}
	sqlSink.Close(ctx)
	require.Equal(t, [][]int{{5, 5}, {6, 6}}, got)
}

func TestSQLProvisionErr(t *testing.T) {
	ctx := mockContext.NewMockContext("1", "2")
	sqlSink := &SQLSinkConnector{}
	require.Error(t, sqlSink.Provision(ctx, map[string]interface{}{}))
	require.Error(t, sqlSink.Provision(ctx, map[string]interface{}{
		"dburl": "123",
	}))
	require.Error(t, sqlSink.Provision(ctx, map[string]interface{}{
		"dburl":        "123",
		"table":        "123",
		"rowKindField": "123",
	}))
}

func TestSQLSinkConfigKV(t *testing.T) {
	ctx := mockContext.NewMockContext("1", "2")
	config := &sqlSinkConfig{
		Fields: []string{"a"},
	}
	_, _, err := config.getKeyValues(ctx, nil)
	require.Error(t, err)

	keys, values, err := config.getKeyValues(ctx, map[string]interface{}{
		"a": "value",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, keys)
	require.Equal(t, []string{"'value'"}, values)

	config = &sqlSinkConfig{
		Fields: []string{"a"},
	}
	keys, values, err = config.getKeyValues(ctx, map[string]interface{}{
		"b": "value",
	})
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, keys)
	require.Equal(t, []string{"NULL"}, values)

	config = &sqlSinkConfig{}
	keys, values, err = config.getKeyValues(ctx, map[string]interface{}{
		"a": "value",
	})
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, keys)
	require.Equal(t, []string{"'value'"}, values)
}
