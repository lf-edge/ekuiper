// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
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
		require.NoError(t, sqlSink.Connect(ctx, func(status string, message string) {
			// do nothing
		}))
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

	// insert 5,5 6,6
	sqlSink := &SQLSinkConnector{}
	require.NoError(t, sqlSink.Provision(ctx, map[string]interface{}{
		"dburl": dburl,
		"table": tableName,
	}))
	require.NoError(t, sqlSink.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
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

	// insert 7,7 8,8
	sqlSink = &SQLSinkConnector{}
	require.NoError(t, sqlSink.Provision(ctx, map[string]interface{}{
		"dburl":        dburl,
		"table":        tableName,
		"rowKindField": "action",
		"keyField":     "a",
		"fields":       []string{"a", "b"},
	}))
	require.NoError(t, sqlSink.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	require.NoError(t, sqlSink.collectList(ctx, []map[string]any{
		{
			"a":      7,
			"b":      7,
			"action": "insert",
		},
		{
			"a":      8,
			"b":      8,
			"action": "insert",
		},
	}))

	got = [][]int{}
	rows, err = sqlSink.conn.GetDB().Query("select a,b from t where a >=7 and b >=7")
	require.NoError(t, err)
	for rows.Next() {
		var a int
		var b int
		require.NoError(t, rows.Scan(&a, &b))
		got = append(got, []int{a, b})
	}
	sqlSink.Close(ctx)
	require.Equal(t, [][]int{{7, 7}, {8, 8}}, got)
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
	_, err := config.getValuesByKeys(ctx, nil, config.Fields)
	require.Error(t, err)

	values, err := config.getValuesByKeys(ctx, map[string]interface{}{
		"a": "value",
	}, config.Fields)
	require.NoError(t, err)
	require.Equal(t, []string{"'value'"}, values)

	config = &sqlSinkConfig{
		Fields: []string{"a"},
	}
	values, err = config.getValuesByKeys(ctx, map[string]interface{}{
		"b": "value",
	}, config.Fields)
	require.NoError(t, err)
	require.Equal(t, []string{"NULL"}, values)

	config = &sqlSinkConfig{}
	values, err = config.getValuesByKeys(ctx, map[string]interface{}{
		"a": "value",
	}, []string{"a"})
	require.NoError(t, err)
	require.Equal(t, []string{"'value'"}, values)
}

func TestSQLSinkAction(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	tableName := "t"
	dburl := fmt.Sprintf("mysql://root:@%v:%v/test", address, port)
	sqlSink := &SQLSinkConnector{}
	require.NoError(t, sqlSink.Provision(ctx, map[string]interface{}{
		"dburl":        dburl,
		"table":        tableName,
		"fields":       []string{"a", "b"},
		"rowKindField": "action",
		"keyField":     "a",
	}))
	require.NoError(t, sqlSink.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	// update
	require.NoError(t, sqlSink.collect(ctx, map[string]any{
		"a":      1,
		"b":      2,
		"action": "update",
	}))
	got := [][]int{}
	rows, err := sqlSink.conn.GetDB().Query("select a,b from t where a=1")
	require.NoError(t, err)
	for rows.Next() {
		var a int
		var b int
		require.NoError(t, rows.Scan(&a, &b))
		got = append(got, []int{a, b})
	}
	require.Equal(t, [][]int{{1, 2}}, got)
	// delete
	require.NoError(t, sqlSink.collect(ctx, map[string]any{
		"a":      1,
		"b":      2,
		"action": "delete",
	}))
	got = [][]int{}
	rows, err = sqlSink.conn.GetDB().Query("select a,b from t where a=1")
	require.NoError(t, err)
	for rows.Next() {
		var a int
		var b int
		require.NoError(t, rows.Scan(&a, &b))
		got = append(got, []int{a, b})
	}
	require.Equal(t, [][]int{}, got)

	// invalid
	require.Error(t, sqlSink.collect(ctx, map[string]any{
		"a":      1,
		"b":      2,
		"action": "mock",
	}))
	require.NoError(t, sqlSink.Close(ctx))
}

func TestSQLSinkReconnect(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	tableName := "t"
	dburl := fmt.Sprintf("mysql://root:@%v:%v/test", address, port)
	sqlSink := &SQLSinkConnector{}
	require.NoError(t, sqlSink.Provision(ctx, map[string]interface{}{
		"dburl":        dburl,
		"table":        tableName,
		"fields":       []string{"a", "b"},
		"rowKindField": "action",
		"keyField":     "a",
	}))
	require.NoError(t, sqlSink.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	s.Close()
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/extensions/impl/sql/dbErr", "return(true)")
	// update
	require.Error(t, sqlSink.collect(ctx, map[string]any{
		"a":      1,
		"b":      2,
		"action": "update",
	}))
	require.True(t, sqlSink.needReconnect)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/extensions/impl/sql/dbErr")
	s, err = testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	require.NoError(t, sqlSink.collect(ctx, map[string]any{
		"a":      1,
		"b":      2,
		"action": "update",
	}))
	require.False(t, sqlSink.needReconnect)
}

func TestConsume(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		exp   map[string]any
	}{
		{
			name: "has fields",
			props: map[string]any{
				"actionField": "action",
				"keyField":    "a",
				"fields":      []string{"a", "b"},
				"other":       "other",
			},
			exp: map[string]any{
				"actionField": "action",
				"keyField":    "a",
				"other":       "other",
			},
		},
		{
			name: "no fields",
			props: map[string]any{
				"actionField": "action",
				"keyField":    "a",
				"dumbfields":  []string{"a", "b"},
				"other":       "other",
			},
			exp: map[string]any{
				"actionField": "action",
				"keyField":    "a",
				"dumbfields":  []string{"a", "b"},
				"other":       "other",
			},
		},
	}
	s := &SQLSinkConnector{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s.Consume(test.props)
			require.Equal(t, test.exp, test.props)
		})
	}
}
