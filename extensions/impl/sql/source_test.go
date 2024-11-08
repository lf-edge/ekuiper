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
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/store"
)

func init() {
	modules.RegisterConnection("sql", client.CreateConnection)
}

var (
	address = "localhost"
	port    = 33061
)

func TestProvisionErr(t *testing.T) {
	ctx := context.Background()
	testcases := []struct {
		props map[string]interface{}
		err   error
	}{
		{
			props: map[string]interface{}{},
			err:   errors.New("interval should be defined"),
		},
		{
			props: map[string]interface{}{
				"interval": "0s",
			},
			err: errors.New("interval should be defined"),
		},
		{
			props: map[string]interface{}{
				"interval": "1s",
			},
			err: errors.New("dburl should be defined"),
		},
		{
			props: map[string]interface{}{
				"interval": "1s",
				"dburl":    "123",
			},
			err: errors.New("dburl.Parse 123 fail with error: parse driver err:invalid database scheme"),
		},
	}
	for _, tc := range testcases {
		err := GetSource().Provision(ctx, tc.props)
		require.Equal(t, tc.err, err)
	}
}

func TestProvisionMockErr(t *testing.T) {
	ctx := context.Background()
	props := map[string]interface{}{
		"interval": "1s",
		"dburl":    "mysql://root:@127.0.0.1:4000/test",
	}
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/extensions/impl/sql/MapToStructErr", "return(true)")
	err := GetSource().Provision(ctx, props)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/extensions/impl/sql/MapToStructErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/extensions/impl/sql/GetQueryGeneratorErr", "return(true)")
	err = GetSource().Provision(ctx, props)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/extensions/impl/sql/GetQueryGeneratorErr")
}

func TestSQLConnectionConnect(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	props := map[string]interface{}{
		"interval": "1s",
		"dburl":    fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"templateSqlQueryCfg": map[string]interface{}{
			"templateSql": "select a,b from t",
		},
	}
	sqlSource := GetSource()
	require.NoError(t, sqlSource.Provision(ctx, props))
	require.NoError(t, sqlSource.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	sqlConnector, ok := sqlSource.(*SQLSourceConnector)
	require.True(t, ok)
	expectedData := map[string]any{
		"a": int64(1),
		"b": int64(1),
	}
	sqlConnector.queryData(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		require.Equal(t, expectedData, data)
	}, func(ctx api.StreamContext, err error) {
		require.NoError(t, err)
	})
	// query data Error
	testcases := []struct {
		path string
	}{
		{
			path: "StatementErr",
		},
		{
			path: "QueryErr",
		},
		{
			path: "ColumnTypesErr",
		},
		{
			path: "ScanErr",
		},
	}
	for _, tc := range testcases {
		fp := "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/" + tc.path
		failpoint.Enable(fp, "return(true)")
		sqlConnector.queryData(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) {
			require.Error(t, err)
		})
		failpoint.Disable(fp)
	}
	sqlSource.Close(ctx)
}

func TestSQLConnectionErr(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	props := map[string]interface{}{
		"interval": "1s",
		"dburl":    "mysql://mock:@mock:123/mock",
		"templateSqlQueryCfg": map[string]interface{}{
			"templateSql": "select a,b from t",
		},
	}
	sqlSource := GetSource()
	require.NoError(t, sqlSource.Provision(ctx, props))
	require.Error(t, sqlSource.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
}

func TestSQLSourceRewind(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	props := map[string]interface{}{
		"interval": "1s",
		"dburl":    fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"internalSqlQueryCfg": map[string]interface{}{
			"table": "t",
			"limit": 1,
			"indexFields": []map[string]interface{}{
				{
					"indexField":     "a",
					"indexValue":     0,
					"indexFieldType": "bigint",
				},
				{
					"indexField":     "b",
					"indexValue":     0,
					"indexFieldType": "bigint",
				},
			},
		},
	}
	sqlSource := GetSource()
	require.NoError(t, sqlSource.Provision(ctx, props))
	require.NoError(t, sqlSource.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	sqlConnector, ok := sqlSource.(*SQLSourceConnector)
	require.True(t, ok)
	expectedData := map[string]any{
		"a": int64(1),
		"b": int64(1),
	}
	dataChan := make(chan any, 1)
	sqlConnector.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		dataChan <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, expectedData, <-dataChan)
	state, err := sqlConnector.GetOffset()
	require.NoError(t, err)
	expectState := store.NewIndexFieldWrap([]*store.IndexField{
		{
			IndexFieldName:     "a",
			IndexFieldValue:    int64(1),
			IndexFieldDataType: "bigint",
		},
		{
			IndexFieldName:     "b",
			IndexFieldValue:    int64(1),
			IndexFieldDataType: "bigint",
		},
	}...).GetStore()
	require.Equal(t, expectState, state)
	require.NoError(t, sqlConnector.ResetOffset(map[string]interface{}{
		"a": int64(2),
		"b": int64(2),
	}))
	expectState2 := store.NewIndexFieldWrap([]*store.IndexField{
		{
			IndexFieldName:     "a",
			IndexFieldValue:    int64(2),
			IndexFieldDataType: "bigint",
		},
		{
			IndexFieldName:     "b",
			IndexFieldValue:    int64(2),
			IndexFieldDataType: "bigint",
		},
	}...).GetStore()
	state2, err := sqlConnector.GetOffset()
	require.NoError(t, err)
	require.Equal(t, expectState2, state2)

	require.NoError(t, sqlConnector.Rewind(expectState))
	gotState, err := sqlConnector.GetOffset()
	require.NoError(t, err)
	require.Equal(t, expectState, gotState)
}

func TestSQLReconnect(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
	props := map[string]interface{}{
		"interval": "1s",
		"dburl":    fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
		"templateSqlQueryCfg": map[string]interface{}{
			"templateSql": "select a,b from t",
		},
	}
	sqlSource := GetSource()
	require.NoError(t, sqlSource.Provision(ctx, props))
	require.Error(t, sqlSource.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	sqlConnector, ok := sqlSource.(*SQLSourceConnector)
	require.True(t, ok)
	sqlConnector.queryData(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) {})
	require.True(t, sqlConnector.needReconnect)

	sqlConnector.queryData(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) {
		require.Error(t, err)
	})
	require.True(t, sqlConnector.needReconnect)

	// start server then reconnect
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer s.Close()
	sqlConnector.queryData(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) {})
	require.False(t, sqlConnector.needReconnect)
}

func TestSQLConfURL(t *testing.T) {
	testcases := []struct {
		props map[string]any
		got   *SQLConf
		exp   *SQLConf
	}{
		{
			props: map[string]any{
				"dburl": "321",
				"url":   "123",
			},
			got: &SQLConf{
				DBUrl: "321",
				URL:   "123",
			},
			exp: &SQLConf{
				DBUrl: "321",
			},
		},
		{
			props: map[string]any{
				"url": "321",
			},
			got: &SQLConf{
				URL: "321",
			},
			exp: &SQLConf{
				DBUrl: "321",
			},
		},
	}
	for _, tc := range testcases {
		g, err := tc.got.resolveDBURL(tc.props)
		require.NoError(t, err)
		require.Equal(t, "321", g["dburl"])
		require.Equal(t, tc.exp, tc.got)
	}
}

func TestBuildScanValueByColumnType(t *testing.T) {
	testcases := []struct {
		colType string
		exp     interface{}
	}{
		{
			colType: "ntext",
			exp:     new(string),
		},
		{
			colType: "nchar",
			exp:     new(string),
		},
		{
			colType: "nchar",
			exp:     new(string),
		},
		{
			colType: "varchar",
			exp:     new(string),
		},
		{
			colType: "char",
			exp:     new(string),
		},
		{
			colType: "text",
			exp:     new(string),
		},
		{
			colType: "DECIMAL",
			exp:     new(float64),
		},
		{
			colType: "NUMERIC",
			exp:     new(float64),
		},
		{
			colType: "FLOAT",
			exp:     new(float64),
		},
		{
			colType: "REAL",
			exp:     new(float64),
		},
		{
			colType: "BOOL",
			exp:     new(bool),
		},
		{
			colType: "int",
			exp:     new(int64),
		},
		{
			colType: "bigint",
			exp:     new(int64),
		},
		{
			colType: "smallint",
			exp:     new(int64),
		},
		{
			colType: "tinyint",
			exp:     new(int64),
		},
	}
	ctx := mockContext.NewMockContext("1", "2")
	for _, tc := range testcases {
		got := buildScanValueByColumnType(ctx, "col", tc.colType, false)
		require.Equal(t, tc.exp, got)
	}
	testcases2 := []struct {
		colType string
		exp     interface{}
	}{
		{
			colType: "varchar",
			exp:     &sql.NullString{},
		},
		{
			colType: "DECIMAL",
			exp:     &sql.NullFloat64{},
		},
		{
			colType: "BOOL",
			exp:     &sql.NullBool{},
		},
		{
			colType: "int",
			exp:     &sql.NullInt64{},
		},
	}
	for _, tc := range testcases2 {
		got := buildScanValueByColumnType(ctx, "col", tc.colType, true)
		require.Equal(t, tc.exp, got)
	}
}
