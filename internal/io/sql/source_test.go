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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/io/sql/client"
	"github.com/lf-edge/ekuiper/v2/internal/io/sql/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
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
			err: errors.New("dburl.Parse 123 fail with error: parse driver err:invalid database scheme, support drivers:[mysql]"),
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
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/sql/MapToStructErr", "return(true)")
	err := GetSource().Provision(ctx, props)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/sql/MapToStructErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/sql/GetQueryGeneratorErr", "return(true)")
	err = GetSource().Provision(ctx, props)
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/sql/GetQueryGeneratorErr")
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
	require.NoError(t, sqlSource.Connect(ctx))
	sqlConnector, ok := sqlSource.(*SQLSourceConnector)
	require.True(t, ok)
	expectedData := map[string]any{
		"a": int64(1),
		"b": int64(1),
	}
	sqlConnector.queryData(ctx, func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
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
		fp := "github.com/lf-edge/ekuiper/v2/internal/io/sql/" + tc.path
		failpoint.Enable(fp, "return(true)")
		sqlConnector.queryData(ctx, func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) {
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
	require.NoError(t, sqlSource.Connect(ctx))
}
