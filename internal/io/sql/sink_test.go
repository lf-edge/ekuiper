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

func TestSQLSink(t *testing.T) {
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
		"dburl": dburl,
		"table": tableName,
	}))
	require.NoError(t, sqlSink.Connect(ctx))
	require.NoError(t, sqlSink.collect(ctx, map[string]interface{}{
		"a": 2,
		"b": 2,
	}))
	rows, err := sqlSink.conn.GetDB().Query("select a,b from t where a = 2 and b = 2")
	require.NoError(t, err)
	for rows.Next() {
		var a int
		var b int
		require.NoError(t, rows.Scan(&a, &b))
		require.Equal(t, 2, a)
		require.Equal(t, 2, a)
	}
}
