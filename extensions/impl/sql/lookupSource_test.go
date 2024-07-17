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

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSQLLookupSource(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")
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
	require.NoError(t, ls.Connect(ctx))
	got, err := ls.Lookup(ctx, []string{"a", "b"}, []string{"a"}, []any{1})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{"a": int64(1), "b": int64(1)}}, got)
}
