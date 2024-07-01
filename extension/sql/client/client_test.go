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

package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extension/sql/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

var (
	address = "localhost"
	port    = 33060
)

func TestSQLClient(t *testing.T) {
	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer func() {
		s.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	props := map[string]interface{}{
		"dburl": fmt.Sprintf("mysql://root:@%v:%v/test", address, port),
	}
	conn, err := CreateConnection(ctx, props)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))

	c, err := CreateClient(ctx, props)
	require.NoError(t, err)
	require.NotNil(t, c.GetDB())
	c.DetachSub(ctx, props)
	c.Close(ctx)
}
