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

package connection

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

func TestConnection(t *testing.T) {
	InitConnectionManagerInTest()
	ctx := context.Background()
	conn, err := CreateNamedConnection(ctx, "id1", "mock", nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.NoError(t, conn.Ping(ctx))
	require.Equal(t, 0, conn.Ref(ctx))
	conn.Attach(ctx)
	require.Equal(t, 1, conn.Ref(ctx))
	conn.Attach(ctx)
	require.Equal(t, 2, conn.Ref(ctx))
	conn.DetachPub(ctx, nil)
	require.Equal(t, 1, conn.Ref(ctx))
	err = DropNameConnection(ctx, "id1")
	require.Error(t, err)
	conn2, err := GetNameConnection("id1")
	require.NoError(t, err)
	require.NotNil(t, conn2)
	conn.DetachSub(ctx, nil)
	require.Equal(t, 0, conn.Ref(ctx))
	err = DropNameConnection(ctx, "id1")
	require.NoError(t, err)
	conn3, err := GetNameConnection("id1")
	require.Error(t, err)
	require.Nil(t, conn3)

	_, err = CreateNamedConnection(ctx, "", "mock", nil)
	require.Error(t, err)
	err = DropNameConnection(ctx, "")
	require.Error(t, err)
	_, err = CreateNamedConnection(ctx, "12", "unkown", nil)
	require.Error(t, err)
}
