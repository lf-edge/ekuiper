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

package client

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestCanceledReconnectPreservesSharedDB(t *testing.T) {
	ctx := mockContext.NewMockContext(t.Name(), "sql")
	conn := &SQLConnection{}
	require.NoError(t, conn.Provision(ctx, "shared", map[string]any{
		"dburl": "sqlite://" + filepath.Join(t.TempDir(), "shared.db"),
	}))
	require.NoError(t, conn.Dial(ctx))
	t.Cleanup(func() { require.NoError(t, conn.Close(ctx)) })
	db := conn.GetDB()
	canceled, cancel := ctx.WithCancel()
	cancel()
	require.ErrorIs(t, conn.Reconnect(canceled), canceled.Err())
	require.Same(t, db, conn.GetDB())
	require.NoError(t, db.Ping())
}
