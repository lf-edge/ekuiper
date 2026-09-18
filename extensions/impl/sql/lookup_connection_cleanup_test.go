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
	_ "modernc.org/sqlite"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestLookupConnectionFailureCleanup(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("lookup_cleanup", "op1")

	failed := &SqlLookupSource{
		conf:  &SQLConf{DBUrl: "unknown-driver://database"},
		props: map[string]any{"dburl": "unknown-driver://database"},
	}
	require.Error(t, failed.Connect(ctx, nil))
	_, err := connection.GetConnectionDetail(ctx, "unknown-driver://database")
	require.Error(t, err, "a failed lookup connection must not remain in the pool")

	validURL := fmt.Sprintf("sqlite://%s/lookup.db", t.TempDir())
	valid := &SqlLookupSource{
		conf:  &SQLConf{DBUrl: validURL},
		props: map[string]any{"dburl": validURL},
	}
	require.NoError(t, valid.Connect(ctx, nil))
	require.NoError(t, valid.Close(ctx))
}
