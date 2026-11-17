// Copyright 2025 EMQ Technologies Co., Ltd.
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

//go:build parquet || full

package parquet

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestBuildParquetSchema(t *testing.T) {
	data := make(map[string]interface{})
	data["k1"] = "v1"
	data["k2"] = int64(1)
	data["k3"] = float64(3)
	data["k4"] = true
	data["k5"] = []interface{}{int64(1)}
	data["k6"] = map[string]interface{}{
		"k7": "v8",
	}
	ctx := mockContext.NewMockContext("test", "test")
	p := &ParquetWriter{}
	content, err := p.Encode(ctx, data)
	require.NoError(t, err)
	require.NotNil(t, content)
	validateParquetFile(t, content, data)
}

func validateParquetFile(t *testing.T, content []byte, expected map[string]any) {
	file, err := parquet.OpenFile(bytes.NewReader(content), int64(len(content)))
	require.NoError(t, err)
	require.Equal(t, int64(1), file.NumRows())
	rows := file.RowGroups()[0].Rows()
	defer rows.Close()
	values := make([]parquet.Row, 1)
	n, err := rows.ReadRows(values)
	require.Equal(t, 1, n)
	require.NoError(t, err)
	actual := make(map[string]any)
	require.NoError(t, file.Schema().Reconstruct(&actual, values[0]))
	// The low-level reader exposes Parquet's three-level LIST layout.
	expected["k5"] = map[string]any{"list": []any{map[string]any{"element": int64(1)}}}
	require.Equal(t, expected, actual)
}
