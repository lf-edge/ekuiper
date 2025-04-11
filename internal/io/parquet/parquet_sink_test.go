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

package parquet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestParquetSink(t *testing.T) {
	schema := `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
		    {"Tag":"name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"},
		    {"Tag":"name=age, type=INT64"}
        ]
	}
`
	ctx := mockContext.NewMockContext("1", "2")
	p := &parquetSink{}
	require.NoError(t, p.Provision(ctx, map[string]any{
		"path":       "TestParquetSink.parquet",
		"jsonSchema": schema,
	}))
	p.Connect(ctx, nil)
	require.NoError(t, p.writeMsg(map[string]interface{}{
		"name": "abc",
		"age":  int64(19),
	}))
	p.Close(ctx)
	_, err := os.Stat("TestParquetSink.parquet")
	require.NoError(t, err)
	os.Remove("TestParquetSink.parquet")
}

func TestParquetSink_Rolling(t *testing.T) {
	schema := `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
		    {"Tag":"name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"},
		    {"Tag":"name=age, type=INT64"}
        ]
	}
`
	ctx := mockContext.NewMockContext("1", "2")
	p := &parquetSink{}
	require.NoError(t, p.Provision(ctx, map[string]any{
		"path":         "TestParquetSink.parquet",
		"jsonSchema":   schema,
		"rollingCount": 1,
	}))
	p.Connect(ctx, nil)
	require.NoError(t, p.writeMsg(map[string]interface{}{
		"name": "abc",
		"age":  int64(19),
	}))
	cnt, err := findByPrefix("TestParquetSink.parquet")
	require.NoError(t, err)
	require.Equal(t, 1, cnt)
	time.Sleep(time.Second)
	require.NoError(t, p.writeMsg(map[string]interface{}{
		"name": "abc",
		"age":  int64(19),
	}))
	cnt, err = findByPrefix("TestParquetSink.parquet")
	require.NoError(t, err)
	require.Equal(t, 2, cnt)
	p.Close(ctx)
	removeByPrefix("TestParquetSink.parquet")
}

func findByPrefix(prefix string) (int, error) {
	dir := "./"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
			count++
		}
	}
	return count, nil
}

func removeByPrefix(prefix string) error {
	dir := "./"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
			os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
	return nil
}
