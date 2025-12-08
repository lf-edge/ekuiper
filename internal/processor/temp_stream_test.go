// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package processor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestTempStream(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Test 1: Create a temp stream
	t.Run("create temp stream", func(t *testing.T) {
		results, err := p.ExecStmt(`CREATE STREAM temp_stream1 (id BIGINT, name STRING) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
		require.NoError(t, err)
		require.Equal(t, []string{"Stream temp_stream1 is created."}, results)
	})

	// Test 2: Verify temp stream can be described
	t.Run("describe temp stream", func(t *testing.T) {
		results, err := p.ExecStmt(`DESCRIBE STREAM temp_stream1;`)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Contains(t, results[0], "id")
		require.Contains(t, results[0], "name")
	})

	// Test 3: Verify temp stream is NOT persisted in database
	t.Run("temp stream not in database", func(t *testing.T) {
		ok, err := p.db.Get("temp_stream1", nil)
		require.NoError(t, err)
		require.False(t, ok, "Temp stream should not be persisted in database")
	})

	// Test 4: Create a normal (non-temp) stream for comparison
	t.Run("create normal stream", func(t *testing.T) {
		results, err := p.ExecStmt(`CREATE STREAM normal_stream1 (id BIGINT, name STRING) WITH (DATASOURCE="test", FORMAT="JSON");`)
		require.NoError(t, err)
		require.Equal(t, []string{"Stream normal_stream1 is created."}, results)
	})

	// Test 5: Verify normal stream IS persisted in database
	t.Run("normal stream in database", func(t *testing.T) {
		var v string
		ok, err := p.db.Get("normal_stream1", &v)
		require.NoError(t, err)
		require.True(t, ok, "Normal stream should be persisted in database")
	})

	// Test 6: Verify temp stream appears in SHOW STREAMS
	t.Run("temp stream in show streams", func(t *testing.T) {
		results, err := p.ExecStmt(`SHOW STREAMS;`)
		require.NoError(t, err)
		// Only normal_stream1 should be in the results since temp_stream1 is not in DB
		require.Contains(t, results, "normal_stream1")
		require.Contains(t, results, "temp_stream1")
	})

	// Test 7: Verify cannot replace with temp option
	t.Run("cannot replace with temp", func(t *testing.T) {
		result, err := p.ExecReplaceStream("normal_stream1", `CREATE STREAM normal_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`, ast.TypeStream)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot replace with temp option")
		require.Empty(t, result)
	})

	// Test 8: Drop temp stream
	t.Run("drop temp stream", func(t *testing.T) {
		results, err := p.ExecStmt(`DROP STREAM temp_stream1;`)
		require.NoError(t, err)
		require.Equal(t, []string{"Stream temp_stream1 is dropped."}, results)
	})
}

func TestTempTable(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Test 1: Create a temp table
	t.Run("create temp table", func(t *testing.T) {
		results, err := p.ExecStmt(`CREATE TABLE temp_table1 (id BIGINT, name STRING) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
		require.NoError(t, err)
		require.Equal(t, []string{"Table temp_table1 is created."}, results)
	})

	// Test 2: Verify temp table is NOT persisted in database
	t.Run("temp table not in database", func(t *testing.T) {
		ok, err := p.db.Get("temp_table1", nil)
		require.NoError(t, err)
		require.False(t, ok, "Temp table should not be persisted in database")
	})

	// Test 3: Create a normal (non-temp) table for comparison
	t.Run("create normal table", func(t *testing.T) {
		results, err := p.ExecStmt(`CREATE TABLE normal_table1 (id BIGINT, name STRING) WITH (DATASOURCE="test", FORMAT="JSON");`)
		require.NoError(t, err)
		require.Equal(t, []string{"Table normal_table1 is created."}, results)
	})

	// Test 4: Verify normal table IS persisted in database
	t.Run("normal table in database", func(t *testing.T) {
		var v string
		ok, err := p.db.Get("normal_table1", &v)
		require.NoError(t, err)
		require.True(t, ok, "Normal table should be persisted in database")
	})

	// Test 5: Verify cannot replace with temp option
	t.Run("cannot replace with temp", func(t *testing.T) {
		result, err := p.ExecReplaceStream("normal_table1", `CREATE TABLE normal_table1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`, ast.TypeTable)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot replace with temp option")
		require.Empty(t, result)
	})
}

func TestTempStream_ShowTables(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create temp and normal tables
	_, err := p.ExecStmt(`CREATE TABLE temp_table1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
	require.NoError(t, err)
	_, err = p.ExecStmt(`CREATE TABLE normal_table1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON");`)
	require.NoError(t, err)

	// Test SHOW TABLES includes both
	results, err := p.ExecStmt(`SHOW TABLES;`)
	require.NoError(t, err)
	require.Contains(t, results, "temp_table1")
	require.Contains(t, results, "normal_table1")
}

func TestTempStream_GetAll(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create temp and normal streams
	_, err := p.ExecStmt(`CREATE STREAM temp_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
	require.NoError(t, err)
	_, err = p.ExecStmt(`CREATE STREAM normal_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON");`)
	require.NoError(t, err)
	_, err = p.ExecStmt(`CREATE TABLE temp_table1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
	require.NoError(t, err)
	_, err = p.ExecStmt(`CREATE TABLE normal_table1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON");`)
	require.NoError(t, err)

	// Test GetAll returns both temp and normal streams/tables
	all, err := p.GetAll()
	require.NoError(t, err)
	require.NotNil(t, all)
	require.Contains(t, all["streams"], "temp_stream1")
	require.Contains(t, all["streams"], "normal_stream1")
	require.Contains(t, all["tables"], "temp_table1")
	require.Contains(t, all["tables"], "normal_table1")
}

func TestTempStream_DescStream(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create temp stream
	_, err := p.ExecStmt(`CREATE STREAM temp_stream1 (id BIGINT, name STRING) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
	require.NoError(t, err)

	// Test DescStream
	stmt, err := p.DescStream("temp_stream1", ast.TypeStream)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	streamStmt, ok := stmt.(*ast.StreamStmt)
	require.True(t, ok)
	require.Equal(t, "temp_stream1", string(streamStmt.Name))
	require.True(t, streamStmt.Options.Temp)
}

func TestTempStream_GetStream(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create temp stream
	_, err := p.ExecStmt(`CREATE STREAM temp_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
	require.NoError(t, err)

	// Test GetStream
	statement, err := p.GetStream("temp_stream1", ast.TypeStream)
	require.NoError(t, err)
	require.Contains(t, statement, "temp_stream1")
	require.Contains(t, statement, "TEMP")

	// Test GetStream for non-existent stream
	_, err = p.GetStream("nonexistent", ast.TypeStream)
	require.Error(t, err)
}

func TestTempStream_ShowTable_WithKind(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create temp scan table
	_, err := p.ExecStmt(`CREATE TABLE temp_scan_table (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true, KIND="scan");`)
	require.NoError(t, err)

	// Test ShowTable with kind filter
	results, err := p.ShowTable("scan")
	require.NoError(t, err)
	require.Contains(t, results, "temp_scan_table")
}

func TestTempStream_DropNonExistent(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Try to drop non-existent temp stream
	_, err := p.DropStream("nonexistent_temp", ast.TypeStream)
	require.Error(t, err)
}

func TestTempStream_ReplaceNormalWithTemp(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create normal stream
	_, err := p.ExecStmt(`CREATE STREAM normal_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON");`)
	require.NoError(t, err)

	// Try to replace with temp stream
	_, err = p.ExecReplaceStream("normal_stream1", `CREATE STREAM normal_stream1 (id BIGINT, name STRING) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`, ast.TypeStream)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot replace with temp option")
}

func TestTempStream_MultipleOperations(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create multiple temp streams
	for i := 1; i <= 5; i++ {
		streamName := "temp_stream" + string(rune('0'+i))
		sql := `CREATE STREAM ` + streamName + ` (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`
		_, err := p.ExecStmt(sql)
		require.NoError(t, err)
	}

	// Verify all are in SHOW STREAMS
	results, err := p.ExecStmt(`SHOW STREAMS;`)
	require.NoError(t, err)
	for i := 1; i <= 5; i++ {
		streamName := "temp_stream" + string(rune('0'+i))
		require.Contains(t, results, streamName)
	}

	// Drop all temp streams
	for i := 1; i <= 5; i++ {
		streamName := "temp_stream" + string(rune('0'+i))
		_, err := p.DropStream(streamName, ast.TypeStream)
		require.NoError(t, err)
	}

	// Verify all are gone
	results, err = p.ExecStmt(`SHOW STREAMS;`)
	require.NoError(t, err)
	for i := 1; i <= 5; i++ {
		streamName := "temp_stream" + string(rune('0'+i))
		require.NotContains(t, results, streamName)
	}
}

func TestGetDataSource(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create a temp stream
	_, err := p.ExecStmt(`CREATE STREAM temp_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
	require.NoError(t, err)

	// Create a normal stream
	_, err = p.ExecStmt(`CREATE STREAM normal_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON");`)
	require.NoError(t, err)

	// Test GetDataSource for temp stream
	stmt, err := p.GetDataSource("temp_stream1")
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.Equal(t, "temp_stream1", string(stmt.Name))
	require.True(t, stmt.Options.Temp)

	// Test GetDataSource for normal stream
	stmt, err = p.GetDataSource("normal_stream1")
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.Equal(t, "normal_stream1", string(stmt.Name))
	require.False(t, stmt.Options.Temp)

	// Test GetDataSource for non-existent stream
	_, err = p.GetDataSource("nonexistent")
	require.Error(t, err)
}

func TestGetStreamProcessorDataSource(t *testing.T) {
	p := NewStreamProcessor()
	p.db.Clean()
	defer p.db.Clean()

	// Create a temp stream
	_, err := p.ExecStmt(`CREATE STREAM temp_stream1 (id BIGINT) WITH (DATASOURCE="test", FORMAT="JSON", TEMP=true);`)
	require.NoError(t, err)

	// Test GetStreamProcessorDataSource for temp stream
	stmt, err := GetStreamProcessorDataSource("temp_stream1")
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.Equal(t, "temp_stream1", string(stmt.Name))
	require.True(t, stmt.Options.Temp)

	// Test GetStreamProcessorDataSource for non-existent stream
	_, err = GetStreamProcessorDataSource("nonexistent")
	require.Error(t, err)
}
