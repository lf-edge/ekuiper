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

package pgvector

import (
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func testProps() map[string]any {
	return map[string]any{
		"url":         "postgres://user:pass@127.0.0.1:5432/testdb",
		"table":       "fault_history",
		"vectorField": "embedding",
		"topK":        3,
		"metric":      "COSINE",
	}
}

func TestValidate(t *testing.T) {
	s := &lookupSource{}
	assert.Error(t, s.Validate(map[string]any{"table": "t"}))
	assert.Error(t, s.Validate(map[string]any{"url": "postgres://x"}))
	assert.Error(t, s.Validate(map[string]any{"url": "postgres://x", "vectorField": "v"}))
	// identifier safety
	assert.Error(t, s.Validate(map[string]any{
		"url": "postgres://x", "table": "t; DROP TABLE users", "vectorField": "v",
	}))
	assert.Error(t, s.Validate(map[string]any{
		"url": "postgres://x", "table": "t", "vectorField": "v; --",
	}))
	assert.Error(t, s.Validate(map[string]any{
		"url": "postgres://x", "table": "t", "vectorField": "v", "metric": "bad",
	}))
	assert.NoError(t, s.Validate(testProps()))
	assert.Equal(t, "<=>", s.op)
	assert.Equal(t, 3, s.c.TopK)

	// operators per metric
	s2 := &lookupSource{}
	assert.NoError(t, s2.Validate(map[string]any{
		"url": "postgres://x", "table": "t", "vectorField": "v", "metric": "l2",
	}))
	assert.Equal(t, "<->", s2.op)

	s3 := &lookupSource{}
	assert.NoError(t, s3.Validate(map[string]any{
		"url": "postgres://x", "table": "t", "vectorField": "v", "metric": "ip",
	}))
	assert.Equal(t, "<#>", s3.op)

	// defaults
	s4 := &lookupSource{}
	assert.NoError(t, s4.Validate(map[string]any{
		"url": "postgres://x", "table": "t", "vectorField": "v",
	}))
	assert.Equal(t, defaultTopK, s4.c.TopK)
	assert.Equal(t, "COSINE", string(s4.metric))
}

func TestBuildQuery(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps()))
	query, args := s.buildQuery([]string{"id", "sop"}, []float64{0.1, 0.2}, nil, nil)
	assert.Equal(t, "SELECT id, sop FROM fault_history ORDER BY embedding <=> $1::vector LIMIT 3", query)
	assert.Equal(t, "[0.1,0.2]", args[0])

	// with filters: parameters start at 2
	query, args = s.buildQuery(nil, []float64{1, 2},
		[]string{"model = $2", "year = $3"}, []any{"A100", 2026})
	assert.Equal(t, "SELECT * FROM fault_history WHERE model = $2 AND year = $3 ORDER BY embedding <=> $1::vector LIMIT 3", query)
	assert.Len(t, args, 3)
	assert.Equal(t, "[1,2]", args[0])
	assert.Equal(t, "A100", args[1])
	assert.Equal(t, 2026, args[2])
}

func TestExtractQuery(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps()))

	vec, filters, vals, err := s.extractQuery([]string{"embedding", "model"}, []any{[]float64{1, 2}, "A100"})
	require.NoError(t, err)
	assert.Equal(t, []float64{1, 2}, vec)
	assert.Equal(t, []string{"model = $2"}, filters)
	assert.Equal(t, []any{"A100"}, vals)

	// string vector payload
	vec, _, _, err = s.extractQuery([]string{"embedding"}, []any{"0.1,0.2"})
	require.NoError(t, err)
	assert.Equal(t, []float64{0.1, 0.2}, vec)

	// missing vector
	_, _, _, err = s.extractQuery([]string{"model"}, []any{"A100"})
	assert.Error(t, err)
}

func TestLookupWithMockDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps()))
	s.db = db

	rows := sqlmock.NewRows([]string{"id", "sop", "distance"}).
		AddRow(1, "check bearing", 0.05).
		AddRow(2, "check motor", 0.4)
	mock.ExpectQuery(`SELECT id, sop, distance FROM fault_history ORDER BY embedding <=> \$1::vector LIMIT 3`).
		WithArgs("[0.1,0.2,0.3]").
		WillReturnRows(rows)

	ctx := mockContext.NewMockContext("test", "tt")
	results, err := s.Lookup(ctx, []string{"id", "sop", "distance"}, []string{"embedding"}, []any{[]float64{0.1, 0.2, 0.3}})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, "check bearing", results[0]["sop"])
	assert.Equal(t, float64(0.05), results[0]["distance"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLookupWithFiltersMockDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps()))
	s.db = db

	mock.ExpectQuery(`SELECT \* FROM fault_history WHERE model = \$2 ORDER BY embedding <=> \$1::vector LIMIT 3`).
		WithArgs("[1,2]", "A100").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(9))

	ctx := mockContext.NewMockContext("test", "tt")
	results, err := s.Lookup(ctx, nil,
		[]string{"embedding", "model"}, []any{[]float64{1, 2}, "A100"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(9), results[0]["id"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLookupDimensionMismatchMockDB(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(map[string]any{
		"url": "postgres://x", "table": "t", "vectorField": "v", "dimension": 4,
	}))
	ctx := mockContext.NewMockContext("test", "tt")
	_, err := s.Lookup(ctx, nil, []string{"v"}, []any{[]float64{1, 2}})
	assert.Error(t, err)
}

func TestLookupQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps()))
	s.db = db

	mock.ExpectQuery(`SELECT \* FROM fault_history ORDER BY embedding <=> \$1::vector LIMIT 3`).
		WithArgs("[1]").
		WillReturnError(fmt.Errorf("connection refused"))

	ctx := mockContext.NewMockContext("test", "tt")
	_, err = s.Lookup(ctx, nil, []string{"embedding"}, []any{[]float64{1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	assert.NoError(t, mock.ExpectationsWereMet())
}
