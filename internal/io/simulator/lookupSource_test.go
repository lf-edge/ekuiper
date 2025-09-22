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

package simulator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSimulatorLookupSource_Lookup(t *testing.T) {
	tests := []struct {
		name         string
		data         []map[string]any
		lookupFields []string
		cmpKeys      []string
		cmpValues    []any
		expected     []map[string]any
	}{
		{
			name: "single match",
			data: []map[string]any{
				{"id": 1, "name": "Alice", "age": 25},
				{"id": 2, "name": "Bob", "age": 30},
				{"id": 3, "name": "Charlie", "age": 35},
			},
			lookupFields: []string{"id", "name"},
			cmpKeys:      []string{"id"},
			cmpValues:    []any{2},
			expected: []map[string]any{
				{"id": 2, "name": "Bob"},
			},
		},
		{
			name: "multiple matches",
			data: []map[string]any{
				{"id": 1, "name": "Alice", "age": 25},
				{"id": 2, "name": "Bob", "age": 30},
				{"id": 1, "name": "Alice", "age": 28},
				{"id": 3, "name": "Charlie", "age": 35},
			},
			lookupFields: []string{"id", "name"},
			cmpKeys:      []string{"id"},
			cmpValues:    []any{1},
			expected: []map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 1, "name": "Alice"},
			},
		},
		{
			name: "no match",
			data: []map[string]any{
				{"id": 1, "name": "Alice", "age": 25},
				{"id": 2, "name": "Bob", "age": 30},
				{"id": 3, "name": "Charlie", "age": 35},
			},
			lookupFields: []string{"id", "name"},
			cmpKeys:      []string{"id"},
			cmpValues:    []any{99},
			expected:     []map[string]any{},
		},
		{
			name: "string value match",
			data: []map[string]any{
				{"id": 1, "name": "Alice", "age": 25},
				{"id": 2, "name": "Bob", "age": 30},
				{"id": 3, "name": "Charlie", "age": 35},
			},
			lookupFields: []string{"id", "name"},
			cmpKeys:      []string{"name"},
			cmpValues:    []any{"Bob"},
			expected: []map[string]any{
				{"id": 2, "name": "Bob"},
			},
		},
		{
			name: "multiple lookup fields",
			data: []map[string]any{
				{"id": 1, "name": "Alice", "age": 25, "city": "New York"},
				{"id": 2, "name": "Bob", "age": 30, "city": "London"},
				{"id": 3, "name": "Charlie", "age": 35, "city": "Paris"},
			},
			lookupFields: []string{"id", "name", "age"},
			cmpKeys:      []string{"name"},
			cmpValues:    []any{"Bob"},
			expected: []map[string]any{
				{"id": 2, "name": "Bob", "age": 30},
			},
		},
		{
			name: "empty data",
			data: []map[string]any{},
			lookupFields: []string{"id", "name"},
			cmpKeys:      []string{"id"},
			cmpValues:    []any{1},
			expected:     []map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create SimulatorLookupSource instance
			source := &SimulatorLookupSource{}
			
			// Create mock context
			ctx := mockContext.NewMockContext("test", "Test")
			
			// Provision the source with test data
			configs := map[string]any{
				"data": tt.data,
			}
			err := source.Provision(ctx, configs)
			require.NoError(t, err)
			
			// Connect the source
			err = source.Connect(ctx, func(status string, message string) {
				// do nothing
			})
			require.NoError(t, err)
			
			// Perform lookup
			result, err := source.Lookup(ctx, tt.lookupFields, tt.cmpKeys, tt.cmpValues)
			
			// Verify results
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
			
			// Close the source
			err = source.Close(ctx)
			require.NoError(t, err)
		})
	}
}

func TestSimulatorLookupSource_Provision(t *testing.T) {
	tests := []struct {
		name     string
		configs  map[string]any
		wantErr  bool
		errMsg   string
	}{
		{
			name: "valid config",
			configs: map[string]any{
				"data": []map[string]any{
					{"id": 1, "name": "Alice"},
					{"id": 2, "name": "Bob"},
				},
			},
			wantErr: false,
		},
		{
			name: "empty data config",
			configs: map[string]any{
				"data": []map[string]any{},
			},
			wantErr: false,
		},
		{
			name: "nil data config",
			configs: map[string]any{
				"data": nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &SimulatorLookupSource{}
			ctx := mockContext.NewMockContext("test", "Test")
			
			err := source.Provision(ctx, tt.configs)
			
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, source.cfg)
			}
		})
	}
}