// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package node

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestAlignTable(t *testing.T) {
	tests := []struct {
		name string
		in   []any
		out  []any // two outputs to cover random order
		out2 []any
	}{
		{
			name: "stream send",
			in: []any{
				&xsql.Tuple{
					Emitter: "stream1",
					Message: map[string]any{"id": 1, "a": 1},
				},
				&xsql.Tuple{
					Emitter: "table1",
					Message: map[string]any{"id": 1, "t1": "data"},
				},
				&xsql.Tuple{
					Emitter: "stream1",
					Message: map[string]any{"id": 1, "a": 2},
				},
				"unknown",
			},
			out: []any{
				&xsql.WindowTuples{
					Content: []xsql.Row{
						&xsql.Tuple{
							Emitter: "stream1",
							Message: map[string]any{"id": 1, "a": 1},
						},
					},
				},
				&xsql.WindowTuples{
					Content: []xsql.Row{
						&xsql.Tuple{
							Emitter: "stream1",
							Message: map[string]any{"id": 1, "a": 2},
						},
						&xsql.Tuple{
							Emitter: "table1",
							Message: map[string]any{"id": 1, "t1": "data"},
						},
					},
				},
				errors.New("run JoinAlignNode error: invalid input type but got string(unknown)"),
			},
		},
		{
			name: "retain exceed",
			in: []any{
				&xsql.Tuple{
					Emitter: "table1",
					Message: map[string]any{"id": 1, "t1": "data2"},
				},
				&xsql.Tuple{
					Emitter: "table1",
					Message: map[string]any{"id": 1, "t1": "data3"},
				},
				&xsql.Tuple{
					Emitter: "table1",
					Message: map[string]any{"id": 1, "t1": "data4"},
				},
				&xsql.Tuple{
					Emitter: "stream1",
					Message: map[string]any{"id": 1, "a": 3},
				},
			},
			out: []any{
				&xsql.WindowTuples{
					Content: []xsql.Row{
						&xsql.Tuple{
							Emitter: "stream1",
							Message: map[string]any{"id": 1, "a": 3},
						},
						&xsql.Tuple{
							Emitter: "table1",
							Message: map[string]any{"id": 1, "t1": "data3"},
						},
						&xsql.Tuple{
							Emitter: "table1",
							Message: map[string]any{"id": 1, "t1": "data4"},
						},
					},
				},
			},
		},
		{
			name: "multiple table",
			in: []any{
				&xsql.Tuple{
					Emitter: "table2",
					Message: map[string]any{"id": 1, "t2": "dd1"},
				},
				&xsql.Tuple{
					Emitter: "table1",
					Message: map[string]any{"id": 1, "t1": "data5"},
				},
				&xsql.Tuple{
					Emitter: "table2",
					Message: map[string]any{"id": 1, "t2": "dd2"},
				},
				&xsql.WindowTuples{
					Content: []xsql.Row{
						&xsql.Tuple{
							Emitter: "stream1",
							Message: map[string]any{"id": 1, "a": 4},
						},
					},
				},
			},
			out: []any{
				&xsql.WindowTuples{
					Content: []xsql.Row{
						&xsql.Tuple{
							Emitter: "stream1",
							Message: map[string]any{"id": 1, "a": 4},
						},
						&xsql.Tuple{
							Emitter: "table1",
							Message: map[string]any{"id": 1, "t1": "data5"},
						},
						&xsql.Tuple{
							Emitter: "table2",
							Message: map[string]any{"id": 1, "t2": "dd1"},
						},
						&xsql.Tuple{
							Emitter: "table2",
							Message: map[string]any{"id": 1, "t2": "dd2"},
						},
					},
				},
			},
			out2: []any{
				&xsql.WindowTuples{
					Content: []xsql.Row{
						&xsql.Tuple{
							Emitter: "stream1",
							Message: map[string]any{"id": 1, "a": 4},
						},
						&xsql.Tuple{
							Emitter: "table2",
							Message: map[string]any{"id": 1, "t2": "dd1"},
						},
						&xsql.Tuple{
							Emitter: "table2",
							Message: map[string]any{"id": 1, "t2": "dd2"},
						},
						&xsql.Tuple{
							Emitter: "table1",
							Message: map[string]any{"id": 1, "t1": "data5"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, e := NewJoinAlignNode("align", []string{"table1", "table2"}, []int{2, 9999}, &def.RuleOption{
				SendError: true,
			})
			assert.NoError(t, e)
			out := make(chan any, 100)
			e = n.AddOutput(out, "test")
			assert.NoError(t, e)
			ctx := mockContext.NewMockContext("test", "test")
			errCh := make(chan error)
			n.Exec(ctx, errCh)
			defer n.Close()
			for _, in := range tt.in {
				n.input <- in
			}
			r := make([]any, 0, len(tt.out))
			for i := 0; i < len(tt.out); i++ {
				rr := <-out
				r = append(r, rr)
			}
			if tt.out2 != nil {
				if !reflect.DeepEqual(tt.out, r) {
					assert.Equal(t, tt.out2, r)
				}
			} else {
				assert.Equal(t, tt.out, r)
			}
		})
	}
}
