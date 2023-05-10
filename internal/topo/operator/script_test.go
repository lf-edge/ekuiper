// Copyright 2023 EMQ Technologies Co., Ltd.
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

//go:build script

package operator

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
)

func TestScriptOp(t *testing.T) {
	tests := []struct {
		script string
		isAgg  bool
		data   interface{}
		result interface{}
	}{
		{
			script: `function exec(msg, meta) {msg.value = msg.value + 1; return msg}`,
			data: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"value": int64(6),
				},
			},
			result: &xsql.Tuple{
				Emitter: "tbl",
				Message: xsql.Message{
					"value": int64(7),
				},
			},
		},
		{
			script: `function exec(msgs) {
					for (let i = 0; i < msgs.length; i++) {
  						msgs[i].value = msgs[i].value + 1;
					} 
					return msgs
				}`,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"value": int64(6),
						},
					},
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"value": 8.5,
						},
					},
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"value": 10.2,
						},
					},
				},
			},
			result: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "",
						Message: xsql.Message{
							"value": int64(7),
						},
					},
					&xsql.Tuple{
						Emitter: "",
						Message: xsql.Message{
							"value": 9.5,
						},
					},
					&xsql.Tuple{
						Emitter: "",
						Message: xsql.Message{
							"value": 11.2,
						},
					},
				},
			},
		},
		{
			script: `function exec(msgs) {
					agg = {value:0}
					for (let i = 0; i < msgs.length; i++) {
						agg.value = agg.value + msgs[i].value;
					} 
					return agg
				}`,
			isAgg: true,
			data: &xsql.WindowTuples{
				Content: []xsql.TupleRow{
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"value": int64(6),
						},
					},
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"value": 8.5,
						},
					},
					&xsql.Tuple{
						Emitter: "tbl",
						Message: xsql.Message{
							"value": 10.2,
						},
					},
				},
			},
			result: &xsql.Tuple{
				Message: xsql.Message{
					"value": 24.7,
				},
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestScriptOp_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp, err := NewScriptOp(tt.script, tt.isAgg)
		if err != nil {
			t.Errorf("NewScriptOp error: %v", err)
			continue
		}
		result := pp.Apply(ctx, tt.data, nil, nil)
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.script, tt.result, result)
		}
	}
}
