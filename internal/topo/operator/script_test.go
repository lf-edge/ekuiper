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

package operator

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"reflect"
	"testing"
)

func TestScriptOp(t *testing.T) {
	var tests = []struct {
		script string
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
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestScriptOp_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	for i, tt := range tests {
		pp, err := NewScriptOp(tt.script)
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
