// Copyright 2021 EMQ Technologies Co., Ltd.
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

package mock

import (
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/sdk/go/api"
)

type FuncTest struct {
	Args   []interface{}
	Result interface{}
	Ok     bool
}

func TestFuncExec(f api.Function, tests []FuncTest, t *testing.T) {
	ctx := newMockFuncContext(newMockContext("rule1", "op1"), 1)
	for i, tt := range tests {
		r, o := f.Exec(tt.Args, ctx)
		if o != tt.Ok {
			t.Errorf("%d ok mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Ok, o)
		} else if !reflect.DeepEqual(tt.Result, r) {
			t.Errorf("%d result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Result, r)
		}
	}
}
