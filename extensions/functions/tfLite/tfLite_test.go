// Copyright 2022 EMQ Technologies Co., Ltd.
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

package main

import (
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestTffunc_Exec(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	type args struct {
		args []interface{}
		ctx  api.FunctionContext
	}
	tests := []struct {
		name  string
		args  args
		want  interface{}
		want1 bool
	}{
		{
			name: "fizzbuzz",
			args: args{
				args: []interface{}{
					"fizzbuzz_model",
					[]interface{}{1, 2, 3, 4, 5, 6, 7},
				},
				ctx: fctx,
			},
			want:  []interface{}{[]float32{0.9971661, 4.145413e-05, 0.0027840463, 8.373417e-06}},
			want1: true,
		},
		{
			name: "sin",
			args: args{
				args: []interface{}{
					"sin_model",
					[]interface{}{1},
				},
				ctx: fctx,
			},
			want:  []interface{}{[]float32{0.86996967}},
			want1: true,
		},
		{
			name: "xor",
			args: args{
				args: []interface{}{
					"xor_model",
					[]interface{}{1, 0},
				},
				ctx: fctx,
			},
			want:  []interface{}{[]float32{0.9586827}},
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Tffunc{}
			got, got1 := f.Exec(tt.args.args, tt.args.ctx)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Exec() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Exec() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
