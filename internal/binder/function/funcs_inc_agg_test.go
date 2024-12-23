// Copyright 2024 EMQ Technologies Co., Ltd.
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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestIncAggFunction(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	registerIncAggFunc()
	testcases := []struct {
		funcName string
		args1    []interface{}
		output1  interface{}
		args2    []interface{}
		output2  interface{}
	}{
		{
			funcName: "inc_count",
			args1:    []interface{}{1},
			output1:  int64(1),
			args2:    []interface{}{1},
			output2:  int64(2),
		},
		{
			funcName: "inc_avg",
			args1:    []interface{}{1},
			output1:  float64(1),
			args2:    []interface{}{3},
			output2:  float64(2),
		},
	}
	for index, tc := range testcases {
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		tempStore, _ := state.CreateStore(tc.funcName, def.AtMostOnce)
		fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), index)
		f, ok := builtins[tc.funcName]
		require.True(t, ok)
		got1, ok := f.exec(fctx, tc.args1)
		require.True(t, ok)
		require.Equal(t, tc.output1, got1)
		got2, ok := f.exec(fctx, tc.args2)
		require.True(t, ok)
		require.Equal(t, tc.output2, got2)
	}
}