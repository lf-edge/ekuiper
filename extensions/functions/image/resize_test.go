// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"os"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestResize(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	var tests = []struct {
		image  string
		result string
	}{
		{
			image:  "img.png",
			result: "resized.png",
		},
	}
	for i, tt := range tests {
		payload, err := os.ReadFile(tt.image)
		if err != nil {
			t.Errorf("Failed to read image file %s", tt.image)
			continue
		}
		resized, err := os.ReadFile(tt.result)
		if err != nil {
			t.Errorf("Failed to read result image file %s", tt.result)
			continue
		}
		result, _ := ResizeWithChan.Exec([]interface{}{
			payload, 100, 100,
		}, fctx)
		if !reflect.DeepEqual(result, resized) {
			t.Errorf("%d result mismatch", i)
		}
	}
}
