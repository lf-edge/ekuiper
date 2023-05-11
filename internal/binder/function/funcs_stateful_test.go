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

package function

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestCompressExec(t *testing.T) {
	ff, ok := builtinStatfulFuncs["compress"]
	if !ok {
		t.Fatal("builtin not found")
	}
	f := ff()
	contextLogger := conf.Log.WithField("rule", "testCompressExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
			},
			result: fmt.Errorf("unsupported compressor: bar"),
		}, { // 1
			args: []interface{}{
				"hello world",
				"zlib",
			},
			result: []byte{120, 156, 0, 11, 0, 244, 255, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 3, 0, 26, 11, 4, 93},
		}, { // 2
			args: []interface{}{
				`{"name":"John Doe","age":30,"email":"john.doe@example.com"}`,
				"zlib",
			},
			result: []byte{120, 156, 0, 59, 0, 196, 255, 123, 34, 110, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 32, 68, 111, 101, 34, 44, 34, 97, 103, 101, 34, 58, 51, 48, 44, 34, 101, 109, 97, 105, 108, 34, 58, 34, 106, 111, 104, 110, 46, 100, 111, 101, 64, 101, 120, 97, 109, 112, 108, 101, 46, 99, 111, 109, 34, 125, 3, 0, 32, 223, 19, 1},
		}, { // 3
			args: []interface{}{
				`{"name":"John Doe","age":30,"email":"john.doe@example.com","address":{"street":"123 Main St","city":"Anytown","state":"CA","zip":"12345"},"phoneNumbers":[{"type":"home","number":"555-555-1234"},{"type":"work","number":"555-555-5678"}],"isActive":true}`,
				"gzip",
			},
			result: fmt.Errorf("compress type must be consistent, previous zlib, now gzip"),
		}, { // 4
			args: []interface{}{
				`hello world`,
				"zlib",
			},
			result: []byte{120, 156, 0, 11, 0, 244, 255, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 3, 0, 26, 11, 4, 93},
		},
	}
	for i, tt := range tests {
		result, _ := f.Exec(tt.args, fctx)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}

func TestDecompressExec(t *testing.T) {
	ff, ok := builtinStatfulFuncs["decompress"]
	if !ok {
		t.Fatal("builtin not found")
	}
	f := ff()
	contextLogger := conf.Log.WithField("rule", "testDecompressExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", api.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
	tests := []struct {
		args   []interface{}
		result interface{}
	}{
		{ // 0
			args: []interface{}{
				"foo",
				"bar",
			},
			result: fmt.Errorf("unsupported decompressor: bar"),
		}, { // 1
			args: []interface{}{
				[]byte{120, 156, 202, 72, 205, 201, 201, 87, 40, 207, 47, 202, 73, 1, 4, 0, 0, 255, 255, 26, 11, 4, 93},
				"zlib",
			},
			result: []byte("hello world"),
		}, { // 2
			args: []interface{}{
				[]byte{120, 156, 170, 86, 202, 75, 204, 77, 85, 178, 82, 242, 202, 207, 200, 83, 112, 201, 79, 85, 210, 81, 74, 76, 79, 85, 178, 50, 54, 208, 81, 74, 205, 77, 204, 204, 81, 178, 82, 202, 202, 207, 200, 211, 75, 201, 79, 117, 72, 173, 72, 204, 45, 200, 73, 213, 75, 206, 207, 85, 170, 5, 4, 0, 0, 255, 255, 32, 223, 19, 1},
				"zlib",
			},
			result: []byte(`{"name":"John Doe","age":30,"email":"john.doe@example.com"}`),
		}, { // 3
			args: []interface{}{
				`{"name":"John Doe","age":30,"email":"john.doe@example.com","address":{"street":"123 Main St","city":"Anytown","state":"CA","zip":"12345"},"phoneNumbers":[{"type":"home","number":"555-555-1234"},{"type":"work","number":"555-555-5678"}],"isActive":true}`,
				"gzip",
			},
			result: fmt.Errorf("decompress type must be consistent, previous zlib, now gzip"),
		}, { // 4
			args: []interface{}{
				[]byte{120, 156, 202, 72, 205, 201, 201, 87, 40, 207, 47, 202, 73, 1, 4, 0, 0, 255, 255, 26, 11, 4, 93},
				"zlib",
			},
			result: []byte("hello world"),
		},
	}
	for i, tt := range tests {
		result, _ := f.Exec(tt.args, fctx)
		if !reflect.DeepEqual(result, tt.result) {
			t.Errorf("%d result mismatch,\ngot:\t%v \nwant:\t%v", i, result, tt.result)
		}
	}
}
