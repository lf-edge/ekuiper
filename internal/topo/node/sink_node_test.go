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

//go:build template || !core
// +build template !core

package node

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mocknode"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestSinkTemplate_Apply(t *testing.T) {
	conf.InitConf()
	transform.RegisterAdditionalFuncs()
	var tests = []struct {
		config map[string]interface{}
		data   []map[string]interface{}
		result [][]byte
	}{
		{
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"wrapper":"w1","content":{{toJson .}},"ab":"{{.ab}}"}`,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`{"wrapper":"w1","content":{"ab":"hello1"},"ab":"hello1"}`), []byte(`{"wrapper":"w1","content":{"ab":"hello2"},"ab":"hello2"}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{"wrapper":"arr","content":{{json .}},"content0":{{json (index . 0)}},ab0":"{{index . 0 "ab"}}"}`,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`{"wrapper":"arr","content":[{"ab":"hello1"},{"ab":"hello2"}],"content0":{"ab":"hello1"},ab0":"hello1"}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>`,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`<div>results</div><ul><li>hello1</li><li>hello2</li></ul>`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{"content":{{toJson .}}}`,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`{"content":[{"ab":"hello1"},{"ab":"hello2"}]}`)},
		}, {
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"newab":"{{.ab}}"}`,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`{"newab":"hello1"}`), []byte(`{"newab":"hello2"}`)},
		}, {
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"newab":"{{.ab}}"}`,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`{"newab":"hello1"}`), []byte(`{"newab":"hello2"}`)},
		}, {
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"__meta":{{toJson .__meta}},"temp":{{.temperature}}}`,
			},
			data:   []map[string]interface{}{{"temperature": 33, "humidity": 70, "__meta": xsql.Metadata{"messageid": 45, "other": "mock"}}},
			result: [][]byte{[]byte(`{"__meta":{"messageid":45,"other":"mock"},"temp":33}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `[{"__meta":{{toJson (index . 0 "__meta")}},"temp":{{index . 0 "temperature"}}}]`,
			},
			data:   []map[string]interface{}{{"temperature": 33, "humidity": 70, "__meta": xsql.Metadata{"messageid": 45, "other": "mock"}}},
			result: [][]byte{[]byte(`[{"__meta":{"messageid":45,"other":"mock"},"temp":33}]`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `[{{range $index, $ele := .}}{{if $index}},{{end}}{"result":{{add $ele.temperature $ele.humidity}}}{{end}}]`,
			},
			data:   []map[string]interface{}{{"temperature": 33, "humidity": 70}, {"temperature": 22.0, "humidity": 50}, {"temperature": 11, "humidity": 90}},
			result: [][]byte{[]byte(`[{"result":103},{"result":72},{"result":101}]`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{{$counter := 0}}{{range $index, $ele := .}}{{if ne 90 $ele.humidity}}{{$counter = add $counter 1}}{{end}}{{end}}{"result":{{$counter}}}`,
			},
			data:   []map[string]interface{}{{"temperature": 33, "humidity": 70}, {"temperature": 22.0, "humidity": 50}, {"temperature": 11, "humidity": 90}},
			result: [][]byte{[]byte(`{"result":2}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{"a":"{{base64 .a}}","b":"{{base64 .b}}","c":"{{b64enc .c}}","d":"{{b64enc .d}}","e":"{{base64 .e}}"}`,
				"sendSingle":   true,
			},
			data:   []map[string]interface{}{{"a": 1, "b": 3.1415, "c": "hello", "d": "{\"hello\" : 3}", "e": map[string]interface{}{"humidity": 20, "temperature": 30}}},
			result: [][]byte{[]byte(`{"a":"MQ==","b":"My4xNDE1","c":"aGVsbG8=","d":"eyJoZWxsbyIgOiAzfQ==","e":"eyJodW1pZGl0eSI6MjAsInRlbXBlcmF0dXJlIjozMH0="}`)},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestSinkTemplate_Apply")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)

	for i, tt := range tests {
		mockSink := mocknode.NewMockSink()
		s := NewSinkNodeWithSink("mockSink", mockSink, tt.config)
		s.Open(ctx, make(chan error))
		s.input <- tt.data
		time.Sleep(1 * time.Second)
		results := mockSink.GetResults()
		if !reflect.DeepEqual(tt.result, results) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, results)
		}
	}
}

func TestOmitEmpty_Apply(t *testing.T) {
	conf.InitConf()
	var tests = []struct {
		config map[string]interface{}
		data   []map[string]interface{}
		result [][]byte
	}{
		{ // 0
			config: map[string]interface{}{
				"sendSingle":  true,
				"omitIfEmpty": true,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`{"ab":"hello1"}`), []byte(`{"ab":"hello2"}`)},
		}, { // 1
			config: map[string]interface{}{
				"sendSingle":  false,
				"omitIfEmpty": true,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}},
			result: [][]byte{[]byte(`[{"ab":"hello1"},{"ab":"hello2"}]`)},
		}, { // 2
			config: map[string]interface{}{
				"sendSingle":  false,
				"omitIfEmpty": false,
			},
			data:   []map[string]interface{}{},
			result: [][]byte{[]byte(`[]`)},
		}, { // 3
			config: map[string]interface{}{
				"sendSingle":  false,
				"omitIfEmpty": false,
			},
			data:   nil,
			result: [][]byte{[]byte(`null`)},
		}, { // 4
			config: map[string]interface{}{
				"sendSingle":  true,
				"omitIfEmpty": false,
			},
			data:   []map[string]interface{}{},
			result: nil,
		}, { // 5
			config: map[string]interface{}{
				"sendSingle":  false,
				"omitIfEmpty": true,
			},
			data:   []map[string]interface{}{},
			result: nil,
		}, { // 6
			config: map[string]interface{}{
				"sendSingle":  false,
				"omitIfEmpty": true,
			},
			data:   nil,
			result: nil,
		}, { // 7
			config: map[string]interface{}{
				"sendSingle":  true,
				"omitIfEmpty": false,
			},
			data:   []map[string]interface{}{},
			result: nil,
		}, { // 8
			config: map[string]interface{}{
				"sendSingle":  true,
				"omitIfEmpty": true,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {}},
			result: [][]byte{[]byte(`{"ab":"hello1"}`)},
		}, { // 9
			config: map[string]interface{}{
				"sendSingle":  true,
				"omitIfEmpty": false,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {}},
			result: [][]byte{[]byte(`{"ab":"hello1"}`), []byte(`{}`)},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestOmitEmpty_Apply")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)

	for i, tt := range tests {
		mockSink := mocknode.NewMockSink()
		s := NewSinkNodeWithSink("mockSink", mockSink, tt.config)
		s.Open(ctx, make(chan error))
		s.input <- tt.data
		time.Sleep(100 * time.Millisecond)
		results := mockSink.GetResults()
		if !reflect.DeepEqual(tt.result, results) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, results)
		}
	}
}

func TestFormat_Apply(t *testing.T) {
	conf.InitConf()
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir = filepath.Join(etcDir, "schemas", "protobuf")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	//Copy init.proto
	bytesRead, err := ioutil.ReadFile("../../schema/test/test1.proto")
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(filepath.Join(etcDir, "test1.proto"), bytesRead, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	schema.InitRegistry()
	transform.RegisterAdditionalFuncs()
	var tests = []struct {
		config map[string]interface{}
		data   []map[string]interface{}
		result [][]byte
	}{
		{
			config: map[string]interface{}{
				"sendSingle": true,
				"format":     `protobuf`,
				"schemaId":   "test1.Person",
			},
			data: []map[string]interface{}{{
				"name":  "test",
				"id":    1,
				"email": "Dddd",
			}},
			result: [][]byte{{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01, 0x1a, 0x04, 0x44, 0x64, 0x64, 0x64}},
		}, {
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"name":"test","email":"{{.ab}}","id":1}`,
				"format":       `protobuf`,
				"schemaId":     "test1.Person",
			},
			data:   []map[string]interface{}{{"ab": "Dddd"}},
			result: [][]byte{{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01, 0x1a, 0x04, 0x44, 0x64, 0x64, 0x64}},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestSinkFormat_Apply")
	ctx := context.WithValue(context.Background(), internal.LoggerKey, contextLogger)

	for i, tt := range tests {
		mockSink := mocknode.NewMockSink()
		s := NewSinkNodeWithSink("mockSink", mockSink, tt.config)
		s.Open(ctx, make(chan error))
		s.input <- tt.data
		time.Sleep(1 * time.Second)
		results := mockSink.GetResults()
		if !reflect.DeepEqual(tt.result, results) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%x\n\ngot=%x\n\n", i, tt.result, results)
		}
	}
}

func TestConfig(t *testing.T) {
	var tests = []struct {
		config map[string]interface{}
		sconf  *SinkConf
		err    error
	}{
		{
			config: map[string]interface{}{
				"sendSingle": true,
			},
			sconf: &SinkConf{
				Concurrency:  1,
				SendSingle:   true,
				Format:       "json",
				BufferLength: 1024,
				SinkConf: conf.SinkConf{
					MemoryCacheThreshold: 1024,
					MaxDiskCache:         1024000,
					BufferPageSize:       256,
					EnableCache:          false,
					ResendInterval:       0,
					CleanCacheAtStop:     false,
				},
			},
		}, {
			config: map[string]interface{}{
				"enableCache":          true,
				"memoryCacheThreshold": 2,
				"bufferPageSize":       2,
				"sendSingle":           true,
				"maxDiskCache":         6,
				"resendInterval":       10,
			},
			sconf: &SinkConf{
				Concurrency:  1,
				SendSingle:   true,
				Format:       "json",
				BufferLength: 1024,
				SinkConf: conf.SinkConf{
					MemoryCacheThreshold: 2,
					MaxDiskCache:         6,
					BufferPageSize:       2,
					EnableCache:          true,
					ResendInterval:       10,
					CleanCacheAtStop:     false,
				},
			},
		}, {
			config: map[string]interface{}{
				"enableCache":          true,
				"memoryCacheThreshold": 2,
				"bufferPageSize":       2,
				"runAsync":             true,
				"maxDiskCache":         6,
				"resendInterval":       10,
			},
			err: errors.New("cache is not supported for async sink, do not use enableCache and runAsync properties together"),
		}, {
			config: map[string]interface{}{
				"enableCache":          true,
				"memoryCacheThreshold": 256,
				"bufferLength":         10,
				"maxDiskCache":         6,
				"resendInterval":       10,
			},
			err: errors.New("invalid cache properties: \nmaxDiskCacheTooSmall:maxDiskCache must be greater than bufferPageSize"),
		}, {
			config: map[string]interface{}{
				"enableCache":          true,
				"memoryCacheThreshold": 7,
				"bufferPageSize":       3,
				"sendSingle":           true,
				"maxDiskCache":         21,
				"resendInterval":       10,
			},
			err: errors.New("invalid cache properties: \nmemoryCacheThresholdNotMultiple:memoryCacheThreshold must be a multiple of bufferPageSize"),
		}, {
			config: map[string]interface{}{
				"enableCache":          true,
				"memoryCacheThreshold": 9,
				"bufferPageSize":       3,
				"sendSingle":           true,
				"maxDiskCache":         22,
				"resendInterval":       10,
			},
			err: errors.New("invalid cache properties: \nmaxDiskCacheNotMultiple:maxDiskCache must be a multiple of bufferPageSize"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestConfig")
	conf.InitConf()
	for i, tt := range tests {
		mockSink := NewSinkNode(fmt.Sprintf("test_%d", i), "mockSink", tt.config)
		sconf, err := mockSink.parseConf(contextLogger)
		if !reflect.DeepEqual(tt.err, err) {
			t.Errorf("%d \terror mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.err, err)
		} else if !reflect.DeepEqual(tt.sconf, sconf) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.sconf, sconf)
		}
	}
}
