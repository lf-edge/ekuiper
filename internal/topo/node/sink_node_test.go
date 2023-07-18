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

//go:build template || !core

package node

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mocknode"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/internal/xsql"
)

func init() {
	testx.InitEnv()
}

func TestBatchSink(t *testing.T) {
	mc := conf.Clock.(*clock.Mock)
	conf.InitConf()
	transform.RegisterAdditionalFuncs()
	tests := []struct {
		config map[string]interface{}
		data   []map[string]interface{}
		result [][]byte
	}{
		{
			config: map[string]interface{}{
				"batchSize": 2,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}, {"ab": "hello3"}},
			result: [][]byte{[]byte(`[{"ab":"hello1"},{"ab":"hello2"}]`)},
		},
		{
			config: map[string]interface{}{
				"lingerInterval": 1000,
			},
			data:   []map[string]interface{}{{"ab": "hello1"}, {"ab": "hello2"}, {"ab": "hello3"}},
			result: [][]byte{[]byte(`[{"ab":"hello1"},{"ab":"hello2"},{"ab":"hello3"}]`)},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	contextLogger := conf.Log.WithField("rule", "TestBatchSink")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	for i, tt := range tests {
		mc.Set(mc.Now())
		mockSink := mocknode.NewMockSink()
		s := NewSinkNodeWithSink("mockSink", mockSink, tt.config)
		s.Open(ctx, make(chan error))
		s.input <- tt.data
		for i := 0; i < 10; i++ {
			mc.Add(1 * time.Second)
			time.Sleep(1 * time.Second)
			// wait until mockSink get results
			if len(mockSink.GetResults()) > 0 {
				break
			}
		}
		results := mockSink.GetResults()
		if !reflect.DeepEqual(tt.result, results) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, results)
		}
	}
}

func TestSinkTemplate_Apply(t *testing.T) {
	conf.InitConf()
	transform.RegisterAdditionalFuncs()
	tests := []struct {
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
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

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
	tests := []struct {
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
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

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
	etcDir, err := conf.GetDataLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir = filepath.Join(etcDir, "schemas", "protobuf")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	// Copy init.proto
	bytesRead, err := os.ReadFile("../../schema/test/test1.proto")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "test1.proto"), bytesRead, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = schema.InitRegistry()
	if err != nil {
		t.Fatal(err)
	}
	transform.RegisterAdditionalFuncs()
	tests := []struct {
		name   string
		config map[string]interface{}
		data   []map[string]interface{}
		result [][]byte
	}{
		{
			name: "test normal protobuf format",
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
			name: "test dateTemplate + protobuf format",
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
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSink := mocknode.NewMockSink()
			s := NewSinkNodeWithSink("mockSink", mockSink, tt.config)
			s.Open(ctx, make(chan error))
			s.input <- tt.data
			var results [][]byte
			time.Sleep(100 * time.Millisecond)
			results = mockSink.GetResults()
			if !reflect.DeepEqual(tt.result, results) {
				t.Errorf("%d \tresult mismatch:\n\nexp=%x\n\ngot=%x\n\n", i, tt.result, results)
			}
		})
	}
}

func TestConfig(t *testing.T) {
	tests := []struct {
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
				"memoryCacheThreshold": 256,
				"bufferLength":         10,
				"maxDiskCache":         6,
				"resendInterval":       10,
			},
			err: errors.New("invalid cache properties: maxDiskCacheTooSmall:maxDiskCache must be greater than bufferPageSize"),
		}, {
			config: map[string]interface{}{
				"enableCache":          true,
				"memoryCacheThreshold": 7,
				"bufferPageSize":       3,
				"sendSingle":           true,
				"maxDiskCache":         21,
				"resendInterval":       10,
			},
			err: errors.New("invalid cache properties: memoryCacheThresholdNotMultiple:memoryCacheThreshold must be a multiple of bufferPageSize"),
		}, {
			config: map[string]interface{}{
				"enableCache":          true,
				"memoryCacheThreshold": 9,
				"bufferPageSize":       3,
				"sendSingle":           true,
				"maxDiskCache":         22,
				"resendInterval":       10,
			},
			err: errors.New("invalid cache properties: maxDiskCacheNotMultiple:maxDiskCache must be a multiple of bufferPageSize"),
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

func TestSinkNode_reset(t *testing.T) {
	mockSink := mocknode.NewMockSink()
	s := NewSinkNodeWithSink("mockSink", mockSink, nil)
	s.reset()
	if s.statManagers != nil {
		t.Errorf("reset() failed")
	}
}

func Test_getSink(t *testing.T) {
	_, err := getSink("mock", map[string]interface{}{"sendSingle": true, "omitIfEmpty": true})
	if err == nil {
		t.Errorf("getSink() failed")
	}
}

func Test_itemToMap(t *testing.T) {
	type args struct {
		item interface{}
	}
	tests := []struct {
		name string
		args args
		want []map[string]interface{}
	}{
		{
			name: "test1",
			args: args{
				item: errors.New("test"),
			},
			want: []map[string]interface{}{
				{"error": "test"},
			},
		},
		{
			name: "test2",
			args: args{
				item: "test2",
			},
			want: []map[string]interface{}{
				{"error": fmt.Sprintf("result is not a map slice but found %#v", "test2")},
			},
		},
		{
			name: "test3",
			args: args{
				item: xsql.Row(&xsql.Tuple{Emitter: "a", Message: map[string]interface{}{"a": 1, "b": "2"}, Timestamp: conf.GetNowInMilli(), Metadata: nil}),
			},
			want: []map[string]interface{}{
				{"a": 1, "b": "2"},
			},
		},
		{
			name: "test4",
			args: args{
				item: xsql.Collection(&xsql.WindowTuples{Content: []xsql.TupleRow{
					&xsql.Tuple{Emitter: "a", Message: map[string]interface{}{"a": 1, "b": "2"}, Timestamp: conf.GetNowInMilli(), Metadata: nil},
				}}),
			},
			want: []map[string]interface{}{
				{"a": 1, "b": "2"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := itemToMap(tt.args.item); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("itemToMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSinkFields_Apply(t *testing.T) {
	conf.InitConf()
	transform.RegisterAdditionalFuncs()
	tests := []struct {
		dt        string
		format    string
		schemaId  string
		delimiter string
		dataField string
		fields    []string
		data      interface{}
		result    [][]byte
	}{
		{
			format: "json",
			fields: []string{"a", "b"},
			data:   map[string]interface{}{"a": "1", "b": "2", "c": "3"},
			result: [][]byte{[]byte(`{"a":"1","b":"2"}`)},
		},
		{
			format: "json",
			fields: []string{"a", "b"},
			data:   []map[string]interface{}{{"a": "1", "b": "2", "c": "3"}},
			result: [][]byte{[]byte(`[{"a":"1","b":"2"}]`)},
		},
		{
			format:    "delimited",
			delimiter: ",",
			fields:    []string{"a", "b"},
			data:      map[string]interface{}{"a": "1", "b": "2", "c": "3"},
			result:    [][]byte{[]byte(`1,2`)},
		},
		{
			format:    "delimited",
			delimiter: ",",
			fields:    []string{"b", "c", "a"},
			data:      map[string]interface{}{"a": "1", "b": "2", "c": "3"},
			result:    [][]byte{[]byte(`2,3,1`)},
		},
		{
			format:   "json",
			schemaId: "",
			fields:   []string{"ax", "bx"},
			dt:       `{"ax": {{.a}}, "bx": {{.b}}}`,
			data:     map[string]interface{}{"a": "1", "b": "2", "c": "3"},
			result:   [][]byte{[]byte(`{"ax":1,"bx":2}`)},
		},
		{
			format:   "json",
			schemaId: "",
			fields:   []string{"a", "b"},
			dt:       `{"ax": {{.a}}, "bx": {{.b}}}`,
			data:     map[string]interface{}{"a": "1", "b": "2", "c": "3"},
			result:   [][]byte{[]byte(`{"a":null,"b":null}`)},
		},
		{
			format:    "json",
			dataField: "device",
			fields:    []string{"a", "b"},
			data:      map[string]interface{}{"device": map[string]interface{}{"a": "1", "b": "2", "c": "3"}, "a": 11, "b": 22, "c": 33},
			result:    [][]byte{[]byte(`{"a":"1","b":"2"}`)},
		},
		{
			format:    "delimited",
			delimiter: ",",
			fields:    []string{"a", "b"},
			dataField: "device",
			data:      map[string]interface{}{"device": map[string]interface{}{"a": "1", "b": "2", "c": "3"}, "a": 11, "b": 22, "c": 33},
			result:    [][]byte{[]byte(`1,2`)},
		},
		{
			format:    "json",
			schemaId:  "",
			fields:    []string{"a", "b"},
			dt:        `{"device": {"a": {{.a}}}}`,
			dataField: "device",
			data:      map[string]interface{}{"a": "1", "b": "2", "c": "3"},
			result:    [][]byte{[]byte(`{"a":1,"b":null}`)},
		},
	}
	contextLogger := conf.Log.WithField("rule", "TestSinkFields_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	for i, tt := range tests {
		tf, _ := transform.GenTransform(tt.dt, tt.format, tt.schemaId, tt.delimiter, tt.dataField, tt.fields)
		vCtx := context.WithValue(ctx, context.TransKey, tf)
		mockSink := mocknode.NewMockSink()
		_ = mockSink.Collect(vCtx, tt.data)
		time.Sleep(1 * time.Second)
		results := mockSink.GetResults()
		if !reflect.DeepEqual(tt.result, results) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, results)
		}
	}
}

func TestSinkCache(t *testing.T) {
	conf.InitConf()
	transform.RegisterAdditionalFuncs()

	contextLogger := conf.Log.WithField("rule", "TestSinkCache")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	data := [][]map[string]interface{}{
		{{"a": 1}},
		{{"a": 2}},
		{{"a": 3}},
		{{"a": 4}},
		{{"a": 5}},
		{{"a": 6}},
		{{"a": 7}},
		{{"a": 8}},
		{{"a": 9}},
		{{"a": 10}},
	}

	t.Run("test cache", func(t *testing.T) {
		hitch := make(chan int, 10)
		config := map[string]interface{}{
			"enableCache": true,
		}
		result := [][]byte{
			[]byte(`[{"a":1}]`),
			[]byte(`[{"a":2}]`),
			[]byte(`[{"a":3}]`),
			[]byte(`[{"a":4}]`),
			[]byte(`[{"a":5}]`),
			[]byte(`[{"a":6}]`),
			[]byte(`[{"a":7}]`),
			[]byte(`[{"a":8}]`),
			[]byte(`[{"a":9}]`),
			[]byte(`[{"a":10}]`),
		}
		mockSink := mocknode.NewMockResendSink(hitch)
		s := NewSinkNodeWithSink("mockSink", mockSink, config)
		s.Open(ctx, make(chan error))
		for i := 0; i < 200; i++ {
			s.input <- data[i%10]
			select {
			case count := <-hitch:
				if count == len(data)*2 {
					goto end
				}
			case <-time.After(1 * time.Second):
			}
		}
	end:
		results := mockSink.GetResults()
		assert.Equal(t, result, results)
	})

	t.Run("test resend cache", func(t *testing.T) {
		hitch := make(chan int, 10)
		config := map[string]interface{}{
			"enableCache":      true,
			"resendAlterQueue": true,
		}
		result := [][]byte{
			[]byte(`[{"a":2}]`),
			[]byte(`[{"a":4}]`),
			[]byte(`[{"a":6}]`),
			[]byte(`[{"a":8}]`),
			[]byte(`[{"a":10}]`),
		}
		resendResult := [][]byte{
			[]byte(`[{"a":1}]`),
			[]byte(`[{"a":3}]`),
			[]byte(`[{"a":5}]`),
		}
		mockSink := mocknode.NewMockResendSink(hitch)
		s := NewSinkNodeWithSink("mockSink", mockSink, config)
		s.Open(ctx, make(chan error))
		for _, d := range data {
			s.input <- d
			<-hitch
		}
		time.Sleep(1 * time.Second)
		results := mockSink.GetResults()
		assert.Equal(t, results, result)
		resentResults := mockSink.GetResendResults()
		assert.Equal(t, resendResult, resentResults[:3])
	})
}
