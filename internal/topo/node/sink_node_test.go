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

package node

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mocknode"
	"reflect"
	"testing"
	"time"
)

func TestSinkTemplate_Apply(t *testing.T) {
	conf.InitConf()
	var tests = []struct {
		config map[string]interface{}
		data   []byte
		result [][]byte
	}{
		{
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"wrapper":"w1","content":{{toJson .}},"ab":"{{.ab}}"}`,
			},
			data:   []byte(`[{"ab":"hello1"},{"ab":"hello2"}]`),
			result: [][]byte{[]byte(`{"wrapper":"w1","content":{"ab":"hello1"},"ab":"hello1"}`), []byte(`{"wrapper":"w1","content":{"ab":"hello2"},"ab":"hello2"}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{"wrapper":"arr","content":{{json .}},"content0":{{json (index . 0)}},ab0":"{{index . 0 "ab"}}"}`,
			},
			data:   []byte(`[{"ab":"hello1"},{"ab":"hello2"}]`),
			result: [][]byte{[]byte(`{"wrapper":"arr","content":[{"ab":"hello1"},{"ab":"hello2"}],"content0":{"ab":"hello1"},ab0":"hello1"}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>`,
			},
			data:   []byte(`[{"ab":"hello1"},{"ab":"hello2"}]`),
			result: [][]byte{[]byte(`<div>results</div><ul><li>hello1</li><li>hello2</li></ul>`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{"content":{{toJson .}}}`,
			},
			data:   []byte(`[{"ab":"hello1"},{"ab":"hello2"}]`),
			result: [][]byte{[]byte(`{"content":[{"ab":"hello1"},{"ab":"hello2"}]}`)},
		}, {
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"newab":"{{.ab}}"}`,
			},
			data:   []byte(`[{"ab":"hello1"},{"ab":"hello2"}]`),
			result: [][]byte{[]byte(`{"newab":"hello1"}`), []byte(`{"newab":"hello2"}`)},
		}, {
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"newab":"{{.ab}}"}`,
			},
			data:   []byte(`[{"ab":"hello1"},{"ab":"hello2"}]`),
			result: [][]byte{[]byte(`{"newab":"hello1"}`), []byte(`{"newab":"hello2"}`)},
		}, {
			config: map[string]interface{}{
				"sendSingle":   true,
				"dataTemplate": `{"__meta":{{toJson .__meta}},"temp":{{.temperature}}}`,
			},
			data:   []byte(`[{"temperature":33,"humidity":70,"__meta": {"messageid":45,"other": "mock"}}]`),
			result: [][]byte{[]byte(`{"__meta":{"messageid":45,"other":"mock"},"temp":33}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `[{"__meta":{{toJson (index . 0 "__meta")}},"temp":{{index . 0 "temperature"}}}]`,
			},
			data:   []byte(`[{"temperature":33,"humidity":70,"__meta": {"messageid":45,"other": "mock"}}]`),
			result: [][]byte{[]byte(`[{"__meta":{"messageid":45,"other":"mock"},"temp":33}]`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `[{{range $index, $ele := .}}{{if $index}},{{end}}{"result":{{add $ele.temperature $ele.humidity}}}{{end}}]`,
			},
			data:   []byte(`[{"temperature":33,"humidity":70},{"temperature":22.0,"humidity":50},{"temperature":11,"humidity":90}]`),
			result: [][]byte{[]byte(`[{"result":103},{"result":72},{"result":101}]`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{{$counter := 0}}{{range $index, $ele := .}}{{if ne 90.0 $ele.humidity}}{{$counter = add $counter 1}}{{end}}{{end}}{"result":{{$counter}}}`,
			},
			data:   []byte(`[{"temperature":33,"humidity":70},{"temperature":22,"humidity":50},{"temperature":11,"humidity":90}]`),
			result: [][]byte{[]byte(`{"result":2}`)},
		}, {
			config: map[string]interface{}{
				"dataTemplate": `{"a":"{{base64 .a}}","b":"{{base64 .b}}","c":"{{b64enc .c}}","d":"{{b64enc .d}}","e":"{{base64 .e}}"}`,
				"sendSingle":   true,
			},
			data:   []byte(`[{"a":1,"b":3.1415,"c":"hello","d":"{\"hello\" : 3}","e":{"humidity":20,"temperature":30}}]`),
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
		s.close(ctx, contextLogger)
		results := mockSink.GetResults()
		if !reflect.DeepEqual(tt.result, results) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, results)
		}
	}
}
