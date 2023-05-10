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
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/testx"
)

func TestConfig(t *testing.T) {
	var tests = []struct {
		conf     map[string]interface{}
		expected *influxSink2
		error    string
	}{
		{ //0
			conf: map[string]interface{}{
				"addr":        "http://192.168.0.3:8086",
				"token":       "Token_test",
				"measurement": "test",
				"org":         "admin",
				"bucket":      "bucket_one",
				"tagKey":      "tag",
				"tagValue":    "value",
				"fields":      "temperature",
			},
			expected: &influxSink2{
				addr:         "http://192.168.0.3:8086",
				token:        "Token_test",
				measurement:  "test",
				org:          "admin",
				bucket:       "bucket_one",
				tagKey:       "tag",
				tagValue:     "value",
				fields:       "temperature",
				cli:          nil,
				fieldMap:     nil,
				hasTransform: false,
			},
		},
		{ //1
			conf: map[string]interface{}{
				"addr":         "http://192.168.0.3:8086",
				"token":        "Token_test",
				"measurement":  "test",
				"org":          "admin",
				"bucket":       "bucket_one",
				"tagKey":       "tag",
				"tagValue":     "value",
				"fields":       "temperature",
				"dataTemplate": "",
			},
			expected: &influxSink2{
				addr:         "http://192.168.0.3:8086",
				token:        "Token_test",
				measurement:  "test",
				org:          "admin",
				bucket:       "bucket_one",
				tagKey:       "tag",
				tagValue:     "value",
				fields:       "temperature",
				cli:          nil,
				fieldMap:     nil,
				hasTransform: false,
			},
		},
		{ //2
			conf: map[string]interface{}{
				"addr":         "http://192.168.0.3:8086",
				"token":        "Token_test",
				"measurement":  "test",
				"org":          "admin",
				"bucket":       "bucket_one",
				"tagKey":       "tag",
				"tagValue":     "value",
				"fields":       "temperature",
				"dataTemplate": "{{funcname .arg1 .arg2}}",
			},
			expected: &influxSink2{
				addr:         "http://192.168.0.3:8086",
				token:        "Token_test",
				measurement:  "test",
				org:          "admin",
				bucket:       "bucket_one",
				tagKey:       "tag",
				tagValue:     "value",
				fields:       "temperature",
				cli:          nil,
				fieldMap:     nil,
				hasTransform: true,
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, test := range tests {
		ifsink := &influxSink2{}
		err := ifsink.Configure(test.conf)
		if !reflect.DeepEqual(test.error, testx.Errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.error, err)
		} else if test.error == "" && !reflect.DeepEqual(test.expected, ifsink) {
			t.Errorf("%d\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.expected, ifsink)
		}
	}
}
