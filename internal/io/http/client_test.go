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

package http

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/io/mock"
)

func TestHeaderConf(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string]interface{}
		props   map[string]interface{}
		err     error
		headers map[string]string
	}{
		{
			name: "template",
			data: map[string]interface{}{
				"id":          1234,
				"temperature": 20,
			},
			props: map[string]interface{}{
				"url":     "http://localhost:9090/",
				"headers": "{\"custom\":\"{{.id}}\",\"sourceCode\":\"1090\",\"serviceCode\":\"1090109107\" }",
			},
			headers: map[string]string{
				"custom":      "1234",
				"sourceCode":  "1090",
				"serviceCode": "1090109107",
			},
		},
		{
			name: "wrong template",
			data: map[string]interface{}{
				"id":          1234,
				"temperature": 20,
			},
			props: map[string]interface{}{
				"url":     "http://localhost:9090/",
				"headers": "{\"custom\":\"{{{.idd}}\",\"sourceCode\":\"1090\",\"serviceCode\":\"1090109107\" }",
			},
			err: fmt.Errorf("fail to parse the header template {\"custom\":\"{{{.idd}}\",\"sourceCode\":\"1090\",\"serviceCode\":\"1090109107\" }: template: sink:1: unexpected \"{\" in command"),
		},
		{
			name: "wrong parsed template",
			data: map[string]interface{}{
				"id":          1234,
				"temperature": 20,
			},
			props: map[string]interface{}{
				"url":     "http://localhost:9090/",
				"headers": "{{.id}}",
			},
			err: fmt.Errorf("parsed header template is not json: 1234"),
		},
		{
			name: "wrong header entry template",
			data: map[string]interface{}{
				"id":          1234,
				"temperature": 20,
			},
			props: map[string]interface{}{
				"url": "http://localhost:9090/",
				"headers": map[string]interface{}{
					"custom": "{{{.id}}",
				},
			},
			err: fmt.Errorf("fail to parse the header entry custom: template: sink:1: unexpected \"{\" in command"),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	ctx := mock.NewMockContext("none", "httppull_init")
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test %d: %s", i, tt.name), func(t *testing.T) {
			r := &ClientConf{}
			err := r.InitConf("", tt.props)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			headers, err := r.parseHeaders(ctx, tt.data)
			if err != nil {
				if tt.err == nil {
					t.Errorf("Expected error: %v", err)
				} else {
					if err.Error() != tt.err.Error() {
						t.Errorf("Error mismatch\nexp\t%v\ngot\t%v", tt.err, err)
					}
				}
				return
			}
			if !reflect.DeepEqual(headers, tt.headers) {
				t.Errorf("Parsed headers mismatch\nexp\t%+v\ngot\t%+v", tt.headers, headers)
			}
		})
	}
}
