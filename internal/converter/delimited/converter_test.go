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

package delimited

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"testing"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		m map[string]interface{}
		r []byte
		e string
	}{
		{
			m: map[string]interface{}{
				"id":   12,
				"name": "test",
			},
			r: []byte(`12:test`),
		}, {
			m: map[string]interface{}{
				"id":   7,
				"name": "John Doe",
				"age":  22,
				"hobbies": map[string]interface{}{
					"indoor": []string{
						"Chess",
					},
					"outdoor": []string{
						"Basketball",
					},
				},
			},
			r: []byte(`22:map[indoor:[Chess] outdoor:[Basketball]]:7:John Doe`),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		c, err := NewConverter(":")
		if err != nil {
			t.Fatal(err)
		}
		a, err := c.Encode(tt.m)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.r, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.r, a)
		}
	}
}

func TestDecode(t *testing.T) {
	c, err := NewConverter("\t")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := NewConverter("\t")
	if err != nil {
		t.Fatal(err)
	}
	ch.(*Converter).SetColumns([]string{"@", "id", "ts", "value"})
	tests := []struct {
		m  map[string]interface{}
		nm map[string]interface{}
		r  []byte
		e  string
	}{
		{
			m: map[string]interface{}{
				"col0": "#",
				"col1": "1",
				"col2": "1670170500",
				"col3": "161.927872",
			},
			nm: map[string]interface{}{
				"@":     "#",
				"id":    "1",
				"ts":    "1670170500",
				"value": "161.927872",
			},
			r: []byte(`#	1	1670170500	161.927872`),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Decode(tt.r)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.m, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.m, a)
		}
		b, err := ch.Decode(tt.r)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.nm, b) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.nm, b)
		}
	}
}
