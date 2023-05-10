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

package custom

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gdexlab/go-render/render"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/internal/testx"
)

func init() {
	testx.InitEnv()
}

func TestCustomConverter(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		t.Fatal(err)
	}
	etcDir := filepath.Join(dataDir, "schemas", "custom")
	err = os.MkdirAll(etcDir, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.RemoveAll(etcDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// build the so file into data/test prior to running the test
	//Copy the helloworld.so
	bytesRead, err := os.ReadFile(filepath.Join(dataDir, "myFormat.so"))
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "myFormat.so"), bytesRead, 0755)
	if err != nil {
		t.Fatal(err)
	}
	schema.InitRegistry()
	testEncode(t)
	testDecode(t)
}

func testEncode(t *testing.T) {
	c, err := LoadConverter("myFormat", "Sample", "")
	if err != nil {
		t.Fatal(err)
	}
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
			r: []byte(`{"id":12,"name":"test","age":0,"hobbies":{"indoor":null,"outdoor":null}}`),
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
			r: []byte(`{"id":7,"name":"John Doe","age":22,"hobbies":{"indoor":["Chess"],"outdoor":["Basketball"]}}`),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Encode(tt.m)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.r, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.r, a)
		}
	}
}

func testDecode(t *testing.T) {
	c, err := LoadConverter("myFormat", "Sample", "")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		r []byte
		e string
	}{
		{
			m: map[string]interface{}{
				"name": "test",
				"age":  int64(0),
				"id":   int64(0),
				"hobbies": map[string]interface{}{
					"indoor":  []string(nil),
					"outdoor": []string(nil),
				},
			},
			r: []byte(`{"name":"test"}`),
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Decode(tt.r)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.m, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, render.AsCode(tt.m), render.AsCode(a))
		}
	}
}
