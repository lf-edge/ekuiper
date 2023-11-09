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

package protobuf

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/internal/testx"
)

func TestEncode(t *testing.T) {
	c, err := NewConverter("../../schema/test/test1.proto", "", "Person")
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
				"id":   1,
				"age":  1,
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01},
		}, {
			m: map[string]interface{}{
				"name":  "test",
				"id":    1,
				"email": "Dddd",
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01, 0x1a, 0x04, 0x44, 0x64, 0x64, 0x64},
		}, {
			m: map[string]interface{}{
				"name": "test",
				"id":   1,
				"code": []any{
					map[string]any{"doubles": []any{1.1, 2.2, 3.3}},
					map[string]any{"doubles": []any{3.3, 1.1}},
				},
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01, 0x22, 0x1b, 0x09, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, 0x3f, 0x09, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0x01, 0x40, 0x09, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x0a, 0x40, 0x22, 0x12, 0x09, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x0a, 0x40, 0x09, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, 0x3f},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Encode(tt.m)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.r, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%x\n\ngot=%x\n\n", i, tt.r, a)
		}
	}
}

func TestEmbedType(t *testing.T) {
	c, err := NewConverter("../../schema/test/test3.proto", "", "DrivingData")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		r []byte
		e string
		d map[string]interface{}
	}{
		{
			m: map[string]interface{}{
				"drvg_mod":         1,
				"drvg_mod_history": []any{1, 2, 3},
				"brk_pedal_sts": map[string]interface{}{
					"valid": int64(0),
				},
				"average_speed": 90.56,
			},
			r: []byte{0x08, 0x01, 0x11, 0xa4, 0x70, 0x3d, 0x0a, 0xd7, 0xa3, 0x56, 0x40, 0x1a, 0x02, 0x08, 0x00, 0x20, 0x01, 0x20, 0x02, 0x20, 0x03},
			d: map[string]interface{}{
				"drvg_mod":         int64(1),
				"drvg_mod_history": []int64{1, 2, 3},
				"brk_pedal_sts": map[string]interface{}{
					"valid": int64(0),
				},
				"average_speed": 90.56,
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for _, tt := range tests {
		a, err := c.Encode(tt.m)
		assert.NoError(t, err)
		assert.Equal(t, tt.r, a)
		m, err := c.Decode(a)
		assert.NoError(t, err)
		assert.Equal(t, tt.d, m)
	}
}

func TestDecode(t *testing.T) {
	c, err := NewConverter("../../schema/test/test1.proto", "", "Person")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		r []byte
	}{
		{
			m: map[string]interface{}{
				"name":  "test",
				"id":    int64(1),
				"email": "Dddd",
				"code":  []interface{}{},
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01, 0x1a, 0x04, 0x44, 0x64, 0x64, 0x64},
		},
		{
			m: map[string]interface{}{
				"name":  "test",
				"id":    int64(1),
				"email": "",
				"code": []map[string]any{
					{"doubles": []float64{1.1, 2.2, 3.3}},
					{"doubles": []float64{3.3, 1.1}},
				},
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01, 0x22, 0x1b, 0x09, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, 0x3f, 0x09, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0x01, 0x40, 0x09, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x0a, 0x40, 0x22, 0x12, 0x09, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x0a, 0x40, 0x09, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, 0x3f},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			a, err := c.Decode(tt.r)
			assert.NoError(t, err)
			assert.Equal(t, tt.m, a)
		})
	}
}

func TestStatic(t *testing.T) {
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
	// Copy the helloworld.so
	bytesRead, err := os.ReadFile(filepath.Join(dataDir, "helloworld.so"))
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(etcDir, "helloworld.so"), bytesRead, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	schema.InitRegistry()
	c, err := NewConverter("../../schema/test/test1.proto", "../../../data/test/schemas/custom/helloworld.so", "HelloReply")
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
				"message": "test",
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74},
		}, {
			m: map[string]interface{}{
				"message": "another test 2",
			},
			r: []byte{0x0a, 0x0e, 0x61, 0x6e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x20, 0x74, 0x65, 0x73, 0x74, 0x20, 0x32},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Encode(tt.m)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.r, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%x\n\ngot=%x\n\n", i, tt.r, a)
		}
		m, err := c.Decode(tt.r)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.m, m) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.m, m)
		}
	}
}

func TestDecodeProto3(t *testing.T) {
	c, err := NewConverter("../../schema/test/test4.proto", "", "Classroom")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		r []byte
	}{
		{
			m: map[string]interface{}{
				"name":   "test",
				"number": int64(1),
				"stu":    []interface{}{},
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01},
		},
		{
			m: map[string]interface{}{
				"name":   "test",
				"number": int64(1),
				"stu": []map[string]interface{}{
					{
						"age":  int64(12),
						"name": "test",
						"info": nil,
					},
				},
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01, 0x1a, 0x08, 0x08, 0x0c, 0x12, 0x04, 0x74, 0x65, 0x73, 0x74},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			a, err := c.Decode(tt.r)
			assert.NoError(t, err)
			assert.Equal(t, tt.m, a)
		})
	}
}

func TestEncodeDecodeForAllTypes(t *testing.T) {
	c, err := NewConverter("../../schema/test/alltypes.proto", "", "AllTypesTest")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		m    map[string]interface{}
		b    []byte
		r    map[string]interface{}
	}{
		{
			name: "all valid",
			m: map[string]interface{}{
				"adouble":     20.44,
				"afloat":      20.44,
				"anint32":     -67,
				"anint64":     -67,
				"auint32":     67,
				"auint64":     67,
				"abool":       true,
				"abytes":      []byte{0x01, 0x02, 0x03},
				"double_list": []float64{1.2, 2.3, 3.4},
				"float_list":  []float64{1.2, 2.3, 3.4},
				"int32_list":  []int64{1, 2, 3},
				"int64_list":  []int64{1, 2, 3},
				"uint32_list": []int64{1, 2, 3},
				"uint64_list": []int64{1, 2, 3},
				"bool_list":   []bool{true, false, true},
				"bytes_list":  [][]byte{{0x01, 0x02, 0x03}, {0x04, 0x05, 0x06}},
			},
			b: []byte{0x9, 0x71, 0x3d, 0xa, 0xd7, 0xa3, 0x70, 0x34, 0x40, 0x15, 0x1f, 0x85, 0xa3, 0x41, 0x18, 0xbd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1, 0x20, 0xbd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1, 0x28, 0x43, 0x30, 0x43, 0x38, 0x1, 0x42, 0x3, 0x1, 0x2, 0x3, 0x4a, 0x18, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0xf3, 0x3f, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x2, 0x40, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0xb, 0x40, 0x52, 0xc, 0x9a, 0x99, 0x99, 0x3f, 0x33, 0x33, 0x13, 0x40, 0x9a, 0x99, 0x59, 0x40, 0x5a, 0x3, 0x2, 0x4, 0x6, 0x62, 0x18, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6a, 0xc, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x72, 0x18, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7a, 0x3, 0x1, 0x0, 0x1, 0x82, 0x1, 0x3, 0x1, 0x2, 0x3, 0x82, 0x1, 0x3, 0x4, 0x5, 0x6},
			r: map[string]interface{}{
				"adouble":     20.44,
				"afloat":      20.440000534057617,
				"anint32":     int64(-67),
				"anint64":     int64(-67),
				"auint32":     int64(67),
				"auint64":     int64(67),
				"abool":       true,
				"abytes":      []byte{0x01, 0x02, 0x03},
				"double_list": []float64{1.2, 2.3, 3.4},
				"float_list":  []float64{1.2000000476837158, 2.299999952316284, 3.4000000953674316},
				"int32_list":  []int64{1, 2, 3},
				"int64_list":  []int64{1, 2, 3},
				"uint32_list": []int64{1, 2, 3},
				"uint64_list": []int64{1, 2, 3},
				"bool_list":   []bool{true, false, true},
				"bytes_list":  [][]byte{{0x01, 0x02, 0x03}, {0x04, 0x05, 0x06}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := c.Encode(tt.m)
			assert.NoError(t, err)
			assert.Equal(t, tt.b, a)
			m, err := c.Decode(a)
			assert.NoError(t, err)
			assert.Equal(t, tt.r, m)
		})
	}
}
