// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		name string
		m    any
		r    []byte
		e    string
	}{
		{
			name: "normal",
			m: map[string]interface{}{
				"id":   1670170500.0,
				"name": "test",
			},
			r: []byte(`1670170500:test`),
		},
		{
			name: "embedded",
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
		{
			name: "list",
			m: []map[string]interface{}{
				{
					"id":   12,
					"name": "test",
				},
				{
					"id":   14,
					"name": "test2",
				},
			},
			r: []byte("12:test\n14:test2"),
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConverter(map[string]any{"delimiter": ":"})
			assert.NoError(t, err)
			a, err := c.Encode(ctx, tt.m)
			if tt.e != "" {
				assert.EqualError(t, err, tt.e)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.r, a)
			}
		})
	}
}

func TestEncodeWithHeader(t *testing.T) {
	tests := []struct {
		name string
		m    any
		r    []byte
		e    string
	}{
		{
			name: "normal",
			m: map[string]interface{}{
				"id":   12,
				"name": "test",
			},
			r: []byte{0x3a, 0x0, 0x0, 0x0, 0x7, 0x69, 0x64, 0x3a, 0x6e, 0x61, 0x6d, 0x65, 0x31, 0x32, 0x3a, 0x74, 0x65, 0x73, 0x74},
		},
		{
			name: "embedded",
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
			r: []byte{0x3a, 0x0, 0x0, 0x0, 0x13, 0x61, 0x67, 0x65, 0x3a, 0x68, 0x6f, 0x62, 0x62, 0x69, 0x65, 0x73, 0x3a, 0x69, 0x64, 0x3a, 0x6e, 0x61, 0x6d, 0x65, 0x32, 0x32, 0x3a, 0x6d, 0x61, 0x70, 0x5b, 0x69, 0x6e, 0x64, 0x6f, 0x6f, 0x72, 0x3a, 0x5b, 0x43, 0x68, 0x65, 0x73, 0x73, 0x5d, 0x20, 0x6f, 0x75, 0x74, 0x64, 0x6f, 0x6f, 0x72, 0x3a, 0x5b, 0x42, 0x61, 0x73, 0x6b, 0x65, 0x74, 0x62, 0x61, 0x6c, 0x6c, 0x5d, 0x5d, 0x3a, 0x37, 0x3a, 0x4a, 0x6f, 0x68, 0x6e, 0x20, 0x44, 0x6f, 0x65},
		},
		{
			name: "list",
			m: []map[string]interface{}{
				{
					"id":   12,
					"name": "test",
				},
				{
					"id":   1670170500.0,
					"name": "test2",
				},
			},
			r: []byte("id:name\n12:test\n1670170500:test2"),
		},
	}
	ctx := mockContext.NewMockContext("test", "op1")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConverter(map[string]any{"delimiter": ":", "hasHeader": true})
			assert.NoError(t, err)
			a, err := c.Encode(ctx, tt.m)
			if tt.e != "" {
				assert.EqualError(t, err, tt.e)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.r, a)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	c, err := NewConverter(map[string]any{"delimiter": "\t"})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := NewConverter(map[string]any{"delimiter": "\t", "fields": []string{"@", "id", "ts", "value"}})
	if err != nil {
		t.Fatal(err)
	}
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
	ctx := mockContext.NewMockContext("test", "op1")
	for i, tt := range tests {
		a, err := c.Decode(ctx, tt.r)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.m, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.m, a)
		}
		b, err := ch.Decode(ctx, tt.r)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.nm, b) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.nm, b)
		}
	}
}

func TestError(t *testing.T) {
	converter, err := NewConverter(map[string]any{"delimiter": ","})
	require.NoError(t, err)
	ctx := mockContext.NewMockContext("test", "op1")
	_, err = converter.Encode(ctx, nil)
	require.Error(t, err)
	errWithCode, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.CovnerterErr, errWithCode.Code())
}
