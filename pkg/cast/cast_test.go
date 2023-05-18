// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package cast

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToStringAlways(t *testing.T) {
	tests := []struct {
		input any
		want  string
	}{
		{
			"test",
			"test",
		},
		{
			100,
			"100",
		},
		{
			nil,
			"",
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, ToStringAlways(tt.input))
	}
}

func TestToString(t *testing.T) {
	tests := []struct {
		input any
		sn    Strictness
		want  string
	}{
		{
			"test",
			CONVERT_SAMEKIND,
			"test",
		},
		{
			[]byte("test"),
			CONVERT_SAMEKIND,
			"test",
		},
		{
			true,
			CONVERT_ALL,
			"true",
		},
		{
			nil,
			CONVERT_ALL,
			"",
		},
		{
			100,
			CONVERT_ALL,
			"100",
		},
		{
			int8(100),
			CONVERT_ALL,
			"100",
		},
		{
			int16(100),
			CONVERT_ALL,
			"100",
		},
		{
			int32(100),
			CONVERT_ALL,
			"100",
		},
		{
			int64(100),
			CONVERT_ALL,
			"100",
		},
		{
			uint(100),
			CONVERT_ALL,
			"100",
		},
		{
			uint8(100),
			CONVERT_ALL,
			"100",
		},
		{
			uint16(100),
			CONVERT_ALL,
			"100",
		},
		{
			uint32(100),
			CONVERT_ALL,
			"100",
		},
		{
			uint64(100),
			CONVERT_ALL,
			"100",
		},
		{
			float32(100.001),
			CONVERT_ALL,
			"100.001",
		},
		{
			100.001,
			CONVERT_ALL,
			"100.001",
		},
		{
			// Stringer test case
			net.IPv4(0, 0, 0, 0),
			CONVERT_ALL,
			"0.0.0.0",
		},
		{
			errors.New("test"),
			CONVERT_ALL,
			"test",
		},
	}
	for _, tt := range tests {
		got, err := ToString(tt.input, tt.sn)
		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}

	_, err := ToString(struct{}{}, STRICT)
	assert.Error(t, err)
}

func TestToIntResult(t *testing.T) {
	tests := []struct {
		input any
		want  int64
	}{
		{
			100,
			100,
		},
		{
			int8(100),
			100,
		},
		{
			int16(100),
			100,
		},
		{
			int32(100),
			100,
		},
		{
			int64(100),
			100,
		},
		{
			uint(100),
			100,
		},
		{
			uint8(100),
			100,
		},
		{
			uint16(100),
			100,
		},
		{
			uint32(100),
			100,
		},
		{
			uint64(100),
			100,
		},
		{
			float32(100),
			100,
		},
		{
			float64(100),
			100,
		},
		{
			"100",
			100,
		},
		{
			false,
			0,
		},
		{
			nil,
			0,
		},
	}
	for _, tt := range tests {
		var (
			got any
			err error
		)
		got, err = ToInt(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, int(tt.want), got)

		got, err = ToInt8(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, int8(tt.want), got)

		got, err = ToInt16(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, int16(tt.want), got)

		got, err = ToInt32(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, int32(tt.want), got)

		got, err = ToInt64(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}

	errTests := []any{
		true,
		nil,
		"1",
	}
	for _, input := range errTests {
		_, err := ToInt(input, STRICT)
		assert.Error(t, err)

		_, err = ToInt8(input, STRICT)
		assert.Error(t, err)

		_, err = ToInt16(input, STRICT)
		assert.Error(t, err)

		_, err = ToInt32(input, STRICT)
		assert.Error(t, err)

		_, err = ToInt64(input, STRICT)
		assert.Error(t, err)
	}
}

func TestToUintResult(t *testing.T) {
	tests := []struct {
		input any
		want  uint64
	}{
		{
			100,
			100,
		},
		{
			int8(100),
			100,
		},
		{
			int16(100),
			100,
		},
		{
			int32(100),
			100,
		},
		{
			int64(100),
			100,
		},
		{
			uint(100),
			100,
		},
		{
			uint8(100),
			100,
		},
		{
			uint16(100),
			100,
		},
		{
			uint32(100),
			100,
		},
		{
			uint64(100),
			100,
		},
		{
			float32(100),
			100,
		},
		{
			float64(100),
			100,
		},
		{
			"100",
			100,
		},
		{
			false,
			0,
		},
		{
			nil,
			0,
		},
	}
	for _, tt := range tests {
		var (
			got any
			err error
		)
		got, err = ToUint8(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, uint8(tt.want), got)

		got, err = ToUint16(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, uint16(tt.want), got)

		got, err = ToUint32(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, uint32(tt.want), got)

		got, err = ToUint64(tt.input, CONVERT_ALL)
		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}

	errTests := []any{
		-1,
		int8(-1),
		int16(-1),
		int32(-1),
		int64(-1),
		float32(-1),
		float64(-1),
		true,
		nil,
		"1",
	}
	for _, input := range errTests {
		_, err := ToUint8(input, STRICT)
		assert.Error(t, err)

		_, err = ToUint16(input, STRICT)
		assert.Error(t, err)

		_, err = ToUint32(input, STRICT)
		assert.Error(t, err)

		_, err = ToUint64(input, STRICT)
		assert.Error(t, err)
	}
}

func TestMapConvert(t *testing.T) {
	source := map[interface{}]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[interface{}]interface{}{"FIELDNAME": "MANDT"},
			map[interface{}]interface{}{"FIELDNAME": "VBELN"},
			map[interface{}]interface{}{"FIELDNAME": "POSNR"},
		},
	}

	assert.Equal(t, map[string]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[string]interface{}{"FIELDNAME": "MANDT"},
			map[string]interface{}{"FIELDNAME": "VBELN"},
			map[string]interface{}{"FIELDNAME": "POSNR"},
		},
	}, ConvertMap(source))
}

func TestToTypedSlice(t *testing.T) {
	tests := []struct {
		s interface{}
		r interface{}
		e string
	}{
		{
			s: []interface{}{"abc", 123},
			r: []string{"abc", "123"},
		},
		{
			s: []interface{}{"addd", "bbb"},
			r: []string{"addd", "bbb"},
		},
		{
			s: []interface{}{nil, "bbb", "ddd"},
			e: "cannot convert []interface {}([<nil> bbb ddd]) to string slice for the 0 element: <nil>",
		},
	}
	t.Logf("The test bucket size is %d.", len(tests))
	for i, tt := range tests {
		result, err := ToTypedSlice(tt.s, func(input interface{}, ssn Strictness) (interface{}, error) {
			if input == nil {
				return nil, nil
			} else {
				return fmt.Sprintf("%v", input), nil
			}
		}, "string", CONVERT_SAMEKIND)

		errString := func(err error) string {
			if err != nil {
				return err.Error()
			}
			return ""
		}

		if !reflect.DeepEqual(tt.e, errString(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.r, result) {
			t.Errorf("%d\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.r, result)
		}
	}
}

func TestMapToStructStrict(t *testing.T) {
	type args struct {
		input  interface{}
		output interface{}
		expect interface{}
	}

	type Result struct {
		Foo string `json:"foo"`
		Bar string `json:"bar"`
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal parse",
			args: args{
				input: map[string]interface{}{
					"foo": "foo",
					"bar": "bar",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
					Bar: "bar",
				},
			},
			wantErr: false,
		},
		{
			name: "input have more than keys",
			args: args{
				input: map[string]interface{}{
					"foo":    "foo",
					"bar":    "bar",
					"foobar": "foobar",
				},
				output: &Result{},
			},
			wantErr: true,
		},
		{
			name: "input have less keys",
			args: args{
				input: map[string]interface{}{
					"foo": "foo",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
				},
			},
			wantErr: false,
		},
		{
			name: "input have unused keys",
			args: args{
				input: map[string]interface{}{
					"foo":    "foo",
					"foobar": "foobar",
				},
				output: &Result{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := MapToStructStrict(tt.args.input, tt.args.output)
			if (err != nil) != tt.wantErr {
				t.Errorf("MapToStructure() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr == false && !reflect.DeepEqual(tt.args.output, tt.args.expect) {
				t.Errorf(" got = %v, want %v", tt.args.output, tt.args.expect)
			}
		})
	}
}

func TestMapToStruct(t *testing.T) {
	type args struct {
		input  interface{}
		output interface{}
		expect interface{}
	}

	type Result struct {
		Foo string `json:"foo"`
		Bar string `json:"bar"`
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal parse",
			args: args{
				input: map[string]interface{}{
					"foo": "foo",
					"bar": "bar",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
					Bar: "bar",
				},
			},
			wantErr: false,
		},
		{
			name: "input have more than keys",
			args: args{
				input: map[string]interface{}{
					"foo":    "foo",
					"bar":    "bar",
					"foobar": "foobar",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
					Bar: "bar",
				},
			},
			wantErr: false,
		},
		{
			name: "input have less keys",
			args: args{
				input: map[string]interface{}{
					"foo": "foo",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
				},
			},
			wantErr: false,
		},
		{
			name: "input have unused keys",
			args: args{
				input: map[string]interface{}{
					"foo":    "foo",
					"foobar": "foobar",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MapToStruct(tt.args.input, tt.args.output); (err != nil) != tt.wantErr {
				t.Errorf("MapToStructure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMapToStructNotCaseSensitive(t *testing.T) {
	type args struct {
		input  interface{}
		output interface{}
		expect interface{}
	}

	type Result struct {
		Foo string `json:"foo"`
		Bar string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal parse",
			args: args{
				input: map[string]interface{}{
					"foo": "foo",
					"bar": "bar",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
					Bar: "bar",
				},
			},
			wantErr: false,
		},
		{
			name: "not case sensitive",
			args: args{
				input: map[string]interface{}{
					"FOO": "foo",
					"BAR": "bar",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
					Bar: "bar",
				},
			},
			wantErr: false,
		},
		{
			name: "keys must match",
			args: args{
				input: map[string]interface{}{
					"foo":  "foo",
					"BARS": "bars",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MapToStruct(tt.args.input, tt.args.output); (err != nil) != tt.wantErr {
				t.Errorf("MapToStructure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMapToStructTag(t *testing.T) {
	type args struct {
		input  interface{}
		output interface{}
		expect interface{}
	}

	type Result struct {
		Foo string `json:"fo"`
		Bar string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal parse",
			args: args{
				input: map[string]interface{}{
					"fo":  "foo",
					"bar": "bar",
				},
				output: &Result{},
				expect: &Result{
					Foo: "foo",
					Bar: "bar",
				},
			},
			wantErr: false,
		},
		{
			name: "key tag not match",
			args: args{
				input: map[string]interface{}{
					"FOO": "foo",
					"BAR": "bar",
				},
				output: &Result{},
				expect: &Result{
					Bar: "bar",
				},
			},
			wantErr: false,
		},
		{
			name: "key tag not match",
			args: args{
				input: map[string]interface{}{
					"foo":  "foo",
					"BARS": "bars",
				},
				output: &Result{},
				expect: &Result{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MapToStruct(tt.args.input, tt.args.output); (err != nil) != tt.wantErr {
				t.Errorf("MapToStructure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestToByteA(t *testing.T) {
	bytea, _ := hex.DecodeString("736f6d6520646174612077697468200020616e6420efbbbf")
	tests := []struct {
		input  interface{}
		output []byte
		err    string
	}{
		{
			input: "foo",
			err:   "illegal string foo, must be base64 encoded string",
		}, {
			input:  []byte("foo"),
			output: []byte("foo"),
			err:    "",
		}, {
			input:  1,
			output: nil,
			err:    "cannot convert int(1) to bytes",
		}, {
			input:  "c29tZSBkYXRhIHdpdGggACBhbmQg77u/",
			output: bytea,
		},
	}
	for i, tt := range tests {
		r, err := ToByteA(tt.input, CONVERT_SAMEKIND)
		if err != nil {
			if err.Error() != tt.err {
				t.Errorf("%d, ToByteA() error = %v, wantErr %v", i, err, tt.err)
				continue
			}
		} else {
			if !reflect.DeepEqual(r, tt.output) {
				t.Errorf("%d: ToByteA() = %x, want %x", i, r, tt.output)
			}
		}
	}
}
