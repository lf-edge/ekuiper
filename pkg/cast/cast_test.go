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

package cast

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMapConvert_Funcs(t *testing.T) {
	source := map[interface{}]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[interface{}]interface{}{"FIELDNAME": "MANDT"},
			map[interface{}]interface{}{"FIELDNAME": "VBELN"},
			map[interface{}]interface{}{"FIELDNAME": "POSNR"},
		},
	}

	exp := map[string]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[string]interface{}{"FIELDNAME": "MANDT"},
			map[string]interface{}{"FIELDNAME": "VBELN"},
			map[string]interface{}{"FIELDNAME": "POSNR"},
		},
	}

	got := ConvertMap(source)
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", exp, got)
	}
}

func TestToTypedSlice(t *testing.T) {
	var tests = []struct {
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
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		result, err := ToTypedSlice(tt.s, func(input interface{}, ssn Strictness) (interface{}, error) {
			if input == nil {
				return nil, nil
			} else {
				return fmt.Sprintf("%v", input), nil
			}
		}, "string", CONVERT_SAMEKIND)

		if !reflect.DeepEqual(tt.e, errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.r, result) {
			t.Errorf("%d\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.r, result)
		}
	}
}

func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
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
