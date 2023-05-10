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

package main

import (
	"testing"

	"github.com/lf-edge/ekuiper/sdk/go/mock"
)

func TestValidate(t *testing.T) {
	f := &echo{}
	var args []interface{}
	err := f.Validate(args)
	if err != nil {
		exp := "echo function only supports 1 parameter but got 0"
		if err.Error() != exp {
			t.Errorf("expect error %s but got %s", exp, err.Error())
		}
		return
	}
	t.Errorf("should have validation error")
}

func TestExec(t *testing.T) {
	var tests = []mock.FuncTest{
		{
			Args:   []interface{}{"abc"},
			Result: "abc",
			Ok:     true,
		}, {
			Args:   []interface{}{1234},
			Result: 1234,
			Ok:     true,
		}, {
			Args:   []interface{}{[]string{"abc"}},
			Result: []string{"abc"},
			Ok:     true,
		},
	}
	f := &echo{}
	mock.TestFuncExec(f, tests, t)
}
