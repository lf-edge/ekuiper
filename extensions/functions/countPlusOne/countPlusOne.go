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
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type countPlusOneFunc struct {
}

func (f *countPlusOneFunc) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("countPlusOne function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *countPlusOneFunc) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	arg, ok := args[0].([]interface{})
	if !ok {
		return fmt.Errorf("arg is not a slice, got %v", args[0]), false
	}
	return len(arg) + 1, true
}

func (f *countPlusOneFunc) IsAggregate() bool {
	return true
}

var CountPlusOne countPlusOneFunc
