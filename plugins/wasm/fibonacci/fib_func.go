package main

import (
	"fmt"
)

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
"github.com/lf-edge/ekuiper/sdk/go/api"
)

type fib struct {
}

func (f *fib) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("echo function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

//func (f *fib) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
//	fmt.Println("[example][fibonacci][fibonacci-Exec] start")
//	result := args[0]
//	// wasm func ...
//
//	return result, true
//}

//func (f *fib) Exec(args []interface{}, ctx FunctionContext) (interface{}, bool) {
//	fmt.Println("[example][fibonacci][fibonacci-Exec] start")
//	result := args[0]
//	// wasm func ...
//
//	return result, true
//}

func (f *fib) IsAggregate() bool {
	return false
}
