// Copyright 2024 EMQ Technologies Co., Ltd.
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

package api

type FunctionContext interface {
	StreamContext
	GetFuncId() int
}

type Function interface {
	// Validate The argument is a list of xsql.Expr
	Validate(args []any) error
	// Exec Execute the function, return the result and if execution is successful.
	// If execution fails, return the error and false.
	Exec(ctx FunctionContext, args []any) (interface{}, bool)
	// IsAggregate If this function is an aggregate function. Each parameter of an aggregate function will be a slice
	IsAggregate() bool
}
