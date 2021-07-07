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

package service

import (
	"github.com/lf-edge/ekuiper/pkg/api"
)

type ExternalFunc struct {
	methodName string
	exe        executor
}

func (f *ExternalFunc) Validate(_ []interface{}) error {
	return nil
}

func (f *ExternalFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	if r, err := f.exe.InvokeFunction(ctx, f.methodName, args); err != nil {
		return err, false
	} else {
		return r, true
	}
}

func (f *ExternalFunc) IsAggregate() bool {
	return false
}
