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

package context

import (
	"fmt"
	"github.com/lf-edge/ekuiper/sdk/go/api"
)

type DefaultFuncContext struct {
	api.StreamContext
	funcId int
}

func NewDefaultFuncContext(ctx api.StreamContext, id int) *DefaultFuncContext {
	return &DefaultFuncContext{
		StreamContext: ctx,
		funcId:        id,
	}
}

func (c *DefaultFuncContext) GetFuncId() int {
	return c.funcId
}

func (c *DefaultFuncContext) convertKey(key string) string {
	return fmt.Sprintf("$$func%d_%s", c.funcId, key)
}
