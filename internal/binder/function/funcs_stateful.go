// Copyright 2023 EMQ Technologies Co., Ltd.
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

package function

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type compressFunc struct {
	compressType string
	compressor   message.Compressor
}

func (c *compressFunc) Validate(args []interface{}) error {
	var eargs []ast.Expr
	for _, arg := range args {
		if t, ok := arg.(ast.Expr); ok {
			eargs = append(eargs, t)
		} else {
			// should never happen
			return fmt.Errorf("receive invalid arg %v", arg)
		}
	}
	return ValidateTwoStrArg(nil, eargs)
}

func (c *compressFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	if args[0] == nil {
		return nil, true
	}
	arg0, err := cast.ToBytes(args[0], cast.CONVERT_SAMEKIND)
	if err != nil {
		return fmt.Errorf("require string or bytea parameter, but got %v", args[0]), false
	}
	arg1 := cast.ToStringAlways(args[1])
	if c.compressor != nil {
		if c.compressType != arg1 {
			return fmt.Errorf("compress type must be consistent, previous %s, now %s", c.compressType, arg1), false
		}
	} else {
		ctx.GetLogger().Infof("creating compressor %s", arg1)
		c.compressor, err = compressor.GetCompressor(arg1)
		if err != nil {
			return err, false
		}
		c.compressType = arg1
	}
	r, e := c.compressor.Compress(arg0)
	if e != nil {
		return e, false
	}
	return r, true
}

func (c *compressFunc) IsAggregate() bool {
	return false
}

func (c *compressFunc) Close(ctx api.StreamContext) error {
	if c.compressor != nil {
		return c.compressor.Close(ctx)
	}
	return nil
}
