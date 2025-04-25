// Copyright 2025 EMQ Technologies Co., Ltd.
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

package analytic

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type changeCapture struct {
	paraLen    int
	vkey       string
	key        string
	compVal    any
	ignoreNull bool
}
type changedTo struct {
	paraLen    int
	key        string
	ignoreNull bool
}

var ve = &xsql.ValuerEval{}

func NewChangeCaptureFunc() api.Function {
	return &changeCapture{ignoreNull: true}
}
func NewChangeToFunc() api.Function { return &changedTo{ignoreNull: true} }

func (c *changeCapture) Validate(args []any) error {
	// args capatureExpr, monitorExpr, [monitorTarget], [ignoreNull]
	l := len(args)
	if l != 2 && l != 3 && l != 4 {
		return fmt.Errorf("two, three or four args but got %d", l)
	}
	return nil
}

// Exec save the last value and compare with new value to see if it has changed
func (c *changeCapture) Exec(ctx api.FunctionContext, args []any) (any, bool) {
	// init key and save, avoid duplicate calculation
	if c.key == "" {
		c.key = args[len(args)-1].(string)
		c.vkey = c.key + "_mon"
		ctx.GetLogger().Info("change_capture func set key and mon key, paralen is %d", c.paraLen)
		c.paraLen = len(args) - 2
	}
	// return immediately if over when condition is not met
	validData := args[len(args)-2].(bool)
	lastVal, err := ctx.GetState(c.key)
	if err != nil {
		return fmt.Errorf("change_capture func error getting state for captured val: %s", err), false
	}
	if !validData {
		return lastVal, true
	}
	// get arg values and validate
	if c.paraLen > 2 {
		c.compVal = args[2]
	}
	if c.paraLen > 3 {
		var ok bool
		c.ignoreNull, ok = args[3].(bool)
		if !ok {
			return fmt.Errorf("change_capture func ignoreNull is not a bool but got %v", args[3]), false
		}
	}
	// return immediately if ignoreNull and current value is null
	monitorVal := args[1]
	if c.ignoreNull && monitorVal == nil {
		return lastVal, true
	}
	// get previous value for comparison before saving new value
	lv, err := ctx.GetState(c.vkey)
	if err != nil {
		return fmt.Errorf("change_capture func error getting state for monitor val: %v", err), false
	}
	err = ctx.PutState(c.vkey, monitorVal)
	if err != nil {
		return fmt.Errorf("change_capture func save monitor val state failed: %s", err), false
	}
	// compare
	if !compare(ctx, "change_capture", monitorVal, lv) { // has changed
		if c.compVal == nil || compare(ctx, "change_capture", c.compVal, monitorVal) { // meet the condition, update ts
			err = ctx.PutState(c.key, args[0])
			if err != nil {
				return fmt.Errorf("change_capture func save capture val state failed: %s", err), false
			}
			lastVal = args[0]
		}
	}
	return lastVal, true
}

func (c *changeCapture) IsAggregate() bool {
	return false
}

func (h *changedTo) Validate(args []any) error {
	// args monitorExpr, monitorTarget, [ignoreNull]
	l := len(args)
	if l != 2 && l != 3 {
		return fmt.Errorf("expect two or three args but got %d", l)
	}
	return nil
}

func (h *changedTo) Exec(ctx api.FunctionContext, args []any) (any, bool) {
	// init key and save, avoid duplicate calculation
	if h.key == "" {
		h.key = args[len(args)-1].(string)
		ctx.GetLogger().Info("change_to func set key")
		h.paraLen = len(args) - 2
	}
	// return immediately if over when condition is not met
	validData := args[len(args)-2].(bool)
	if !validData {
		return false, true
	}
	// return immediately if ignoreNull and current value is null
	// get arg values and validate
	if h.paraLen > 3 {
		var ok bool
		h.ignoreNull, ok = args[2].(bool)
		if !ok {
			return fmt.Errorf("change_to func ignoreNull is not a bool but got %v", args[2]), false
		}
	}
	monitorVal := args[0]
	if h.ignoreNull && monitorVal == nil {
		return false, true
	}
	lv, err := ctx.GetState(h.key)
	if err != nil {
		return fmt.Errorf("change_to func error getting state for previous val: %v", err), false
	}
	err = ctx.PutState(h.key, monitorVal)
	if err != nil {
		return fmt.Errorf("change_to func save val state failed: %v", err), false
	}
	// only branch to return true
	if !compare(ctx, "change_to", monitorVal, lv) && compare(ctx, "change_to", monitorVal, args[1]) {
		return true, true
	}
	return false, true
}

func compare(ctx api.StreamContext, funcName string, a, b any) bool {
	cd := ve.SimpleDataEval(a, b, ast.EQ)
	cc, ok := cd.(bool)
	if !ok {
		ctx.GetLogger().Warnf("%s requires a bool condition to compare monitor change but got %v", funcName, cd)
	}
	return cc
}

func (h *changedTo) IsAggregate() bool {
	return false
}
