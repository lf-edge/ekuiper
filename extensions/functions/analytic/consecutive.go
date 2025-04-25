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
)

// The state is the value which changed and the changed timestamp
type consecutiveCount struct {
	key string // key for state of count
}
type consecutiveStart struct {
	key  string // key for state of captured start value
	ckey string // key for state of the last condition
}

func NewConsecutiveCountFunc() api.Function {
	return &consecutiveCount{}
}
func NewConsecutiveStartFunc() api.Function { return &consecutiveStart{} }

func (c *consecutiveCount) Validate(args []any) error {
	// args: conditionExpr
	l := len(args)
	if l != 1 {
		return fmt.Errorf("require 1 arg but got %d", l)
	}
	return nil
}

func (c *consecutiveCount) Exec(ctx api.FunctionContext, args []any) (any, bool) {
	// init key and save, avoid duplicate calculation
	if c.key == "" {
		c.key = args[len(args)-1].(string)
	}
	// return immediately if over when condition is not met
	validData := args[len(args)-2].(bool)
	cc, err := ctx.GetState(c.key)
	if err != nil {
		return fmt.Errorf("consecutive_count func error getting state for %s: %s", c.key, err), false
	}
	count := 0
	if cc != nil {
		count = cc.(int)
	}
	if !validData {
		return count, true
	}
	condition := false
	ok := false
	condition, ok = args[0].(bool)
	if !ok {
		ctx.GetLogger().Errorf("consecutive_count requires a bool condition but got %v", args[0])
	}
	if condition {
		count++
		err = ctx.IncrCounter(c.key, 1)
	} else {
		count = 0
		err = ctx.PutState(c.key, 0)
	}
	if err != nil {
		return fmt.Errorf("consecutive_count func save state failed: %s", err), false
	}
	return count, true
}

func (c *consecutiveCount) IsAggregate() bool {
	return false
}

func (c *consecutiveStart) Validate(args []any) error {
	// args conditionExpr, captureVal
	l := len(args)
	if l != 2 {
		return fmt.Errorf("require 2 args but got %d", l)
	}
	return nil
}

func (c *consecutiveStart) Exec(ctx api.FunctionContext, args []any) (any, bool) {
	// init key and save, avoid duplicate calculation
	if c.key == "" {
		c.key = args[len(args)-1].(string)
	}
	// return immediately if over when condition is not met
	validData := args[len(args)-2].(bool)
	// could be nil
	lcc, err := ctx.GetState(c.ckey)
	if err != nil {
		return fmt.Errorf("consecutive_start func error getting state for last condtion: %s", err), false
	}
	lc := false
	if lcc != nil {
		lc = lcc.(bool)
	}
	lv, err := ctx.GetState(c.key)
	if err != nil {
		return fmt.Errorf("consecutive_start func error getting state for captured value: %s", err), false
	}
	if !validData {
		return lv, true
	}
	condition := false
	ok := false
	condition, ok = args[0].(bool)
	if !ok {
		ctx.GetLogger().Errorf("consecutive_start requires a bool condition but got %v", args[0])
	}
	if condition != lc {
		if condition {
			lv = args[1]
			err = ctx.PutState(c.key, lv)
			if err != nil {
				return fmt.Errorf("consecutive_start func save state failed: %s", err), false
			}
		}
		err = ctx.PutState(c.ckey, condition)
		if err != nil {
			return fmt.Errorf("consecutive_start func save state failed: %s", err), false
		}
	}
	if condition {
		return lv, true
	} else {
		return nil, true
	}
}

func (c *consecutiveStart) IsAggregate() bool {
	return false
}
