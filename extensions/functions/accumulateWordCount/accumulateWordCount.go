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
	"strings"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

/**
 **	A function which will count how many words had been received from the beginning
 ** to demonstrate how to use states
 ** There are 2 arguments:
 **  0: column, the column to be calculated. The column value type must be string
 **  1: separator, a string literal for word separator
 **/

type accumulateWordCountFunc struct{}

func (f *accumulateWordCountFunc) Validate(args []interface{}) error {
	if len(args) != 2 {
		return fmt.Errorf("wordCount function only supports 2 parameter but got %d", len(args))
	}
	if arg1, ok := args[1].(ast.Expr); ok {
		if _, ok := arg1.(*ast.StringLiteral); !ok {
			return fmt.Errorf("the second parameter of wordCount function must be a string literal")
		}
	}
	return nil
}

func (f *accumulateWordCountFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	logger := ctx.GetLogger()
	fmt.Printf("Exec accumulate")
	col, ok := args[0].(string)
	if !ok {
		logger.Debugf("Exec accumulateWordCountFunc with arg0 %s", col)
		return fmt.Errorf("args[0] is not a string, got %v", args[0]), false
	}

	sep, ok := args[1].(string)
	if !ok {
		logger.Debugf("Exec accumulateWordCountFunc with arg1 %s", sep)
		return fmt.Errorf("args[1] is not a string, got %v", args[0]), false
	}

	err := ctx.IncrCounter("allwordcount", len(strings.Split(col, sep)))
	if err != nil {
		logger.Debugf("call accumulateWordCountFunc incrCounter error %s", err)
		return err, false
	}
	if c, err := ctx.GetCounter("allwordcount"); err != nil {
		logger.Debugf("call accumulateWordCountFunc getCounter error %s", err)
		return err, false
	} else {
		return c, true
	}
}

func (f *accumulateWordCountFunc) IsAggregate() bool {
	return false
}

func AccumulateWordCount() api.Function {
	return &accumulateWordCountFunc{}
}
