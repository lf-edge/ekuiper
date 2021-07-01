package main

import (
	"fmt"
	"github.com/emqx/kuiper/pkg/api"
	"github.com/emqx/kuiper/pkg/ast"
	"strings"
)

/**
 **	A function which will count how many words had been received from the beginning
 ** to demonstrate how to use states
 ** There are 2 arguments:
 **  0: column, the column to be calculated. The column value type must be string
 **  1: separator, a string literal for word separator
 **/

type accumulateWordCountFunc struct {
}

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
