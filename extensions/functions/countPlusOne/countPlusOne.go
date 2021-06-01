package main

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
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
