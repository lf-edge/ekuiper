package main

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
)

type echo struct {
}

func (f *echo) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("echo function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *echo) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	result := args[0]
	return result, true
}

func (f *echo) IsAggregate() bool {
	return false
}

var Echo echo
