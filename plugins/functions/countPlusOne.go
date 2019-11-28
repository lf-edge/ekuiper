package main

import "fmt"

type countPlusOneFunc struct {
}

func (f *countPlusOneFunc) Validate(args []interface{}) error{
	if len(args) != 1{
		return fmt.Errorf("countPlusOne function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *countPlusOneFunc) Exec(args []interface{}) (interface{}, bool) {
	arg := args[0].([]interface{})
	return len(arg) + 1, true
}

func (f *countPlusOneFunc) IsAggregate() bool {
	return true
}

var CountPlusOne countPlusOneFunc
