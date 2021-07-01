package service

import (
	"github.com/emqx/kuiper/pkg/api"
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
