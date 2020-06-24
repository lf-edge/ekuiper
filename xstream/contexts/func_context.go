package contexts

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
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

func (c *DefaultFuncContext) IncrCounter(key string, amount int) error {
	return c.StreamContext.IncrCounter(c.convertKey(key), amount)
}

func (c *DefaultFuncContext) GetCounter(key string) (int, error) {
	return c.StreamContext.GetCounter(c.convertKey(key))
}

func (c *DefaultFuncContext) PutState(key string, value interface{}) error {
	return c.StreamContext.PutState(c.convertKey(key), value)
}

func (c *DefaultFuncContext) GetState(key string) (interface{}, error) {
	return c.StreamContext.GetState(c.convertKey(key))
}

func (c *DefaultFuncContext) DeleteState(key string) error {
	return c.StreamContext.DeleteState(c.convertKey(key))
}

func (c *DefaultFuncContext) GetFuncId() int {
	return c.funcId
}

func (c *DefaultFuncContext) convertKey(key string) string {
	return fmt.Sprintf("$$func%d_%s", c.funcId, key)
}
