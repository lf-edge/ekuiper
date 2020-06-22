package contexts

import (
	"context"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/states"
	"github.com/sirupsen/logrus"
	"time"
)

const LoggerKey = "$$logger"

type DefaultContext struct {
	ruleId     string
	opId       string
	instanceId int
	ctx        context.Context
	err        error

	state states.StateContext
}

func Background() *DefaultContext {
	c := &DefaultContext{
		ctx: context.Background(),
	}
	s := states.NewStateContext(states.MEMORY, c.GetLogger())
	c.state = s
	return c
}

func WithValue(parent *DefaultContext, key, val interface{}) *DefaultContext {
	parent.ctx = context.WithValue(parent.ctx, key, val)
	return parent
}

//Implement context interface
func (c *DefaultContext) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *DefaultContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *DefaultContext) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.ctx.Err()
}

func (c *DefaultContext) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}

// Stream metas
func (c *DefaultContext) GetContext() context.Context {
	return c.ctx
}

func (c *DefaultContext) GetLogger() api.Logger {
	l, ok := c.ctx.Value(LoggerKey).(*logrus.Entry)
	if l != nil && ok {
		return l
	}
	return common.Log.WithField("caller", "default")
}

func (c *DefaultContext) GetRuleId() string {
	return c.ruleId
}

func (c *DefaultContext) GetOpId() string {
	return c.opId
}

func (c *DefaultContext) GetInstanceId() int {
	return c.instanceId
}

func (c *DefaultContext) SetError(err error) {
	c.err = err
}

func (c *DefaultContext) WithMeta(ruleId string, opId string) api.StreamContext {
	return &DefaultContext{
		ruleId:     ruleId,
		opId:       opId,
		instanceId: 0,
		ctx:        c.ctx,
		state:      c.state,
	}
}

func (c *DefaultContext) WithInstance(instanceId int) api.StreamContext {
	return &DefaultContext{
		instanceId: instanceId,
		ruleId:     c.ruleId,
		opId:       c.opId,
		ctx:        c.ctx,
		state:      c.state,
	}
}

func (c *DefaultContext) WithCancel() (api.StreamContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.ctx)
	return &DefaultContext{
		ruleId:     c.ruleId,
		opId:       c.opId,
		instanceId: c.instanceId,
		ctx:        ctx,
		state:      c.state,
	}, cancel
}

func (c *DefaultContext) IncrCounter(key string, amount int) error {
	return c.state.IncrCounter(key, amount)
}

func (c *DefaultContext) GetCounter(key string) (int, error) {
	return c.state.GetCounter(key)
}

func (c *DefaultContext) PutState(key string, value interface{}) error {
	return c.state.PutState(key, value)
}

func (c *DefaultContext) GetState(key string) (interface{}, error) {
	return c.state.GetState(key)
}

func (c *DefaultContext) DeleteState(key string) error {
	return c.state.DeleteState(key)
}
