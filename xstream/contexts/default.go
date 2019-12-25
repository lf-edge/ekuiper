package contexts

import (
	"context"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
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
}

func Background() *DefaultContext {
	c := &DefaultContext{
		ctx: context.Background(),
	}
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

func (c *DefaultContext) GetError() error {
	return c.err
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
	}
}

func (c *DefaultContext) WithInstance(instanceId int) api.StreamContext {
	return &DefaultContext{
		instanceId: instanceId,
		ruleId:     c.ruleId,
		opId:       c.opId,
		ctx:        c.ctx,
	}
}

func (c *DefaultContext) WithCancel() (api.StreamContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.ctx)
	return &DefaultContext{
		ruleId:     c.ruleId,
		opId:       c.opId,
		instanceId: c.instanceId,
		ctx:        ctx,
	}, cancel
}
