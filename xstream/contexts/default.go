package contexts

import (
	"context"
	"engine/common"
	"github.com/sirupsen/logrus"
)

type DefaultContext struct {
	ruleId string
	opId   string
	ctx    context.Context
	logger *logrus.Entry
}

func NewDefaultContext(ruleId string, opId string, ctx context.Context) *DefaultContext{
	c := &DefaultContext{
		ruleId: ruleId,
		opId:	opId,
		ctx:    ctx,
		logger: common.GetLogger(ctx),
	}
	return c
}

func (c *DefaultContext) GetContext() context.Context {
	return c.ctx
}

func (c *DefaultContext) GetLogger() *logrus.Entry {
	return c.logger
}

func (c *DefaultContext) GetRuleId() string {
	return c.ruleId
}

func (c *DefaultContext) GetOpId() string {
	return c.opId
}