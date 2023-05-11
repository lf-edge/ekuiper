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

package context

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/lf-edge/ekuiper/sdk/go/api"
)

const LoggerKey = "$$logger"

type DefaultContext struct {
	ruleId     string
	opId       string
	instanceId int
	ctx        context.Context
	// Only initialized after withMeta set
	logger api.Logger
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

// Implement context interface
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
	return LogEntry("rule", c.ruleId)
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
