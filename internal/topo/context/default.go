// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"bytes"
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"
)

const LoggerKey = "$$logger"

type DefaultContext struct {
	ruleId     string
	opId       string
	instanceId int
	ctx        context.Context
	err        error
	//Only initialized after withMeta set
	store    api.Store
	state    *sync.Map
	snapshot map[string]interface{}
	// cache
	tpReg sync.Map
	jpReg sync.Map
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
	return conf.Log.WithField("caller", "default")
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

func (c *DefaultContext) GetRootPath() string {
	loc, _ := conf.GetLoc("")
	return loc
}

func (c *DefaultContext) SetError(err error) {
	c.err = err
}

func (c *DefaultContext) ParseDynamicProp(prop string, data interface{}) (interface{}, error) {
	re := regexp.MustCompile(`{{(.*?)}}`)
	if re.Match([]byte(prop)) {
		var (
			tp  *template.Template
			err error
		)
		if raw, ok := c.tpReg.Load(prop); ok {
			tp = raw.(*template.Template)
		} else {
			tp, err = transform.GenTp(prop)
			if err != nil {
				return fmt.Sprintf("%v", data), err
			}
			c.tpReg.Store(prop, tp)
		}
		var output bytes.Buffer
		err = tp.Execute(&output, data)
		if err != nil {
			return fmt.Sprintf("%v", data), err
		}
		return output.String(), nil
	} else if strings.HasPrefix(prop, "{$") { //TO BE REMOVED: will be extracted as a new function in the next release
		var (
			je  conf.JsonPathEval
			err error
		)
		if raw, ok := c.jpReg.Load(prop); ok {
			je = raw.(conf.JsonPathEval)
		} else {
			je, err = conf.GetJsonPathEval(prop[1:])
			if err != nil {
				return nil, err
			}
			c.jpReg.Store(prop, je)
		}
		return je.Eval(data)
	} else {
		return prop, nil
	}
}

func (c *DefaultContext) WithMeta(ruleId string, opId string, store api.Store) api.StreamContext {
	s, err := store.GetOpState(opId)
	if err != nil {
		c.GetLogger().Warnf("Initialize context store error for %s: %s", opId, err)
	}
	return &DefaultContext{
		ruleId:     ruleId,
		opId:       opId,
		instanceId: 0,
		ctx:        c.ctx,
		store:      store,
		state:      s,
		tpReg:      sync.Map{},
		jpReg:      sync.Map{},
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
	if v, ok := c.state.Load(key); ok {
		if vi, err := cast.ToInt(v, cast.STRICT); err != nil {
			return fmt.Errorf("state[%s] must be an int", key)
		} else {
			c.state.Store(key, vi+amount)
		}
	} else {
		c.state.Store(key, amount)
	}
	return nil
}

func (c *DefaultContext) GetCounter(key string) (int, error) {
	if v, ok := c.state.Load(key); ok {
		if vi, err := cast.ToInt(v, cast.STRICT); err != nil {
			return 0, fmt.Errorf("state[%s] is not a number, but %v", key, v)
		} else {
			return vi, nil
		}
	} else {
		c.state.Store(key, 0)
		return 0, nil
	}
}

func (c *DefaultContext) PutState(key string, value interface{}) error {
	c.state.Store(key, value)
	return nil
}

func (c *DefaultContext) GetState(key string) (interface{}, error) {
	if v, ok := c.state.Load(key); ok {
		return v, nil
	} else {
		return nil, nil
	}
}

func (c *DefaultContext) DeleteState(key string) error {
	c.state.Delete(key)
	return nil
}

func (c *DefaultContext) Snapshot() error {
	c.snapshot = cast.SyncMapToMap(c.state)
	return nil
}

func (c *DefaultContext) SaveState(checkpointId int64) error {
	err := c.store.SaveState(checkpointId, c.opId, c.snapshot)
	if err != nil {
		return err
	}
	c.snapshot = nil
	return nil
}

func (c *DefaultContext) GetConnection(connectSelector string) (interface{}, error) {
	return connection.GetConnection(connectSelector)
}

func (c *DefaultContext) ReleaseConnection(connectSelector string) {
	connection.ReleaseConnection(connectSelector)
}
