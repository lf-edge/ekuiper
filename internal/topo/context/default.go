// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"regexp"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/sirupsen/logrus"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/transform"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

const (
	LoggerKey        = "$$logger"
	RuleStartKey     = "$$ruleStart"
	RuleWaitGroupKey = "$$ruleWaitGroup"
	TraceStrategyKey = "$$TraceStrategyKey"
)

const (
	AlwaysTraceStrategy = iota
	HeadTraceStrategy
)

type TraceStrategy int

type TraceStrategyWrapper struct {
	sync.RWMutex
	Strategy TraceStrategy
}

type DefaultContext struct {
	ruleId         string
	opId           string
	instanceId     int
	ctx            context.Context
	err            error
	isTraceEnabled *atomic.Bool
	strategy       *TraceStrategyWrapper
	// Only initialized after withMeta set
	store    api.Store
	state    *sync.Map
	snapshot map[string]interface{}
	// cache
	tpReg sync.Map
	jpReg sync.Map
}

func RuleBackground(ruleName string) *DefaultContext {
	if !conf.Config.Basic.EnableResourceProfiling {
		return Background()
	}
	ctx := pprof.WithLabels(context.Background(), pprof.Labels("rule", ruleName))
	pprof.SetGoroutineLabels(ctx)
	c := &DefaultContext{
		ctx:            ctx,
		isTraceEnabled: &atomic.Bool{},
		strategy:       &TraceStrategyWrapper{Strategy: AlwaysTraceStrategy},
	}
	c.isTraceEnabled.Store(false)
	return c
}

func Background() *DefaultContext {
	c := &DefaultContext{
		ctx:            context.Background(),
		isTraceEnabled: &atomic.Bool{},
	}
	c.isTraceEnabled.Store(false)
	c.strategy = &TraceStrategyWrapper{Strategy: AlwaysTraceStrategy}
	return c
}

func WithContext(ctx context.Context) *DefaultContext {
	c := &DefaultContext{
		ctx:            ctx,
		isTraceEnabled: &atomic.Bool{},
	}
	c.isTraceEnabled.Store(false)
	c.strategy = &TraceStrategyWrapper{Strategy: AlwaysTraceStrategy}
	return c
}

func WithValue(parent *DefaultContext, key, val interface{}) *DefaultContext {
	parent.ctx = context.WithValue(parent.ctx, key, val)
	return parent
}

func (c *DefaultContext) PropagateTracer(par *DefaultContext) {
	c.isTraceEnabled = par.isTraceEnabled
	c.strategy = par.strategy
}

// Deadline Implement context interface
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

// ParseTemplate parse template string against data
// The templates are built only once and cached in the context by its raw string as the key
// If the prop string is not a template, a nil template is cached to indicate it has been parsed, and it will return the original string
func (c *DefaultContext) ParseTemplate(prop string, data interface{}) (string, error) {
	var (
		tp  *template.Template
		err error
	)
	if raw, ok := c.tpReg.Load(prop); ok {
		if raw != nil {
			tp = raw.(*template.Template)
		} else {
			return prop, nil
		}
	} else { // not parsed before
		re := regexp.MustCompile(`{{(.*?)}}`)
		// check if it is a template
		if re.Match([]byte(prop)) {
			tp, err = transform.GenTp(prop)
			if err != nil {
				return fmt.Sprintf("%v", data), fmt.Errorf("Template Invalid: %v", err)
			}
			c.tpReg.Store(prop, tp)
		} else {
			c.tpReg.Store(prop, nil)
			return prop, nil
		}
	}
	var output bytes.Buffer
	err = tp.Execute(&output, data)
	if err != nil {
		return fmt.Sprintf("%v", data), err
	}
	return output.String(), nil
}

func (c *DefaultContext) ParseJsonPath(prop string, data interface{}) (interface{}, error) {
	var (
		je  conf.JsonPathEval
		err error
	)
	if raw, ok := c.jpReg.Load(prop); ok {
		je = raw.(conf.JsonPathEval)
	} else {
		je, err = conf.GetJsonPathEval(prop)
		if err != nil {
			return nil, err
		}
		c.jpReg.Store(prop, je)
	}
	return je.Eval(data)
}

func (c *DefaultContext) WithMeta(ruleId string, opId string, store api.Store) api.StreamContext {
	s, err := store.GetOpState(opId)
	if err != nil {
		c.GetLogger().Warnf("Initialize context store error for %s: %s", opId, err)
	}
	return &DefaultContext{
		ruleId:         ruleId,
		opId:           opId,
		instanceId:     0,
		ctx:            c.ctx,
		store:          store,
		state:          s,
		tpReg:          sync.Map{},
		jpReg:          sync.Map{},
		isTraceEnabled: c.isTraceEnabled,
		strategy:       c.strategy,
	}
}

func (c *DefaultContext) WithInstance(instanceId int) api.StreamContext {
	return &DefaultContext{
		instanceId:     instanceId,
		ruleId:         c.ruleId,
		opId:           c.opId,
		ctx:            c.ctx,
		state:          c.state,
		isTraceEnabled: c.isTraceEnabled,
		strategy:       c.strategy,
	}
}

func (c *DefaultContext) WithRuleId(ruleId string) api.StreamContext {
	return &DefaultContext{
		instanceId:     c.instanceId,
		ruleId:         ruleId,
		opId:           c.opId,
		ctx:            c.ctx,
		state:          c.state,
		isTraceEnabled: c.isTraceEnabled,
		strategy:       c.strategy,
	}
}

func (c *DefaultContext) WithOpId(opId string) api.StreamContext {
	return &DefaultContext{
		instanceId:     c.instanceId,
		ruleId:         c.ruleId,
		opId:           opId,
		ctx:            c.ctx,
		state:          c.state,
		isTraceEnabled: c.isTraceEnabled,
		strategy:       c.strategy,
	}
}

func (c *DefaultContext) WithCancel() (api.StreamContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.ctx)
	return &DefaultContext{
		ruleId:         c.ruleId,
		opId:           c.opId,
		instanceId:     c.instanceId,
		ctx:            ctx,
		state:          c.state,
		isTraceEnabled: c.isTraceEnabled,
		strategy:       c.strategy,
	}, cancel
}

func (c *DefaultContext) IncrCounter(key string, amount int) error {
	for {
		if v, ok := c.state.Load(key); ok {
			if vi, err := cast.ToInt(v, cast.STRICT); err != nil {
				return fmt.Errorf("state[%s] must be an int", key)
			} else {
				if c.state.CompareAndSwap(key, vi, vi+amount) {
					break
				}
			}
		} else {
			if _, loaded := c.state.LoadOrStore(key, amount); !loaded {
				break
			}
		}
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
		c.state.CompareAndSwap(key, nil, 0)
		return 0, nil
	}
}

func (c *DefaultContext) GetAllState() map[string]interface{} {
	m := make(map[string]interface{})
	c.state.Range(func(key, value interface{}) bool {
		m[key.(string)] = value
		return true
	})
	return m
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

func (c *DefaultContext) EnableTracer(enabled bool) {
	c.isTraceEnabled.Store(enabled)
}

func (c *DefaultContext) IsTraceEnabled() bool {
	return c.isTraceEnabled.Load()
}

func (c *DefaultContext) GetStrategy() TraceStrategy {
	c.strategy.RLock()
	defer c.strategy.RUnlock()
	return c.strategy.Strategy
}

func (c *DefaultContext) SetStrategy(s TraceStrategy) {
	c.strategy.Lock()
	defer c.strategy.Unlock()
	c.strategy.Strategy = s
}

func StringToStrategy(s string) TraceStrategy {
	switch strings.ToLower(s) {
	case "always":
		return AlwaysTraceStrategy
	case "head":
		return HeadTraceStrategy
	}
	return AlwaysTraceStrategy
}
