package contexts

import (
	"context"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/sirupsen/logrus"
	"sync"
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

func (c *DefaultContext) GetRootPath() string {
	loc, _ := common.GetLoc("")
	return loc
}

func (c *DefaultContext) SetError(err error) {
	c.err = err
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
		if vi, err := common.ToInt(v, false); err != nil {
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
		if vi, err := common.ToInt(v, false); err != nil {
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
	c.snapshot = common.SyncMapToMap(c.state)
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
