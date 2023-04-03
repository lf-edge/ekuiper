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

package mock

import (
	"context"
	filename "github.com/keepeye/logrus-filename"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/sirupsen/logrus"
	"time"
)

type mockContext struct {
	Ctx    context.Context
	RuleId string
	OpId   string
}

// Implement context interface
func (c *mockContext) Deadline() (deadline time.Time, ok bool) {
	return c.Ctx.Deadline()
}

func (c *mockContext) Done() <-chan struct{} {
	return c.Ctx.Done()
}

func (c *mockContext) Err() error {
	return c.Ctx.Err()
}

func (c *mockContext) Value(key interface{}) interface{} {
	return c.Ctx.Value(key)
}

// Stream metas
func (c *mockContext) GetContext() context.Context {
	return c.Ctx
}

func (c *mockContext) GetLogger() api.Logger {
	return Logger
}

func (c *mockContext) GetRuleId() string {
	return c.RuleId
}

func (c *mockContext) GetOpId() string {
	return c.OpId
}

func (c *mockContext) GetInstanceId() int {
	return 0
}

func (c *mockContext) GetRootPath() string {
	//loc, _ := conf.GetLoc("")
	return "root path"
}

func (c *mockContext) SetError(err error) {

}

func (c *mockContext) WithMeta(ruleId string, opId string) api.StreamContext {
	return c
}

func (c *mockContext) WithInstance(_ int) api.StreamContext {
	return c
}

func (c *mockContext) WithCancel() (api.StreamContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.Ctx)
	return &mockContext{
		RuleId: c.RuleId,
		OpId:   c.OpId,
		Ctx:    ctx,
	}, cancel
}

func (c *mockContext) IncrCounter(key string, amount int) error {
	return nil
}

func (c *mockContext) GetCounter(key string) (int, error) {
	return 0, nil
}

func (c *mockContext) PutState(key string, value interface{}) error {
	return nil
}

func (c *mockContext) GetState(key string) (interface{}, error) {
	return nil, nil
}

func (c *mockContext) DeleteState(key string) error {
	return nil
}

func (c *mockContext) Snapshot() error {
	return nil
}

func (c *mockContext) SaveState(checkpointId int64) error {
	return nil
}

func newMockContext(ruleId string, opId string) api.StreamContext {
	return &mockContext{Ctx: context.Background(), RuleId: ruleId, OpId: opId}
}

type mockFuncContext struct {
	api.StreamContext
	funcId int
}

func (fc *mockFuncContext) GetFuncId() int {
	return fc.funcId
}

func newMockFuncContext(ctx api.StreamContext, id int) api.FunctionContext {
	return &mockFuncContext{
		StreamContext: ctx,
		funcId:        id,
	}
}

var Logger *logrus.Logger

func init() {
	l := logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "file"
	l.AddHook(filenameHook)
	l.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		DisableColors:   true,
		FullTimestamp:   true,
	})
	l.WithField("type", "main")
	Logger = l
}
