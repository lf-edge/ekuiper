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

package runtime

import (
	context2 "context"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/sdk/go/api"
	"github.com/lf-edge/ekuiper/sdk/go/connection"
	"github.com/lf-edge/ekuiper/sdk/go/context"
	"sync"
)

type funcRuntime struct {
	s      api.Function
	ch     connection.DataInOutChannel
	ctx    context2.Context
	cancel context2.CancelFunc
	key    string
}

func setupFuncRuntime(con *Control, s api.Function) (*funcRuntime, error) {
	// connect to mq server
	ch, err := connection.CreateFuncChannel(con.SymbolName)
	if err != nil {
		return nil, err
	}
	context.Log.Info("setup function channel")
	ctx, cancel := context2.WithCancel(context2.Background())
	return &funcRuntime{
		s:      s,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
		key:    fmt.Sprintf("func_%s", con.SymbolName),
	}, nil
}

// TODO how to stop? Nearly never end because each function only have one instance
func (s *funcRuntime) run() {
	fmt.Println("[function.go][run()] start")
	defer s.stop()
	err := s.ch.Run(func(req []byte) []byte {
		d := &FuncData{}
		err := json.Unmarshal(req, d)
		if err != nil {
			return encodeReply(false, err)
		}
		context.Log.Debugf("running func with %+v", d)
		switch d.Func {
		case "Validate":
			arg, ok := d.Arg.([]interface{})
			if !ok {
				return encodeReply(false, "argument is not interface array")
			}
			err = s.s.Validate(arg)
			if err == nil {
				return encodeReply(true, "")
			} else {
				return encodeReply(false, err.Error())
			}
		case "Exec":
			fmt.Println("[function.go][run()][switch-case] Exec")
			arg, ok := d.Arg.([]interface{})
			if !ok {
				return encodeReply(false, "argument is not interface array")
			}
			fmt.Println("[function.go][run()][switch-case] arg: ", arg)
			farg, fctx, err := parseFuncContextArgs(arg)
			if err != nil {
				return encodeReply(false, err.Error())
			}
			r, b := s.s.Exec(farg, fctx)
			return encodeReply(b, r)
		case "IsAggregate":
			result := s.s.IsAggregate()
			return encodeReply(true, result)
		default:
			return encodeReply(false, fmt.Sprintf("invalid func %s", d.Func))
		}
	})
	context.Log.Error(err)
}

// TODO multiple error
func (s *funcRuntime) stop() error {
	s.cancel()
	err := s.ch.Close()
	if err != nil {
		context.Log.Info(err)
	}
	context.Log.Info("closed function data channel")
	reg.Delete(s.key)
	return nil
}

func (s *funcRuntime) isRunning() bool {
	return s.ctx.Err() == nil
}

func encodeReply(state bool, arg interface{}) []byte {
	r, _ := json.Marshal(FuncReply{
		State:  state,
		Result: arg,
	})
	return r
}

func parseFuncContextArgs(args []interface{}) ([]interface{}, api.FunctionContext, error) {
	if len(args) < 1 {
		return nil, nil, fmt.Errorf("exec function context not found")
	}
	fargs, temp := args[:len(args)-1], args[len(args)-1]
	rawCtx, ok := temp.(string)
	if !ok {
		return nil, nil, fmt.Errorf("cannot parse function raw context %v", temp)
	}
	fmt.Println("[function.go][run()][switch-case][parseFuncContextArgs] fargs: ", fargs)
	fmt.Println("[function.go][run()][switch-case][parseFuncContextArgs] temp: ", temp)
	// {"ruleId":"rule1","opId":"op1","instanceId":1,"funcId":1}
	m := &FuncMeta{}
	err := json.Unmarshal([]byte(rawCtx), m)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse function context %v", rawCtx)
	}
	fmt.Println("[function.go][run()][switch-case][parseFuncContextArgs] m: ", m)
	if m.RuleId == "" || m.OpId == "" {
		err := fmt.Sprintf("invalid arg %v, ruleId, opId are required", m)
		context.Log.Errorf(err)
		return nil, nil, fmt.Errorf(err)
	}
	key := fmt.Sprintf("%s_%s_%d_%d", m.RuleId, m.OpId, m.InstanceId, m.FuncId)
	if c, ok := exeFuncCtxMap.Load(key); ok {
		return args, c.(api.FunctionContext), nil
	} else {
		contextLogger := context.LogEntry("rule", m.RuleId)
		ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta(m.RuleId, m.OpId)
		fctx := context.NewDefaultFuncContext(ctx, m.FuncId)
		exeFuncCtxMap.Store(key, fctx)
		return args, fctx, nil
	}
}

var exeFuncCtxMap = &sync.Map{}
