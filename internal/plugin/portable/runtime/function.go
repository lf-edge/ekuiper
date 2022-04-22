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

package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// PortableFunc each function symbol only has a singleton
// Each singleton are long running go routine.
// TODO think about ending a portable func when needed.
type PortableFunc struct {
	symbolName string
	reg        *PluginMeta
	dataCh     DataReqChannel
	isAgg      int // 0 - not calculate yet, 1 - no, 2 - yes
}

func NewPortableFunc(symbolName string, reg *PluginMeta) (*PortableFunc, error) {
	// Setup channel and route the data
	conf.Log.Infof("Start running portable function meta %+v", reg)
	pm := GetPluginInsManager()
	ins, err := pm.getOrStartProcess(reg, PortbleConf)
	if err != nil {
		return nil, err
	}
	conf.Log.Infof("Plugin started successfully")

	// Create function channel
	dataCh, err := CreateFunctionChannel(symbolName)
	if err != nil {
		return nil, err
	}

	// Start symbol
	c := &Control{
		SymbolName: symbolName,
		PluginType: TYPE_FUNC,
	}
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, conf.Log)
	err = ins.StartSymbol(ctx, c)
	if err != nil {
		return nil, err
	}

	err = dataCh.Handshake()
	if err != nil {
		return nil, fmt.Errorf("function %s handshake error: %v", reg.Name, err)
	}

	return &PortableFunc{
		symbolName: reg.Name,
		reg:        reg,
		dataCh:     dataCh,
	}, nil
}

func (f *PortableFunc) Validate(args []interface{}) error {
	// TODO function arg encoding
	jsonArg, err := encode("Validate", args)
	if err != nil {
		return err
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		return err
	}
	fr := &FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		return err
	}
	if fr.State {
		return nil
	} else {
		return fmt.Errorf("validate return state is false, got %+v", fr)
	}
}

func (f *PortableFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	ctx.GetLogger().Debugf("running portable func with args %+v", args)
	ctxRaw, err := encodeCtx(ctx)
	if err != nil {
		return err, false
	}
	jsonArg, err := encode("Exec", append(args, ctxRaw))
	if err != nil {
		return err, false
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		return err, false
	}
	fr := &FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		return err, false
	}
	if !fr.State {
		if fr.Result != nil {
			return fmt.Errorf("%s", fr.Result), false
		} else {
			return nil, false
		}
	}
	return fr.Result, fr.State
}

func (f *PortableFunc) IsAggregate() bool {
	if f.isAgg > 0 {
		return f.isAgg > 1
	}
	jsonArg, err := encode("IsAggregate", nil)
	if err != nil {
		conf.Log.Error(err)
		return false
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		conf.Log.Error(err)
		return false
	}
	fr := &FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		conf.Log.Error(err)
		return false
	}
	if fr.State {
		r, ok := fr.Result.(bool)
		if !ok {
			conf.Log.Errorf("IsAggregate result is not bool, got %s", string(res))
			return false
		} else {
			if r {
				f.isAgg = 2
			} else {
				f.isAgg = 1
			}
			return r
		}
	} else {
		conf.Log.Errorf("IsAggregate return state is false, got %+v", fr)
		return false
	}
}

func (f *PortableFunc) Close() error {
	return f.dataCh.Close()
	// Symbol must be closed by instance manager
	//		ins.StopSymbol(ctx, c)
}

func encode(funcName string, arg interface{}) ([]byte, error) {
	c := FuncData{
		Func: funcName,
		Arg:  arg,
	}
	return json.Marshal(c)
}

func encodeCtx(ctx api.FunctionContext) (string, error) {
	m := FuncMeta{
		Meta: Meta{
			RuleId:     ctx.GetRuleId(),
			OpId:       ctx.GetOpId(),
			InstanceId: ctx.GetInstanceId(),
		},
		FuncId: ctx.GetFuncId(),
	}
	bs, err := json.Marshal(m)
	return string(bs), err
}
