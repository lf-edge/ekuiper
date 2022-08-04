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
		fmt.Println("[plugin][portable][runtime][function.go] StartSymbol err: ", err)
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
	fmt.Println("[internal][plugin][portable][runtime][function.go] Exec")
	ctx.GetLogger().Debugf("running portable func with args %+v", args)
	ctxRaw, err := encodeCtx(ctx)
	fmt.Println("[internal][plugin][portable][runtime][function.go] ctxRaw: ", ctxRaw)
	if err != nil {
		return err, false
	}
	jsonArg, err := encode("Exec", append(args, ctxRaw))
	if err != nil {
		return err, false
	}
	fmt.Println("[internal][plugin][portable][runtime][function.go] jsonArg(string):", string(jsonArg))
	res, err := f.dataCh.Req(jsonArg)
	/*
		[internal][plugin][portable][runtime][function.go] args:  [twelve]
		[internal][plugin][portable][runtime][function.go] ctx:  &{0xc0001be480 1}
		[internal][plugin][portable][runtime][function.go] jsonArg:  [123 34 102 117 110 99 34 58 34 69 120 101 99 34 44
		34 97 114 103 34 58 91 34 116 119 101 108 118 101 34 44 34 123 92 34 114 117 108 101 73 100 92 34 58 92 34 114 117
		108 101 49 92 34 44 92 34 111 112 73 100 92 34 58 92 34 111 112 49 92 34 44 92 34 105 110 115 116 97 110 99 101 73
		100 92 34 58 49 44 92 34 102 117 110 99 73 100 92 34 58 49 125 34 93 125]
		[internal][plugin][portable][runtime][function.go] res:  [123 34 115 116 97 116 101 34 58 116 114 117 101 44 34
		114 101 115 117 108 116 34 58 34 116 119 101 108 118 101 34 125]
	*/
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
