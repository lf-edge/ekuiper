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
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type PortableSink struct {
	symbolName string
	reg        *PluginMeta
	props      map[string]interface{}
	dataCh     DataOutChannel
	clean      func() error
}

func NewPortableSink(symbolName string, reg *PluginMeta) *PortableSink {
	return &PortableSink{
		symbolName: symbolName,
		reg:        reg,
	}
}

func (ps *PortableSink) Configure(props map[string]interface{}) error {
	ps.props = props
	return nil
}

func (ps *PortableSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Start running portable sink %s with conf %+v", ps.symbolName, ps.props)
	pm := GetPluginInsManager()
	ins, err := pm.getOrStartProcess(ps.reg, PortbleConf)
	if err != nil {
		return err
	}
	ctx.GetLogger().Infof("Plugin started successfully")

	// Control: send message to plugin to ask starting symbol
	c := &Control{
		Meta: &Meta{
			RuleId:     ctx.GetRuleId(),
			OpId:       ctx.GetOpId(),
			InstanceId: ctx.GetInstanceId(),
		},
		SymbolName: ps.symbolName,
		PluginType: TYPE_SINK,
		Config:     ps.props,
	}
	err = ins.StartSymbol(ctx, c)
	if err != nil {
		return err
	}

	// must start symbol firstly
	dataCh, err := CreateSinkChannel(ctx)
	if err != nil {
		return err
	}

	ps.clean = func() error {
		ctx.GetLogger().Info("closing sink data channe")
		dataCh.Close()
		return ins.StopSymbol(ctx, c)
	}
	ps.dataCh = dataCh
	return nil
}

func (ps *PortableSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("Receive %+v", item)
	// TODO item type
	switch input := item.(type) {
	case []byte:
		return ps.dataCh.Send(input)
	default:
		return ps.dataCh.Send([]byte(fmt.Sprintf("%v", input)))
	}
}

func (ps *PortableSink) Close(ctx api.StreamContext) error {
	if ps.clean != nil {
		return ps.clean()
	}
	return nil
}
