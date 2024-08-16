// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.nanomsg.org/mangos/v3"

	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// Error handling: wrap all error in a function to handle

type PortableSource struct {
	symbolName string
	reg        *PluginMeta
	clean      func() error
	dataCh     DataInChannel

	topic string
	props map[string]any
}

func (ps *PortableSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	ps.props = configs
	return nil
}

func (ps *PortableSource) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Start running portable source %s with datasource %s and conf %+v", ps.symbolName, ps.topic, ps.props)
	pm := GetPluginInsManager()
	ins, err := pm.GetOrStartProcess(ps.reg, PortbleConf)
	if err != nil {
		return err
	}
	ctx.GetLogger().Infof("Plugin started successfully")

	// wait for plugin data
	dataCh, err := CreateSourceChannel(ctx)
	if err != nil {
		return err
	}

	// Control: send message to plugin to ask starting symbol
	c := &Control{
		Meta: Meta{
			RuleId:     ctx.GetRuleId(),
			OpId:       ctx.GetOpId(),
			InstanceId: ctx.GetInstanceId(),
		},
		SymbolName: ps.symbolName,
		PluginType: TYPE_SOURCE,
		DataSource: ps.topic,
		Config:     ps.props,
	}
	err = ins.StartSymbol(ctx, c)
	if err != nil {
		ctx.GetLogger().Error(err)
		_ = dataCh.Close()
		return err
	}
	ps.dataCh = dataCh
	ps.clean = func() error {
		ctx.GetLogger().Info("clean up source")
		err1 := dataCh.Close()
		err2 := ins.StopSymbol(ctx, c)
		if err1 != nil {
			err1 = fmt.Errorf("%s:%v", "dataCh", err1)
		}
		if err2 != nil {
			err2 = fmt.Errorf("%s:%v", "symbol", err2)
		}
		return errors.Join(err1, err2)
	}
	return nil
}

func (ps *PortableSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	go func() {
		for {
			var msg []byte
			// make sure recv has timeout
			msg, err := ps.dataCh.Recv()
			switch err {
			case mangos.ErrClosed:
				ctx.GetLogger().Info("stop source after close")
				return
			case mangos.ErrRecvTimeout:
				ctx.GetLogger().Debug("source receive timeout, retry")
				select {
				case <-ctx.Done():
					ctx.GetLogger().Info("stop source")
					return
				default:
					continue
				}
			case nil:
				// do nothing
			default:
				ingestError(ctx, err)
				return
			}
			rcvTime := timex.GetNow()
			ingest(ctx, msg, map[string]interface{}{}, rcvTime)
		}
	}()
	return nil
}

func NewPortableSource(symbolName string, reg *PluginMeta) *PortableSource {
	return &PortableSource{
		symbolName: symbolName,
		reg:        reg,
	}
}

func (ps *PortableSource) Configure(topic string, props map[string]interface{}) error {
	ps.topic = topic
	ps.props = props
	return nil
}

func (ps *PortableSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing source %s", ps.symbolName)
	if ps.clean != nil {
		return ps.clean()
	}
	return nil
}
