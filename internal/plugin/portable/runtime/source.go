// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"go.nanomsg.org/mangos/v3"
)

// Error handling: wrap all error in a function to handle

type PortableSource struct {
	symbolName string
	reg        *PluginMeta
	clean      func() error

	topic string
	props map[string]interface{}
}

func NewPortableSource(symbolName string, reg *PluginMeta) *PortableSource {
	return &PortableSource{
		symbolName: symbolName,
		reg:        reg,
	}
}

func (ps *PortableSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	ctx.GetLogger().Infof("Start running portable source %s with datasource %s and conf %+v", ps.symbolName, ps.topic, ps.props)
	pm := GetPluginInsManager()
	ins, err := pm.getOrStartProcess(ps.reg, PortbleConf)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	ctx.GetLogger().Infof("Plugin started successfully")

	// wait for plugin data
	dataCh, err := CreateSourceChannel(ctx)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}

	// Control: send message to plugin to ask starting symbol
	c := &Control{
		Meta: &Meta{
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
		infra.DrainError(ctx, err, errCh)
		_ = dataCh.Close()
		return
	}
	ps.clean = func() error {
		ctx.GetLogger().Info("clean up source")
		err1 := dataCh.Close()
		err2 := ins.StopSymbol(ctx, c)
		e := make(errorx.MultiError)
		if err1 != nil {
			e["dataCh"] = err1
		}
		if err != nil {
			e["symbol"] = err2
		}
		return e.GetError()
	}

	for {
		var msg []byte
		// make sure recv has timeout
		msg, err = dataCh.Recv()
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
			infra.DrainError(ctx, fmt.Errorf("cannot receive from mangos Socket: %s", err.Error()), errCh)
			return
		}
		result := &api.DefaultSourceTuple{}
		e := json.Unmarshal(msg, result)
		if e != nil {
			ctx.GetLogger().Errorf("Invalid data format, cannot decode %s to json format with error %s", string(msg), e)
			continue
		}
		select {
		case consumer <- result:
			ctx.GetLogger().Debugf("send data to source node")
		case <-ctx.Done():
			ctx.GetLogger().Info("stop source")
			return
		}
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
