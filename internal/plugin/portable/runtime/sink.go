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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.nanomsg.org/mangos/v3"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type sinkConf struct {
	RequireAck bool `json:"requireAck"`
}

type PortableSink struct {
	symbolName string
	reg        *PluginMeta
	props      map[string]interface{}
	dataCh     DataOutChannel
	ackCh      DataInChannel
	c          *sinkConf
	clean      func() error
}

func (ps *PortableSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	ps.props = configs
	c := &sinkConf{}
	err := cast.MapToStruct(configs, c)
	if err != nil {
		return err
	}
	ps.c = c
	ctx.GetLogger().Infof("require ack: %v", c.RequireAck)
	return nil
}

func (ps *PortableSink) Connect(ctx api.StreamContext, _ api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Start running portable sink %s with conf %+v", ps.symbolName, ps.props)
	pm := GetPluginInsManager()
	ins, err := pm.GetOrStartProcess(ps.reg, PortbleConf)
	if err != nil {
		return err
	}
	ctx.GetLogger().Infof("Plugin started successfully")

	ackCh, err := CreateSinkAckChannel(ctx)
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
		_ = ins.StopSymbol(ctx, c)
		return err
	}

	ps.clean = func() error {
		ctx.GetLogger().Info("clean up sink")
		err1 := dataCh.Close()
		err2 := ackCh.Close()
		err3 := ins.StopSymbol(ctx, c)
		if err1 != nil {
			err1 = fmt.Errorf("%s:%v", "close dataCh error", err1)
		}
		if err2 != nil {
			err2 = fmt.Errorf("%s:%v", "close ackCh error", err2)
		}
		if err3 != nil {
			err3 = fmt.Errorf("%s:%v", "close symbol error", err3)
		}
		return errors.Join(err1, err2, err3)
	}
	ps.dataCh = dataCh
	ps.ackCh = ackCh
	return nil
}

func (ps *PortableSink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	//ctx.GetLogger().Debugf("Receive %+v", item)
	e := ps.dataCh.Send(item.Raw())
	if e != nil {
		return errorx.NewIOErr(e.Error())
	}
	if ps.c.RequireAck {
		msg, err := recvAck(ctx, ps.ackCh)
		if err != nil {
			return err
		}
		r := &ackResponse{}
		if err := json.Unmarshal(msg, r); err != nil {
			return err
		}
		if len(r.Error) > 0 {
			return errorx.NewIOErr(r.Error)
		}
	}
	return nil
}

func NewPortableSink(symbolName string, reg *PluginMeta) *PortableSink {
	return &PortableSink{
		symbolName: symbolName,
		reg:        reg,
	}
}

type ackResponse struct {
	Error string `json:"error"`
}

func (ps *PortableSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sink %s", ps.symbolName)
	if ps.clean != nil {
		return ps.clean()
	}
	return nil
}

func recvAck(ctx api.StreamContext, dataCh DataInChannel) ([]byte, error) {
	var msg []byte
	var err error
	// make sure recv has timeout
	for {
		msg, err = dataCh.Recv()
		switch err {
		case mangos.ErrClosed:
			ctx.GetLogger().Info("stop sink ack after close")
			return nil, err
		case mangos.ErrRecvTimeout:
			ctx.GetLogger().Debug("sink ack receive timeout, retry")
			select {
			case <-ctx.Done():
				ctx.GetLogger().Info("stop sink ack")
			default:
				continue
			}
		case nil:
			return msg, nil
		}
	}
}
