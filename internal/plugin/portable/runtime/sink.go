// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

	"go.nanomsg.org/mangos/v3"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type PortableSink struct {
	symbolName string
	reg        *PluginMeta
	props      map[string]interface{}
	dataCh     DataOutChannel
	ackCh      DataInChannel
	// 0 indicates no ack, and 1 indicates need ack
	requiredACKs int
	clean        func() error
}

func NewPortableSink(symbolName string, reg *PluginMeta) *PortableSink {
	return &PortableSink{
		symbolName: symbolName,
		reg:        reg,
	}
}

func (ps *PortableSink) Configure(props map[string]interface{}) error {
	ps.props = props
	c, ok := props["requiredACKs"]
	if ok {
		acks, ok := c.(int)
		if ok {
			ps.requiredACKs = acks
		}
	}
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
		err2 := ins.StopSymbol(ctx, c)
		if err1 != nil {
			err1 = fmt.Errorf("%s:%v", "dataCh", err1)
		}
		if err2 != nil {
			err2 = fmt.Errorf("%s:%v", "symbol", err2)
		}
		return errors.Join(err1, err2)
	}
	ps.dataCh = dataCh
	ps.ackCh = ackCh
	return nil
}

func (ps *PortableSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("Receive %+v", item)
	if val, _, err := ctx.TransformOutput(item); err == nil {
		ctx.GetLogger().Debugf("Send %s", val)
		e := ps.dataCh.Send(val)
		if e != nil {
			return errorx.NewIOErr(e.Error())
		}
		if ps.requiredACKs > 0 {
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
	} else {
		ctx.GetLogger().Errorf("Found error %s", err.Error())
		return err
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
			ctx.GetLogger().Info("stop source after close")
			return nil, err
		case mangos.ErrRecvTimeout:
			ctx.GetLogger().Debug("source receive timeout, retry")
			select {
			case <-ctx.Done():
				ctx.GetLogger().Info("stop dataInChannel")
			default:
				continue
			}
		case nil:
			return msg, nil
		}
	}
}
