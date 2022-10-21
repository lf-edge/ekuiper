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

package sink

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type MQTTSink struct {
	tpc      string
	qos      byte
	retained bool

	config map[string]interface{}
	cli    api.MessageClient
}

func (ms *MQTTSink) hasKeys(str []string, ps map[string]interface{}) bool {
	for _, v := range str {
		if _, ok := ps[v]; ok {
			return true
		}
	}
	return false
}

func (ms *MQTTSink) Configure(ps map[string]interface{}) error {
	tpc, ok := ps["topic"]
	if !ok {
		return fmt.Errorf("mqtt sink is missing property topic")
	}

	var qos byte = 0
	if qosRec, ok := ps["qos"]; ok {
		if v, err := cast.ToInt(qosRec, cast.STRICT); err == nil {
			qos = byte(v)
		}
		if qos != 0 && qos != 1 && qos != 2 {
			return fmt.Errorf("not valid qos value %v, the value could be only int 0 or 1 or 2", qos)
		}
	}

	retained := false
	if pk, ok := ps["retained"]; ok {
		if v, ok := pk.(bool); ok {
			retained = v
		}
	}

	ms.config = ps
	ms.qos = qos
	ms.tpc = tpc.(string)
	ms.retained = retained

	return nil
}

func (ms *MQTTSink) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	cli, err := clients.GetClient("mqtt", ms.config)
	if err != nil {
		log.Errorf("found error when get mqtt client config %v, error %s", ms.config, err.Error())
		return err
	}

	ms.cli = cli

	return nil
}

func (ms *MQTTSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	jsonBytes, _, err := ctx.TransformOutput(item)
	if err != nil {
		return err
	}

	logger.Debugf("%s publish %s", ctx.GetOpId(), jsonBytes)
	tpc, err := ctx.ParseTemplate(ms.tpc, item)
	if err != nil {
		return err
	}

	para := map[string]interface{}{
		"qos":      ms.qos,
		"retained": ms.retained,
	}

	if err := ms.cli.Publish(ctx, tpc, jsonBytes, para); err != nil {
		return fmt.Errorf("%s: %s", errorx.IOErr, err.Error())
	}
	return nil
}

func (ms *MQTTSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing mqtt sink")
	if ms.cli != nil {
		ms.cli.Release(ctx)
	}
	return nil
}
