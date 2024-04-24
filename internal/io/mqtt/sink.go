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

package mqtt

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/io"
	"github.com/lf-edge/ekuiper/v2/internal/topo/connection/clients"
	mqttClient "github.com/lf-edge/ekuiper/v2/internal/topo/connection/clients/mqtt"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

// AdConf is the advanced configuration for the mqtt sink
type AdConf struct {
	Tpc      string `json:"topic"`
	Qos      byte   `json:"qos"`
	Retained bool   `json:"retained"`

	ResendTopic string `json:"resendDestination"`
}

type MQTTSink struct {
	adconf     *AdConf
	config     map[string]interface{}
	cli        io.MessageClient
	sendParams map[string]any
}

func (ms *MQTTSink) Provision(_ api.StreamContext, ps map[string]any) error {
	adconf := &AdConf{}
	err := cast.MapToStruct(ps, adconf)
	if err != nil {
		return err
	}

	if adconf.Tpc == "" {
		return fmt.Errorf("mqtt sink is missing property topic")
	}
	if err := validateMQTTSinkTopic(adconf.Tpc); err != nil {
		return err
	}
	if adconf.Qos != 0 && adconf.Qos != 1 && adconf.Qos != 2 {
		return fmt.Errorf("invalid qos value %v, the value could be only int 0 or 1 or 2", adconf.Qos)
	}
	ms.config = ps
	if adconf.ResendTopic == "" {
		adconf.ResendTopic = adconf.Tpc
	}
	ms.adconf = adconf
	ms.sendParams = map[string]any{
		"qos":      adconf.Qos,
		"retained": adconf.Retained,
	}
	mc := &mqttClient.MQTTClient{}
	return mc.CfgValidate(ms.config)
}

func (ms *MQTTSink) Connect(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	cli, err := clients.GetClient("mqtt", ms.config)
	if err != nil {
		log.Errorf("found error when get mqtt client config %v, error %s", ms.config, err.Error())
		return err
	}
	ms.cli = cli

	return nil
}

func validateMQTTSinkTopic(topic string) error {
	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return fmt.Errorf("mqtt sink topic shouldn't contain # or +")
	}
	return nil
}

func (ms *MQTTSink) Collect(ctx api.StreamContext, item []byte) error {
	tpc, err := ctx.ParseTemplate(ms.adconf.Tpc, item)
	if err != nil {
		return err
	}

	if err := ms.cli.Publish(ctx, tpc, item, ms.sendParams); err != nil {
		return errorx.NewIOErr(err.Error())
	}
	return nil
}

func (ms *MQTTSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing mqtt sink")
	if ms.cli != nil {
		clients.ReleaseClient(ctx, ms.cli)
	}
	return nil
}

var _ api.BytesCollector = &MQTTSink{}
