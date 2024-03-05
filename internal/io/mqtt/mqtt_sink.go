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

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	mqttClient "github.com/lf-edge/ekuiper/internal/topo/connection/clients/mqtt"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/message"
)

// AdConf is the advanced configuration for the mqtt sink
type AdConf struct {
	Tpc         string `json:"topic"`
	Qos         byte   `json:"qos"`
	Retained    bool   `json:"retained"`
	Compression string `json:"compression"`
	ResendTopic string `json:"resendDestination"`
}

type MQTTSink struct {
	adconf     *AdConf
	config     map[string]interface{}
	cli        api.MessageClient
	compressor message.Compressor
	sendParams map[string]any
}

func (ms *MQTTSink) hasKeys(str []string, ps map[string]interface{}) bool {
	for _, v := range str {
		if _, ok := ps[v]; ok {
			return true
		}
	}
	return false
}

func validateMQTTSinkTopic(topic string) error {
	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return fmt.Errorf("mqtt sink topic shouldn't contain # or +")
	}
	return nil
}

func (ms *MQTTSink) Ping(_ string, props map[string]interface{}) error {
	if err := ms.Configure(props); err != nil {
		return err
	}
	cli, err := clients.GetClient("mqtt", ms.config)
	if err != nil {
		return err
	}
	defer func() {
		clients.ReleaseClient(context.Background(), cli)
	}()
	return cli.Ping()
}

func (ms *MQTTSink) Configure(ps map[string]interface{}) error {
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
	if adconf.Compression != "" {
		ms.compressor, err = compressor.GetCompressor(adconf.Compression)
		if err != nil {
			return fmt.Errorf("invalid compression method %s", adconf.Compression)
		}
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
	return ms.collectWithTopic(ctx, item, ms.adconf.Tpc)
}

func (ms *MQTTSink) CollectResend(ctx api.StreamContext, item interface{}) error {
	return ms.collectWithTopic(ctx, item, ms.adconf.ResendTopic)
}

func (ms *MQTTSink) collectWithTopic(ctx api.StreamContext, item interface{}, topic string) error {
	logger := ctx.GetLogger()
	jsonBytes, _, err := ctx.TransformOutput(item)
	if err != nil {
		return err
	}
	logger.Debugf("%s publish %s", ctx.GetOpId(), jsonBytes)
	if ms.compressor != nil {
		jsonBytes, err = ms.compressor.Compress(jsonBytes)
		if err != nil {
			return err
		}
	}

	tpc, err := ctx.ParseTemplate(topic, item)
	if err != nil {
		return err
	}

	if err := ms.cli.Publish(ctx, tpc, jsonBytes, ms.sendParams); err != nil {
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
