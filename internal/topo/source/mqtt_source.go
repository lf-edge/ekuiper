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

package source

import (
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"path"
	"strconv"
)

type MQTTSource struct {
	qos    int
	format string
	tpc    string

	config map[string]interface{}
	model  modelVersion
	schema map[string]interface{}
	conn   MQTT.Client
}

type MQTTConfig struct {
	Format            string `json:"format"`
	Qos               int    `json:"qos"`
	KubeedgeModelFile string `json:"kubeedgeModelFile"`
	KubeedgeVersion   string `json:"kubeedgeVersion"`
}

func (ms *MQTTSource) WithSchema(_ string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) Configure(topic string, props map[string]interface{}) error {
	cfg := &MQTTConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	ms.tpc = topic
	ms.format = cfg.Format
	ms.qos = cfg.Qos
	ms.config = props

	if 0 != len(cfg.KubeedgeModelFile) {
		p := path.Join("sources", cfg.KubeedgeModelFile)
		ms.model = modelFactory(cfg.KubeedgeVersion)
		err = conf.LoadConfigFromPath(p, ms.model)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MQTTSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	var client MQTT.Client
	log := ctx.GetLogger()

	con, err := ctx.GetConnection("mqtt", ms.config)
	if err != nil {
		log.Errorf("The mqtt client for connection %v get fail with error: %s", ms.config, err)
		errCh <- err
		return
	}
	client = con.(MQTT.Client)
	log.Infof("The mqtt client for connection  %v get successfully", ms.config)

	ms.conn = client
	subscribe(ms, client, ctx, consumer)
}

func subscribe(ms *MQTTSource, client MQTT.Client, ctx api.StreamContext, consumer chan<- api.SourceTuple) {
	log := ctx.GetLogger()
	h := func(client MQTT.Client, msg MQTT.Message) {
		log.Debugf("instance %d received %s", ctx.GetInstanceId(), msg.Payload())
		result, e := message.Decode(msg.Payload(), ms.format)
		//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
		if e != nil {
			log.Errorf("Invalid data format, cannot decode %s to %s format with error %s", string(msg.Payload()), ms.format, e)
			return
		}

		meta := make(map[string]interface{})
		meta["topic"] = msg.Topic()
		meta["messageid"] = strconv.Itoa(int(msg.MessageID()))

		if nil != ms.model {
			sliErr := ms.model.checkType(result, msg.Topic())
			for _, v := range sliErr {
				log.Errorf(v)
			}
		}

		select {
		case consumer <- api.NewDefaultSourceTuple(result, meta):
			log.Debugf("send data to source node")
		case <-ctx.Done():
			return
		}
	}

	if token := client.Subscribe(ms.tpc, byte(ms.qos), h); token.Wait() && token.Error() != nil {
		log.Errorf("Found error: %s", token.Error())
	} else {
		log.Infof("Successfully subscribe to topic %s", ms.tpc)
	}
}

func (ms *MQTTSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Mqtt Source instance %d Done", ctx.GetInstanceId())
	if ms.conn != nil && ms.conn.IsConnected() {
		ms.conn.Unsubscribe(ms.tpc)
	}
	ctx.ReleaseConnection(ms.config)
	return nil
}
