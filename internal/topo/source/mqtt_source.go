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

package source

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients/mqtt"
	defaultCtx "github.com/lf-edge/ekuiper/internal/topo/context"
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
	buflen int

	config map[string]interface{}
	model  modelVersion
	schema map[string]interface{}

	cli api.MessageClient
}

type MQTTConfig struct {
	Format            string `json:"format"`
	Qos               int    `json:"qos"`
	BufferLen         int    `json:"bufferLength"`
	KubeedgeModelFile string `json:"kubeedgeModelFile"`
	KubeedgeVersion   string `json:"kubeedgeVersion"`
}

func (ms *MQTTSource) WithSchema(_ string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) Configure(topic string, props map[string]interface{}) error {
	cfg := &MQTTConfig{
		BufferLen: 1024,
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.BufferLen <= 0 {
		cfg.BufferLen = 1024
	}
	ms.buflen = cfg.BufferLen
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
	log := ctx.GetLogger()

	cli, err := ctx.GetClient("mqtt", ms.config)
	if err != nil {
		errCh <- err
		log.Errorf("found error when get mqtt client config %v, error %s", ms.config, err.Error())
		return
	}
	ms.cli = cli
	err = subscribe(ms, ctx, consumer)
	if err != nil {
		errCh <- err
	}
}

func subscribe(ms *MQTTSource, ctx api.StreamContext, consumer chan<- api.SourceTuple) error {
	log := ctx.GetLogger()

	messages := make(chan *api.MessageEnvelope, ms.buflen)
	topics := []api.TopicChannel{{Topic: ms.tpc, Messages: messages}}
	err := make(chan error, len(topics))
	req := &mqtt.RequestInfo{
		Qos: byte(ms.qos),
	}
	c := mqtt.WithRequestInfo(ctx.(*defaultCtx.DefaultContext), req)

	if e := ms.cli.Subscribe(c, topics, err); e != nil {
		log.Errorf("Failed to subscribe to mqtt topic %s, error %s\n", ms.tpc, e.Error())
		return e
	} else {
		log.Infof("Successfully subscribed to topic %s.", ms.tpc)
		for {
			select {
			case <-ctx.Done():
				log.Infof("Exit subscription to edgex messagebus topic %s.", ms.tpc)
				return nil
			case e1 := <-err:
				log.Errorf("the subscription to mqtt topic %s have error %s.\n", ms.tpc, e1.Error())
				return e1
			case env, ok := <-messages:
				if !ok { // the source is closed
					log.Infof("Exit subscription to edgex messagebus topic %s.", ms.tpc)
					return nil
				}
				msg := env.MqttMsg

				result, e := message.Decode(msg.Payload(), ms.format)
				//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
				if e != nil {
					log.Errorf("Invalid data format, cannot decode %s to %s format with error %s", string(msg.Payload()), ms.format, e)
					return e
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
					return nil
				}
			}
		}
	}
}

func (ms *MQTTSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Mqtt Source instance %d Done", ctx.GetInstanceId())
	if ms.cli != nil {
		ms.cli.Release(ctx)
	}
	return nil
}
