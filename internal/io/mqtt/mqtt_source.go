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

package mqtt

import (
	"fmt"
	"path"
	"strconv"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type MQTTSource struct {
	qos    int
	format string
	tpc    string
	buflen int

	config map[string]interface{}
	model  modelVersion
	schema map[string]interface{}

	cli          api.MessageClient
	decompressor message.Decompressor
}

type MQTTConfig struct {
	Format            string `json:"format"`
	Qos               int    `json:"qos"`
	BufferLen         int    `json:"bufferLength"`
	KubeedgeModelFile string `json:"kubeedgeModelFile"`
	KubeedgeVersion   string `json:"kubeedgeVersion"`
	Decompression     string `json:"decompression"`
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

	if cfg.Decompression != "" {
		dc, err := compressor.GetDecompressor(cfg.Decompression)
		if err != nil {
			return fmt.Errorf("get decompressor %s fail with error: %v", cfg.Decompression, err)
		}
		ms.decompressor = dc
	}

	if 0 != len(cfg.KubeedgeModelFile) {
		p := path.Join("sources", cfg.KubeedgeModelFile)
		ms.model = modelFactory(cfg.KubeedgeVersion)
		err = conf.LoadConfigFromPath(p, ms.model)
		if err != nil {
			return err
		}
	}

	cli, err := clients.GetClient("mqtt", ms.config)
	if err != nil {
		return err
	}
	ms.cli = cli

	return nil
}

func (ms *MQTTSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	err := subscribe(ms, ctx, consumer)
	if err != nil {
		errCh <- err
	}
}

// should only return fatal error
func subscribe(ms *MQTTSource, ctx api.StreamContext, consumer chan<- api.SourceTuple) error {
	log := ctx.GetLogger()

	messages := make(chan interface{}, ms.buflen)
	topics := []api.TopicChannel{{Topic: ms.tpc, Messages: messages}}
	err := make(chan error, len(topics))

	para := map[string]interface{}{
		"qos": byte(ms.qos),
	}
	if e := ms.cli.Subscribe(ctx, topics, err, para); e != nil {
		log.Errorf("Failed to subscribe to mqtt topic %s, error %s\n", ms.tpc, e.Error())
		return e
	} else {
		log.Infof("Successfully subscribed to topic %s.", ms.tpc)
		var tuples []api.SourceTuple
		for {
			select {
			case <-ctx.Done():
				log.Infof("Exit subscription to mqtt messagebus topic %s.", ms.tpc)
				return nil
			case e1 := <-err:
				tuples = []api.SourceTuple{
					&xsql.ErrorSourceTuple{
						Error: fmt.Errorf("the subscription to mqtt topic %s have error %s.\n", ms.tpc, e1.Error()),
					},
				}
			case env, ok := <-messages:
				if !ok { // the source is closed
					log.Infof("Exit subscription to mqtt messagebus topic %s.", ms.tpc)
					return nil
				}
				tuples = getTuples(ctx, ms, env)
			}
			for _, t := range tuples {
				select {
				case consumer <- t:
					log.Debugf("send data to source node")
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

func getTuples(ctx api.StreamContext, ms *MQTTSource, env interface{}) []api.SourceTuple {
	rcvTime := conf.GetNow()
	msg, ok := env.(pahoMqtt.Message)
	if !ok { // should never happen
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("can not convert interface data to mqtt message %v.", env),
			},
		}
	}
	payload := msg.Payload()
	var err error
	if ms.decompressor != nil {
		payload, err = ms.decompressor.Decompress(payload)
		if err != nil {
			return []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: fmt.Errorf("can not decompress mqtt message %v.", err),
				},
			}
		}
	}
	results, e := ctx.DecodeIntoList(payload)
	//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
	if e != nil {
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("Invalid data format, cannot decode %s with error %s", string(msg.Payload()), e),
			},
		}
	}
	meta := make(map[string]interface{})
	meta["topic"] = msg.Topic()
	meta["messageid"] = strconv.Itoa(int(msg.MessageID()))

	tuples := make([]api.SourceTuple, 0, len(results))
	for _, result := range results {
		if nil != ms.model {
			sliErr := ms.model.checkType(result, msg.Topic())
			for _, v := range sliErr {
				ctx.GetLogger().Errorf(v)
			}
		}
		tuples = append(tuples, api.NewDefaultSourceTupleWithTime(result, meta, rcvTime))
	}
	return tuples
}

func (ms *MQTTSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Mqtt Source instance %d Done", ctx.GetInstanceId())
	if ms.cli != nil {
		clients.ReleaseClient(ctx, ms.cli)
	}
	return nil
}
