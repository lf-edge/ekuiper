// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

//go:build edgex
// +build edgex

package edgex

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/v3/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v3/pkg/types"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"strings"
)

type EdgexClient struct {
	mbconf types.MessageBusConfig
	client messaging.MessageClient
}

type EdgexConf struct {
	Protocol string            `json:"protocol"`
	Server   string            `json:"server"`
	Host     string            `json:"host"`
	Port     int               `json:"port"`
	Type     string            `json:"type"`
	Optional map[string]string `json:"optional"`
}

// Modify the copied conf to print no password.
func printConf(mbconf types.MessageBusConfig) {
	var printableOptional = make(map[string]string)
	for k, v := range mbconf.Optional {
		if strings.EqualFold(k, "password") {
			printableOptional[k] = "*"
		} else {
			printableOptional[k] = v
		}
	}
	mbconf.Optional = printableOptional
	conf.Log.Infof("Use configuration for edgex messagebus %v", mbconf)
}

func (es *EdgexClient) CfgValidate(props map[string]interface{}) error {
	edgeAddr := "localhost"
	c := &EdgexConf{
		Protocol: "redis",
		Port:     6379,
		Type:     messaging.Redis,
		Optional: nil,
	}

	if o, ok := props["optional"]; ok {
		switch ot := o.(type) {
		case map[string]string:
			c.Optional = ot
		case map[string]interface{}:
			c.Optional = make(map[string]string)
			for k, v := range ot {
				c.Optional[k] = fmt.Sprintf("%v", v)
			}
		default:
			return fmt.Errorf("invalid optional config %v, must be a map", o)
		}
		delete(props, "optional")
	}

	err := cast.MapToStruct(props, c)
	if err != nil {
		return fmt.Errorf("map config map to struct fail with error: %v", err)
	}

	if c.Host != "" {
		edgeAddr = c.Host
	} else if c.Server != "" {
		edgeAddr = c.Server
	}

	if c.Type != messaging.MQTT && c.Type != messaging.Redis &&
		c.Type != messaging.NatsCore && c.Type != messaging.NatsJetStream {
		return fmt.Errorf("specified wrong type value %s", c.Type)
	}
	if c.Port < 0 {
		return fmt.Errorf("specified wrong port value, expect positive integer but got %d", c.Port)
	}

	mbconf := types.MessageBusConfig{
		Broker: types.HostInfo{
			Host:     edgeAddr,
			Port:     c.Port,
			Protocol: c.Protocol,
		},
		Type: c.Type}
	mbconf.Optional = c.Optional
	es.mbconf = mbconf

	printConf(mbconf)

	return nil
}

func (es *EdgexClient) Connect() error {
	client, err := messaging.NewMessageClient(es.mbconf)
	if err != nil {
		return err
	}

	if err := client.Connect(); err != nil {
		conf.Log.Errorf("The connection to edgex messagebus failed.")
		return fmt.Errorf("Failed to connect to edgex message bus: " + err.Error())
	}
	es.client = client
	return nil
}

func (es *EdgexClient) Publish(env types.MessageEnvelope, topic string) error {
	if err := es.client.Publish(env, topic); err != nil {
		conf.Log.Errorf("Publish to topic %s has error : %s.", topic, err.Error())
		return fmt.Errorf("Failed to publish to edgex message bus: " + err.Error())
	}
	return nil
}

func (es *EdgexClient) Subscribe(msg chan types.MessageEnvelope, topic string, err chan error) error {
	topics := []types.TopicChannel{{Topic: topic, Messages: msg}}
	if err := es.client.Subscribe(topics, err); err != nil {
		conf.Log.Errorf("Failed to subscribe to edgex messagebus with topic %s has error : %s.", topic, err.Error())
		return err
	}

	return nil
}

func (es *EdgexClient) GetClient() (interface{}, error) {

	client, err := messaging.NewMessageClient(es.mbconf)
	if err != nil {
		return nil, err
	}

	if err := client.Connect(); err != nil {
		conf.Log.Errorf("The connection to edgex messagebus failed.")
		return nil, fmt.Errorf("Failed to connect to edgex message bus: " + err.Error())
	}
	conf.Log.Infof("The connection to edgex messagebus is established successfully.")

	es.client = client
	return client, nil
}

func (es *EdgexClient) Disconnect() error {
	conf.Log.Infof("Closing the connection to edgex messagebus.")
	if e := es.client.Disconnect(); e != nil {
		return e
	}
	return nil
}
