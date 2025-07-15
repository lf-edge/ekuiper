// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

package client

import (
	"fmt"
	"strings"

	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type Client struct {
	mbconf types.MessageBusConfig
	client messaging.MessageClient
	id     string
}

var optKeys = map[string]string{
	"clientid": "ClientId", "username": "Username", "password": "Password", "qos": "Qos", "keepalive": "KeepAlive", "retained": "Retained", "connectionpayload": "ConnectionPayload", "certfile": "CertFile", "keyfile": "KeyFile", "certpemblock": "CertPEMBlock", "keypemblock": "KeyPEMBlock", "skipcertverify": "SkipCertVerify",
}

func GetConnection(_ api.StreamContext) modules.Connection {
	return &Client{}
}

func (es *Client) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	es.id = conId
	err := es.CfgValidate(props)
	if err != nil {
		return err
	}
	client, err := messaging.NewMessageClient(es.mbconf)
	if err != nil {
		return err
	}
	es.client = client
	return nil
}

func (es *Client) GetId(ctx api.StreamContext) string {
	return es.id
}

func (es *Client) Dial(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("connecting to edgex")
	if err := es.client.Connect(); err != nil {
		conf.Log.Errorf("The connection to edgex messagebus failed.")
		return fmt.Errorf("Failed to connect to edgex message bus: %v", err)
	}
	conf.Log.Infof("The connection to edgex messagebus is established successfully.")
	return nil
}

func (es *Client) Ping(_ api.StreamContext) error {
	if es.client != nil {
		return nil
	}
	return fmt.Errorf("client is nil")
}

func (es *Client) DetachSub(ctx api.StreamContext, props map[string]any) {
	topic, ok := props["topic"]
	ctx.GetLogger().Infof("detach edgex sub %v", topic)
	if ok && es.client != nil {
		err := es.client.Unsubscribe(topic.(string))
		if err != nil {
			ctx.GetLogger().Error(err)
		}
	}
}

func (es *Client) Close(ctx api.StreamContext) error {
	if es.client != nil {
		return es.client.Disconnect()
	}
	return nil
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
	printableOptional := make(map[string]string)
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

func (es *Client) CfgValidate(props map[string]interface{}) error {
	edgeAddr := "localhost"
	c := &EdgexConf{
		Protocol: "tcp",
		Port:     1883,
		Type:     messaging.MQTT,
		Optional: nil,
	}

	if o, ok := props["optional"]; ok {
		switch ot := o.(type) {
		case map[string]string:
			c.Optional = ot
		case map[string]interface{}:
			c.Optional = make(map[string]string)
			for k, v := range ot {
				if nk, ok := optKeys[k]; ok {
					k = nk
				}
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

	if c.Type != messaging.MQTT &&
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
		Type: c.Type,
	}
	mbconf.Optional = c.Optional
	es.mbconf = mbconf

	printConf(mbconf)

	return nil
}

func (es *Client) Publish(env types.MessageEnvelope, topic string) error {
	if err := es.client.Publish(env, topic); err != nil {
		conf.Log.Errorf("Publish to topic %s has error : %s.", topic, err.Error())
		return fmt.Errorf("Failed to publish to edgex message bus: %v", err)
	}
	return nil
}

func (es *Client) Subscribe(msg chan types.MessageEnvelope, topic string, err chan error) error {
	topics := []types.TopicChannel{{Topic: topic, Messages: msg}}
	if err := es.client.Subscribe(topics, err); err != nil {
		conf.Log.Errorf("Failed to subscribe to edgex messagebus with topic %s has error : %s.", topic, err.Error())
		return err
	}
	return nil
}

func (es *Client) Disconnect() error {
	conf.Log.Infof("Closing the connection to edgex messagebus.")
	if e := es.client.Disconnect(); e != nil {
		return e
	}
	return nil
}
