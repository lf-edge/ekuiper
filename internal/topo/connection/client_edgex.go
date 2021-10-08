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

// +build edgex

package connection

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func init() {
	registerClientFactory("edgex", func(s *ConSelector) Client {
		return &EdgexClient{selector: s}
	})
}

type EdgexClient struct {
	selector *ConSelector
	mbconf   types.MessageBusConfig
	client   messaging.MessageClient
}

type EdgexConf struct {
	Protocol string            `json:"protocol"`
	Server   string            `json:"server"`
	Port     int               `json:"port"`
	Type     string            `json:"type"`
	Optional map[string]string `json:"optional"`
}

func (es *EdgexClient) CfgValidate(props map[string]interface{}) error {

	c := &EdgexConf{}
	err := cast.MapToStructStrict(props, c)
	if err != nil {
		return fmt.Errorf("read properties %v fail for connection selector %s with error: %v", props, es.selector.ConnSelectorCfg, err)
	}

	if c.Server == "" {
		return fmt.Errorf("missing server property for connection selector %s", es.selector.ConnSelectorCfg)
	}

	if c.Port == 0 {
		return fmt.Errorf("missing port property for connection selector %s", es.selector.ConnSelectorCfg)
	}

	if c.Type != messaging.ZeroMQ && c.Type != messaging.MQTT && c.Type != messaging.Redis {
		return fmt.Errorf("specified wrong type value %s for connection selector %s", c.Type, es.selector.ConnSelectorCfg)
	}

	mbconf := types.MessageBusConfig{
		SubscribeHost: types.HostInfo{
			Protocol: c.Protocol,
			Host:     c.Server,
			Port:     c.Port,
		},
		PublishHost: types.HostInfo{
			Host:     c.Server,
			Port:     c.Port,
			Protocol: c.Protocol,
		},
		Type: c.Type}
	mbconf.Optional = c.Optional
	es.mbconf = mbconf

	return nil
}

func (es *EdgexClient) GetClient() (interface{}, error) {

	client, err := messaging.NewMessageClient(es.mbconf)
	if err != nil {
		return nil, err
	}

	if err := client.Connect(); err != nil {
		conf.Log.Errorf("The connection to edgex messagebus failed for connection selector : %s.", es.selector.ConnSelectorCfg)
		return nil, fmt.Errorf("Failed to connect to edgex message bus: " + err.Error())
	}
	conf.Log.Infof("The connection to edgex messagebus is established successfully for connection selector : %s.", es.selector.ConnSelectorCfg)

	es.client = client
	return client, nil
}

func (es *EdgexClient) CloseClient() error {
	conf.Log.Infof("Closing the connection to edgex messagebus for connection selector : %s.", es.selector.ConnSelectorCfg)
	if e := es.client.Disconnect(); e != nil {
		return e
	}
	return nil
}
