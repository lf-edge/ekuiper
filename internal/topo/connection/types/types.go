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

package types

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type ClientFactoryFunc func(props map[string]interface{}) (ClientWrapper, error)

type ClientWrapper interface {
	Subscribe(c api.StreamContext, subChan []TopicChannel, messageErrors chan error) error
	Release(c api.StreamContext)
	Publish(c api.StreamContext, topic string, message []byte) error
	SetConnectionSelector(conSelector string)
	AddRef()
}

type MessageClient interface {
	Subscribe(c api.StreamContext, subChan []TopicChannel, messageErrors chan error) error
	Release(c api.StreamContext)
	Publish(c api.StreamContext, topic string, message []byte) error
}

type MessageEnvelope struct {
	Payload []byte

	//mqtt
	MqttMsg MQTT.Message
	//edgex
	EdgexMsg types.MessageEnvelope
}

// TopicChannel is the data structure for subscriber
type TopicChannel struct {
	// Topic for subscriber to filter on if any
	Topic string
	// Messages is the returned message channel for the subscriber
	Messages chan<- *MessageEnvelope
}
