// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

// Client is the interface for mqtt client. There are two implementations v4 and v5
type Client interface {
	Connect(ctx api.StreamContext) error
	Subscribe(ctx api.StreamContext, topic string, qos byte, callback MessageHandler) error
	Unsubscribe(ctx api.StreamContext, topic string) error
	Disconnect(ctx api.StreamContext)
	Publish(ctx api.StreamContext, topic string, qos byte, retained bool, payload []byte, properties map[string]string) error
	ParseMsg(ctx api.StreamContext, msg any) ([]byte, map[string]any, map[string]string)
}

type SubscriptionInfo struct {
	Qos     byte
	Handler MessageHandler
}

type CommonConfig struct {
	Server   string `json:"server"`
	PVersion string `json:"protocolVersion"`
}

type (
	ConnectHandler      func(ctx api.StreamContext)
	ConnectErrorHandler func(ctx api.StreamContext, e error)
	MessageHandler      func(ctx api.StreamContext, msg any)
)
