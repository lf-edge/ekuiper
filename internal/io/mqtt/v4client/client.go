// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package v4client

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type Client struct {
	cli                 pahoMqtt.Client
	EnableClientSession bool
}

type ConnectionConfig struct {
	Server              string `json:"server"`
	PVersion            string `json:"protocolVersion"`
	ClientId            string `json:"clientid"`
	Uname               string `json:"username"`
	Password            string `json:"password"`
	EnableClientSession bool   `json:"enableClientSession"`
	pversion            uint   // 3 or 4
	tls                 *tls.Config
}

func Provision(ctx api.StreamContext, props map[string]any, onConnect client.ConnectHandler, onConnectLost client.ConnectErrorHandler, onReconnect client.ConnectHandler) (*Client, error) {
	c, err := ValidateConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	opts := pahoMqtt.NewClientOptions().AddBroker(c.Server).SetProtocolVersion(c.pversion).SetAutoReconnect(true).SetMaxReconnectInterval(connection.DefaultMaxInterval).SetClientID(c.ClientId).SetTLSConfig(c.tls)

	if c.EnableClientSession {
		c.EnableClientSession = true
		opts.SetCleanSession(false)
	}

	if c.Uname != "" {
		opts = opts.SetUsername(c.Uname)
	}
	if c.Password != "" {
		opts = opts.SetPassword(c.Password)
	}

	opts.OnConnect = func(_ pahoMqtt.Client) {
		onConnect(ctx)
	}
	opts.OnConnectionLost = func(_ pahoMqtt.Client, err error) {
		onConnectLost(ctx, err)
	}
	opts.OnReconnecting = func(_ pahoMqtt.Client, _ *pahoMqtt.ClientOptions) {
		onReconnect(ctx)
	}

	cli := pahoMqtt.NewClient(opts)
	return &Client{cli: cli, EnableClientSession: c.EnableClientSession}, nil
}

func (c *Client) Connect(_ api.StreamContext) error {
	token := c.cli.Connect()
	return handleToken(token)
}

func (c *Client) ParseMsg(ctx api.StreamContext, p any) ([]byte, map[string]any, map[string]string) {
	if msg, ok := p.(pahoMqtt.Message); ok {
		meta := map[string]any{
			"topic":     msg.Topic(),
			"qos":       msg.Qos(),
			"messageId": msg.MessageID(),
		}
		return msg.Payload(), meta, nil
	} else {
		ctx.GetLogger().Errorf("receive invalid msg %v", p)
	}
	return nil, nil, nil
}

func (c *Client) Publish(_ api.StreamContext, topic string, qos byte, retained bool, payload []byte, _ map[string]string) error {
	token := c.cli.Publish(topic, qos, retained, payload)
	return handleToken(token)
}

func (c *Client) Subscribe(ctx api.StreamContext, topic string, qos byte, callback client.MessageHandler) error {
	topics := strings.Split(topic, ",")
	if len(topics) < 2 {
		token := c.cli.Subscribe(topic, qos, func(_ pahoMqtt.Client, message pahoMqtt.Message) {
			callback(ctx, message)
		})
		return handleToken(token)
	}
	filters := make(map[string]byte)
	for _, subTopic := range topics {
		filters[subTopic] = qos
	}
	token := c.cli.SubscribeMultiple(filters, func(c pahoMqtt.Client, message pahoMqtt.Message) {
		callback(ctx, message)
	})
	return handleToken(token)
}

func (c *Client) Unsubscribe(_ api.StreamContext, topic string) error {
	if c.EnableClientSession {
		return nil
	}
	for _, subTopic := range strings.Split(topic, ",") {
		token := c.cli.Unsubscribe(subTopic)
		if err := handleToken(token); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Disconnect(ctx api.StreamContext) {
	c.cli.Disconnect(1000)
}

func ValidateConfig(ctx api.StreamContext, props map[string]any) (*ConnectionConfig, error) {
	c := &ConnectionConfig{PVersion: "3.1.1"}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}

	if c.Server == "" {
		return nil, fmt.Errorf("missing server property")
	}

	if c.ClientId == "" {
		c.ClientId = uuid.New().String()
	}
	// Default to MQTT 3.1.1 or NanoMQ cannot connect
	switch c.PVersion {
	case "3.1":
		c.pversion = 3
	case "3.1.1", "4":
		c.pversion = 4
	}
	tlsConfig, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	c.tls = tlsConfig
	return c, nil
}

func handleToken(token pahoMqtt.Token) error {
	if !token.WaitTimeout(5 * time.Second) {
		return errorx.NewIOErr("timeout")
	} else if token.Error() != nil {
		return errorx.NewIOErr(token.Error().Error())
	}
	return nil
}

var _ client.Client = &Client{}
