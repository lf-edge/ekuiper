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
	"crypto/tls"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type Connection struct {
	pahoMqtt.Client
	selId     string
	id        string
	logger    api.Logger
	connected atomic.Bool
	status    atomic.Value
	scHandler api.StatusChangeHandler
	conf      *ConnectionConfig
	// key is the topic. Each topic will have only one connector
	subscriptions map[string]*subscriptionInfo
}

type ConnectionConfig struct {
	Server   string `json:"server"`
	PVersion string `json:"protocolVersion"`
	ClientId string `json:"clientid"`
	Uname    string `json:"username"`
	Password string `json:"password"`
	pversion uint   // 3 or 4
	tls      *tls.Config
}

type subscriptionInfo struct {
	Qos     byte
	Handler pahoMqtt.MessageHandler
}

func CreateConnection(_ api.StreamContext) modules.Connection {
	return &Connection{
		subscriptions: make(map[string]*subscriptionInfo),
	}
}

func (conn *Connection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	c, err := ValidateConfig(props)
	if err != nil {
		return err
	}
	opts := pahoMqtt.NewClientOptions().AddBroker(c.Server).SetProtocolVersion(c.pversion).SetAutoReconnect(true).SetMaxReconnectInterval(connection.DefaultMaxInterval)

	opts = opts.SetTLSConfig(c.tls)

	if c.Uname != "" {
		opts = opts.SetUsername(c.Uname)
	}
	if c.Password != "" {
		opts = opts.SetPassword(c.Password)
	}

	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionConnecting})
	opts.OnConnect = conn.onConnect
	opts.OnConnectionLost = conn.onConnectLost
	opts.OnReconnecting = conn.onReconnecting

	cli := pahoMqtt.NewClient(opts)
	conn.logger = ctx.GetLogger()
	conn.selId = c.ClientId
	conn.Client = cli
	conn.conf = c
	conn.id = conId
	return nil
}

func (conn *Connection) GetId(ctx api.StreamContext) string {
	return conn.id
}

func (conn *Connection) Dial(ctx api.StreamContext) error {
	token := conn.Client.Connect()
	err := handleToken(token)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("found error when connecting for %s: %s", conn.conf.Server, err))
	}
	// store connected status immediately to avoid publish error due to onConnect is called slower
	conn.connected.Store(true)
	ctx.GetLogger().Infof("new mqtt client created")
	return nil
}

func (conn *Connection) Status(_ api.StreamContext) modules.ConnectionStatus {
	return conn.status.Load().(modules.ConnectionStatus)
}

func (conn *Connection) SetStatusChangeHandler(ctx api.StreamContext, sch api.StatusChangeHandler) {
	conn.scHandler = sch
}

func (conn *Connection) onConnect(_ pahoMqtt.Client) {
	conn.connected.Store(true)
	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionConnected})
	if conn.scHandler != nil {
		conn.scHandler(api.ConnectionConnected, "")
	}
	conn.logger.Infof("The connection to mqtt broker is established")
	for topic, info := range conn.subscriptions {
		err := conn.Subscribe(topic, info.Qos, info.Handler)
		if err != nil { // should never happen. If happens because of connection, it will retry later
			conn.logger.Errorf("Failed to subscribe topic %s: %v", topic, err)
		}
	}
}

func (conn *Connection) onConnectLost(_ pahoMqtt.Client, err error) {
	conn.connected.Store(false)
	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionDisconnected, ErrMsg: err.Error()})
	if conn.scHandler != nil {
		conn.scHandler(api.ConnectionDisconnected, err.Error())
	}
	conn.logger.Infof("%v", err)
}

func (conn *Connection) onReconnecting(_ pahoMqtt.Client, _ *pahoMqtt.ClientOptions) {
	conn.status.Store(modules.ConnectionStatus{Status: api.ConnectionConnecting})
	if conn.scHandler != nil {
		conn.scHandler(api.ConnectionConnecting, "")
	}
	conn.logger.Debugf("Reconnecting to mqtt broker")
}

func (conn *Connection) DetachSub(ctx api.StreamContext, props map[string]any) {
	topic, err := getTopicFromProps(props)
	if err != nil {
		return
	}
	delete(conn.subscriptions, topic)
	conn.Client.Unsubscribe(topic)
}

func (conn *Connection) Close(ctx api.StreamContext) error {
	if conn == nil || conn.Client == nil {
		return nil
	}
	conn.Client.Disconnect(1)
	return nil
}

func (conn *Connection) Ping(ctx api.StreamContext) error {
	if conn.connected.Load() {
		return nil
	}
	return errors.New("failed to connect to broker")
}

// MQTT features

func (conn *Connection) Publish(topic string, qos byte, retained bool, payload any) error {
	// Need to return error immediately so that we can enable cache immediately
	if conn == nil || !conn.connected.Load() {
		return errorx.NewIOErr("mqtt client is not connected")
	}
	token := conn.Client.Publish(topic, qos, retained, payload)
	return handleToken(token)
}

func (conn *Connection) Subscribe(topic string, qos byte, callback pahoMqtt.MessageHandler) error {
	conn.subscriptions[topic] = &subscriptionInfo{
		Qos:     qos,
		Handler: callback,
	}
	token := conn.Client.Subscribe(topic, qos, callback)
	return handleToken(token)
}

func handleToken(token pahoMqtt.Token) error {
	if !token.WaitTimeout(5 * time.Second) {
		return errorx.NewIOErr("timeout")
	} else if token.Error() != nil {
		return errorx.NewIOErr(token.Error().Error())
	}
	return nil
}

func ValidateConfig(props map[string]any) (*ConnectionConfig, error) {
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
	default:
		return nil, fmt.Errorf("unsupported protocol version %s", c.PVersion)
	}
	tlsConfig, err := cert.GenTLSConfig(props, "mqtt")
	if err != nil {
		return nil, err
	}
	c.tls = tlsConfig
	return c, nil
}

const (
	dataSourceProp = "datasource"
)

func getTopicFromProps(props map[string]any) (string, error) {
	v, ok := props[dataSourceProp]
	if ok {
		return v.(string), nil
	}
	return "", fmt.Errorf("topic or datasource not defined")
}

var _ modules.StatefulDialer = &Connection{}
