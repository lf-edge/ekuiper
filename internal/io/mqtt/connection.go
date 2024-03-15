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

package mqtt

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type Connection struct {
	pahoMqtt.Client
	selId     string
	logger    api.Logger
	connected atomic.Bool
	refCount  atomic.Int32
	// key is the topic. Each topic will have only one connector
	subscriptions map[string]*SubscriptionInfo
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

type SubscriptionInfo struct {
	Qos        byte
	Handler    pahoMqtt.MessageHandler
	ErrHandler func(error)
}

func (conn *Connection) onConnect(_ pahoMqtt.Client) {
	conn.connected.Store(true)
	conn.logger.Infof("The connection to mqtt broker is established")
	for topic, info := range conn.subscriptions {
		err := conn.Subscribe(topic, info)
		if err != nil { // should never happen, if happened, stop the rule
			panic(fmt.Sprintf("Failed to subscribe topic %s: %v", topic, err))
		}
	}
}

func (conn *Connection) onConnectLost(_ pahoMqtt.Client, err error) {
	conn.connected.Store(false)
	e := fmt.Errorf("The connection to mqtt broker is lost due to %v", err)
	for _, info := range conn.subscriptions {
		info.ErrHandler(e)
	}
	conn.logger.Infof("%v", err)
}

func (conn *Connection) onReconnecting(_ pahoMqtt.Client, _ *pahoMqtt.ClientOptions) {
	conn.logger.Infof("Reconnecting to mqtt broker")
}

// Do not call this directly. Call connection pool Attach method to get the connection
func (conn *Connection) attach() {
	conn.refCount.Add(1)
}

// Do not call this directly. Call connection pool Detach method to release the connection
func (conn *Connection) detach(topic string) bool {
	delete(conn.subscriptions, topic)
	conn.Client.Unsubscribe(topic)
	if conn.refCount.Add(-1) == 0 {
		go conn.Close()
		return true
	}
	return false
}

func (conn *Connection) Subscribe(topic string, info *SubscriptionInfo) error {
	conn.subscriptions[topic] = info
	token := conn.Client.Subscribe(topic, info.Qos, info.Handler)
	return handleToken(token)
}

func (conn *Connection) Close() {
	conn.Client.Disconnect(1)
}

func (conn *Connection) Ping() error {
	if conn.Client.IsConnected() {
		return nil
	} else {
		return errors.New("mqtt ping failed")
	}
}

// CreateClient creates a new mqtt client. It is anonymous and does not require a name.
func CreateClient(ctx api.StreamContext, selId string, props map[string]any) (*Connection, error) {
	if selId != "" {
		selectCfg := &conf.ConSelector{
			ConnSelectorStr: selId,
		}
		if err := selectCfg.Init(); err != nil {
			return nil, err
		}
		cf, err := selectCfg.ReadCfgFromYaml()
		if err != nil {
			return nil, err
		}
		props = cf
	}
	c, err := validateConfig(props)
	if err != nil {
		return nil, err
	}
	opts := pahoMqtt.NewClientOptions().AddBroker(c.Server).SetProtocolVersion(c.pversion)

	opts = opts.SetTLSConfig(c.tls)

	if c.Uname != "" {
		opts = opts.SetUsername(c.Uname)
	}
	if c.Password != "" {
		opts = opts.SetPassword(c.Password)
	}
	opts = opts.SetClientID(c.ClientId)
	opts = opts.SetAutoReconnect(true)

	con := &Connection{
		logger:        ctx.GetLogger(),
		selId:         c.ClientId,
		subscriptions: make(map[string]*SubscriptionInfo),
	}
	opts.OnConnect = con.onConnect
	opts.OnConnectionLost = con.onConnectLost
	opts.OnReconnecting = con.onReconnecting

	cli := pahoMqtt.NewClient(opts)
	token := cli.Connect()
	err = handleToken(token)
	if err != nil {
		return nil, fmt.Errorf("found error when connecting for %s: %s", c.Server, err)
	}
	ctx.GetLogger().Infof("new mqtt client created")
	con.Client = cli
	con.attach()
	return con, nil
}

func validateConfig(props map[string]any) (*ConnectionConfig, error) {
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

func handleToken(token pahoMqtt.Token) error {
	if !token.WaitTimeout(5 * time.Second) {
		return errorx.NewIOErr("timeout")
	} else if token.Error() != nil {
		return errorx.NewIOErr(token.Error().Error())
	}
	return nil
}

func (conn *Connection) GetClientId() string {
	return conn.selId
}
