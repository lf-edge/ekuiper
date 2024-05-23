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
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
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

const (
	dataSourceProp = "datasource"
	topicProp      = "topic"
	qosProp        = "qos"
	retainedProp   = "retained"
)

func (conn *Connection) Publish(payload any, props map[string]any) error {
	if !conn.connected.Load() {
		return errorx.NewIOErr("mqtt client is not connected")
	}
	topic, err := getTopicFromProps(props)
	if err != nil {
		return err
	}
	qos := getQosFromProps(props)
	retained := getRetained(props)
	token := conn.Client.Publish(topic, qos, retained, payload)
	return handleToken(token)
}

func (conn *Connection) onConnect(_ pahoMqtt.Client) {
	conn.connected.Store(true)
	conn.logger.Infof("The connection to mqtt broker is established")
	for topic, info := range conn.subscriptions {
		err := conn.subscribe(topic, info)
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
func (conn *Connection) Attach() {
	conn.refCount.Add(1)
}

func (conn *Connection) Ref() int {
	return int(conn.refCount.Load())
}

// Do not call this directly. Call connection pool Detach method to release the connection
func (conn *Connection) DetachSub(props map[string]any) {
	topic, err := getTopicFromProps(props)
	if err != nil {
		return
	}
	delete(conn.subscriptions, topic)
	conn.Client.Unsubscribe(topic)
}

func (conn *Connection) DetachPub(props map[string]any) {
}

func (conn *Connection) Subscribe(ctx api.StreamContext, props map[string]any, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	qos := getQosFromProps(props)
	topic, err := getTopicFromProps(props)
	if err != nil {
		return err
	}
	info := &SubscriptionInfo{
		Qos: qos,
		Handler: func(client pahoMqtt.Client, message pahoMqtt.Message) {
			conn.onMessage(ctx, message, ingest)
		},
		ErrHandler: func(err error) {
			ingestError(ctx, err)
		},
	}
	conn.subscriptions[topic] = info
	token := conn.Client.Subscribe(topic, info.Qos, info.Handler)
	return handleToken(token)
}

func (conn *Connection) subscribe(topic string, info *SubscriptionInfo) error {
	conn.subscriptions[topic] = info
	token := conn.Client.Subscribe(topic, info.Qos, info.Handler)
	return handleToken(token)
}

func (conn *Connection) onMessage(ctx api.StreamContext, msg pahoMqtt.Message, ingest api.BytesIngest) {
	if msg != nil {
		ctx.GetLogger().Debugf("Received message %s from topic %s", string(msg.Payload()), msg.Topic())
	}
	rcvTime := timex.GetNow()
	ingest(ctx, msg.Payload(), map[string]interface{}{
		"topic":     msg.Topic(),
		"qos":       msg.Qos(),
		"messageId": msg.MessageID(),
	}, rcvTime)
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
	c, err := ValidateConfig(props)
	if err != nil {
		return nil, err
	}
	opts := pahoMqtt.NewClientOptions().AddBroker(c.Server).SetProtocolVersion(c.pversion).SetAutoReconnect(true).SetMaxReconnectInterval(time.Minute)

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
	if len(selId) > 0 {
		con.Attach()
	}
	return con, nil
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

func (conn *Connection) GetClientId() string {
	return conn.selId
}

func CreateAnonymousConnection(ctx api.StreamContext, props map[string]any) (*Connection, error) {
	cli, err := CreateClient(ctx, "", props)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func getTopicFromProps(props map[string]any) (string, error) {
	v, ok := props[topicProp]
	if ok {
		return v.(string), nil
	}
	v, ok = props[dataSourceProp]
	if ok {
		return v.(string), nil
	}
	return "", fmt.Errorf("topic or datasource not defined")
}

func getQosFromProps(props map[string]any) byte {
	qos := byte(0)
	v, ok := props[qosProp]
	if ok {
		switch x := v.(type) {
		case int:
			qos = byte(x)
		case int64:
			qos = byte(int(x))
		case float64:
			qos = byte(int(x))
		default:
			return qos
		}
	}
	return qos
}

func getRetained(props map[string]any) bool {
	retained := false
	v, ok := props[retainedProp]
	if ok {
		v2, ok2 := v.(bool)
		if ok2 {
			retained = v2
		}
	}
	return retained
}
