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

package mqtt

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type MQTTConnectionConfig struct {
	Server   string `json:"server"`
	PVersion string `json:"protocolVersion"`
	ClientId string `json:"clientid"`
	Uname    string `json:"username"`
	Password string `json:"password"`
}

type MQTTClient struct {
	srv      string
	clientid string
	pVersion uint
	uName    string
	password string
	tls      *tls.Config

	conn MQTT.Client
}

func (ms *MQTTClient) CfgValidate(props map[string]interface{}) error {
	cfg := MQTTConnectionConfig{}

	err := cast.MapToStruct(props, &cfg)
	if err != nil {
		return fmt.Errorf("failed to get config, the error is %s", err)
	}

	if cfg.Server != "" {
		ms.srv = cfg.Server
	} else {
		return fmt.Errorf("missing server property")
	}

	if cfg.ClientId == "" {
		if newUUID, err := uuid.NewUUID(); err != nil {
			return fmt.Errorf("failed to get uuid, the error is %s", err)
		} else {
			ms.clientid = newUUID.String()
		}
	} else {
		ms.clientid = cfg.ClientId
	}
	// Default to MQTT 3.1.1 or NanoMQ cannot connect
	ms.pVersion = 4
	if cfg.PVersion == "3.1" {
		ms.pVersion = 3
	}
	tlsConfig, err := cert.GenTLSConfig(props, "mqtt")
	if err != nil {
		return err
	}
	conf.Log.Infof("Connect MQTT broker %s with TLS configs", ms.srv)
	ms.tls = tlsConfig
	if err := ms.checkMQTTServer(); err != nil {
		return err
	}
	ms.uName = cfg.Uname
	ms.password = strings.Trim(cfg.Password, " ")
	return nil
}

func (ms *MQTTClient) checkMQTTServer() error {
	if ms.tls != nil && !ms.tls.InsecureSkipVerify {
		if !strings.HasPrefix(strings.ToLower(ms.srv), "ssl://") {
			return fmt.Errorf("mqtt server should start with ssl:// when tls enabled")
		}
	}
	return nil
}

func (ms *MQTTClient) Connect(connHandler MQTT.OnConnectHandler, lostHandler MQTT.ConnectionLostHandler) error {
	if conf.Config.Basic.Debug {
		MQTT.DEBUG = conf.Log
		MQTT.ERROR = conf.Log
	}
	opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetProtocolVersion(4)

	opts = opts.SetTLSConfig(ms.tls)

	if ms.uName != "" {
		opts = opts.SetUsername(ms.uName)
	}
	if ms.password != "" {
		opts = opts.SetPassword(ms.password)
	}
	opts = opts.SetClientID(ms.clientid)
	opts = opts.SetAutoReconnect(true)
	opts.OnConnect = connHandler
	opts.OnConnectionLost = lostHandler
	opts.OnReconnecting = func(MQTT.Client, *MQTT.ClientOptions) {
		conf.Log.Infof("Reconnecting to mqtt broker %s client id %s", ms.srv, ms.clientid)
	}

	c := MQTT.NewClient(opts)
	token := c.Connect()
	err := handleToken(token)
	if err != nil {
		conf.Log.Errorf("The connection to mqtt broker %s failed: %s", ms.srv, err)
		return fmt.Errorf("found error when connecting for %s: %s", ms.srv, err)
	}
	conf.Log.Infof("The connection to mqtt broker is established successfully for %s.", ms.srv)
	ms.conn = c
	return nil
}

func (ms *MQTTClient) Subscribe(topic string, qos byte, handler MQTT.MessageHandler) error {
	token := ms.conn.Subscribe(topic, qos, handler)
	err := handleToken(token)
	if err != nil {
		return fmt.Errorf("found error when subscribing to %s of topic %s: %s", ms.srv, topic, err)
	}
	return nil
}

func (ms *MQTTClient) Publish(topic string, qos byte, retained bool, message []byte) error {
	token := ms.conn.Publish(topic, qos, retained, message)
	err := handleToken(token)
	if err != nil {
		return fmt.Errorf("found error when publishing to %s of topic %s: %s", ms.srv, topic, err)
	}
	return nil
}

func handleToken(token MQTT.Token) error {
	if !token.WaitTimeout(5 * time.Second) {
		return errorx.NewIOErr("timeout")
	} else if token.Error() != nil {
		return errorx.NewIOErr(token.Error().Error())
	}
	return nil
}

func (ms *MQTTClient) Disconnect() error {
	conf.Log.Infof("Closing the connection to mqtt broker for %s", ms.srv)
	if ms.conn != nil && ms.conn.IsConnected() {
		ms.conn.Disconnect(5000)
	}
	return nil
}
