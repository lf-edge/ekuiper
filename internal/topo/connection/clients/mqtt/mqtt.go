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
	Server             string `json:"server"`
	PVersion           string `json:"protocolVersion"`
	ClientId           string `json:"clientid"`
	Uname              string `json:"username"`
	Password           string `json:"password"`
	Certification      string `json:"certificationPath"`
	PrivateKPath       string `json:"privateKeyPath"`
	RootCaPath         string `json:"rootCaPath"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify"`
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

	tlsOpts := cert.TlsConfigurationOptions{
		SkipCertVerify: cfg.InsecureSkipVerify,
		CertFile:       cfg.Certification,
		KeyFile:        cfg.PrivateKPath,
		CaFile:         cfg.RootCaPath,
	}
	conf.Log.Infof("Connect MQTT broker %s with TLS configs: %v.", ms.srv, tlsOpts)
	tlscfg, err := cert.GenerateTLSForClient(tlsOpts)
	if err != nil {
		return err
	}
	ms.tls = tlscfg
	ms.uName = cfg.Uname
	ms.password = strings.Trim(cfg.Password, " ")

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
	// timeout
	if !token.WaitTimeout(5 * time.Second) {
		conf.Log.Errorf("The connection to mqtt broker %s failed: connection timeout", ms.srv)
		return fmt.Errorf("found error when connecting for %s: timeout", ms.srv)
	} else if token.Error() != nil {
		conf.Log.Errorf("The connection to mqtt broker %s failed : %s ", ms.srv, token.Error())
		return fmt.Errorf("found error when connecting for %s: %s", ms.srv, token.Error())
	}
	conf.Log.Infof("The connection to mqtt broker is established successfully for %s.", ms.srv)
	ms.conn = c
	return nil
}

func (ms *MQTTClient) Subscribe(topic string, qos byte, handler MQTT.MessageHandler) error {
	if token := ms.conn.Subscribe(topic, qos, handler); token.WaitTimeout(5*time.Second) && token.Error() != nil {
		return fmt.Errorf("%s: %s", errorx.IOErr, token.Error())
	}
	return nil
}

func (ms *MQTTClient) Publish(topic string, qos byte, retained bool, message []byte) error {
	if token := ms.conn.Publish(topic, qos, retained, message); token.WaitTimeout(5*time.Second) && token.Error() != nil {
		return fmt.Errorf("%s: %s", errorx.IOErr, token.Error())
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
