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

package sink

import (
	"crypto/tls"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"strings"
)

type MQTTSink struct {
	srv      string
	tpc      string
	clientid string
	pVersion uint
	qos      byte
	uName    string
	password string
	certPath string
	pkeyPath string
	conSel   string

	insecureSkipVerify bool
	retained           bool

	conn MQTT.Client
}

func (ms *MQTTSink) hasKeys(str []string, ps map[string]interface{}) bool {
	for _, v := range str {
		if _, ok := ps[v]; ok {
			return true
		}
	}
	return false
}

func (ms *MQTTSink) Configure(ps map[string]interface{}) error {
	conSelector := ""
	if pk, ok := ps["connectionSelector"]; ok {
		if v, ok := pk.(string); ok {
			conSelector = v
		}
		keys := []string{"server", "clientId", "protocolVersion", "username", "password", "certificationPath", "privateKeyPath", "insecureSkipVerify"}
		if ms.hasKeys(keys, ps) {
			return fmt.Errorf("already have connection selector: %s, remove connection related config", conSelector)
		}
		ms.conSel = conSelector
	} else {

		srv := ""
		if pk, ok := ps["server"]; ok {
			if v, ok := pk.(string); ok {
				srv = v
			}
		} else {
			return fmt.Errorf("mqtt sink is missing property server")
		}

		clientid, ok := ps["clientId"]
		if !ok {
			if uuid, err := uuid.NewUUID(); err != nil {
				return fmt.Errorf("mqtt sink fails to get uuid, the error is %s", err)
			} else {

				clientid = uuid.String()
			}
		}
		var pVersion uint = 3
		pVersionStr, ok := ps["protocolVersion"]
		if ok {
			v, _ := pVersionStr.(string)
			if v == "3.1" {
				pVersion = 3
			} else if v == "3.1.1" {
				pVersion = 4
			} else {
				return fmt.Errorf("unknown protocol version %s, the value could be only 3.1 or 3.1.1 (also refers to MQTT version 4)", pVersionStr)
			}
		}

		uName := ""
		un, ok := ps["username"]
		if ok {
			v, _ := un.(string)
			if strings.Trim(v, " ") != "" {
				uName = v
			}
		}

		password := ""
		pwd, ok := ps["password"]
		if ok {
			v, _ := pwd.(string)
			if strings.Trim(v, " ") != "" {
				password = v
			}
		}

		certPath := ""
		if cp, ok := ps["certificationPath"]; ok {
			if v, ok := cp.(string); ok {
				certPath = v
			}
		}

		pKeyPath := ""
		if pk, ok := ps["privateKeyPath"]; ok {
			if v, ok := pk.(string); ok {
				pKeyPath = v
			}
		}

		insecureSkipVerify := false
		if pk, ok := ps["insecureSkipVerify"]; ok {
			if v, ok := pk.(bool); ok {
				insecureSkipVerify = v
			}
		}

		ms.srv = srv
		ms.clientid = clientid.(string)
		ms.pVersion = pVersion

		ms.uName = uName
		ms.password = password
		ms.certPath = certPath
		ms.pkeyPath = pKeyPath
		ms.insecureSkipVerify = insecureSkipVerify
	}

	tpc, ok := ps["topic"]
	if !ok {
		return fmt.Errorf("mqtt sink is missing property topic")
	}

	var qos byte = 0
	if qosRec, ok := ps["qos"]; ok {
		if v, err := cast.ToInt(qosRec, cast.STRICT); err == nil {
			qos = byte(v)
		}
		if qos != 0 && qos != 1 && qos != 2 {
			return fmt.Errorf("not valid qos value %v, the value could be only int 0 or 1 or 2", qos)
		}
	}

	retained := false
	if pk, ok := ps["retained"]; ok {
		if v, ok := pk.(bool); ok {
			retained = v
		}
	}

	ms.qos = qos
	ms.tpc = tpc.(string)
	ms.retained = retained

	return nil
}

func (ms *MQTTSink) Open(ctx api.StreamContext) error {
	var client MQTT.Client
	log := ctx.GetLogger()
	if ms.conSel != "" {
		con, err := ctx.GetConnection(ms.conSel)
		if err != nil {
			log.Errorf("The mqtt client for connection selector %s get fail with error: %s", ms.conSel, err)
			return err
		}
		client = con.(MQTT.Client)
		log.Infof("The mqtt client for connection selector %s get successfully", ms.conSel)
	} else {
		log.Infof("Opening mqtt sink for rule %s.", ctx.GetRuleId())
		opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetClientID(ms.clientid)

		if ms.certPath != "" || ms.pkeyPath != "" {
			log.Infof("Connect MQTT broker with certification and keys.")
			if cp, err := conf.ProcessPath(ms.certPath); err == nil {
				if kp, err1 := conf.ProcessPath(ms.pkeyPath); err1 == nil {
					if cer, err2 := tls.LoadX509KeyPair(cp, kp); err2 != nil {
						return err2
					} else {
						opts.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cer}, InsecureSkipVerify: ms.insecureSkipVerify})
					}
				} else {
					return err1
				}
			} else {
				return err
			}
		} else {
			log.Infof("Connect MQTT broker with username and password.")
			if ms.uName != "" {
				opts = opts.SetUsername(ms.uName)
			}

			if ms.password != "" {
				opts = opts.SetPassword(ms.password)
			}
		}

		opts.SetAutoReconnect(true)
		var reconn = false
		opts.SetConnectionLostHandler(func(client MQTT.Client, e error) {
			log.Errorf("The connection %s is disconnected due to error %s, will try to re-connect later.", ms.srv+": "+ms.clientid, e)
			ms.conn = client
			reconn = true
		})

		opts.SetOnConnectHandler(func(client MQTT.Client) {
			if reconn {
				log.Infof("The connection is %s re-established successfully.", ms.srv+": "+ms.clientid)
			}
		})

		client = MQTT.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return fmt.Errorf("Found error: %s", token.Error())
		}
		log.Infof("The connection to server %s:%d was established successfully", ms.srv, ms.clientid)
	}

	ms.conn = client
	return nil
}

func (ms *MQTTSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	jsonBytes, _, err := ctx.TransformOutput(item)
	if err != nil {
		return err
	}
	c := ms.conn
	logger.Debugf("%s publish %s", ctx.GetOpId(), jsonBytes)
	tpc, err := ctx.ParseDynamicProp(ms.tpc, item)
	if err != nil {
		return err
	}
	if tpc, ok := tpc.(string); !ok {
		return fmt.Errorf("the value %v of dynamic prop %s for topic is not a string", ms.tpc, tpc)
	}
	if token := c.Publish(tpc.(string), ms.qos, ms.retained, jsonBytes); token.Wait() && token.Error() != nil {
		return fmt.Errorf("%s: %s", errorx.IOErr, token.Error())
	}
	return nil
}

func (ms *MQTTSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing mqtt sink")
	if ms.conn != nil && ms.conn.IsConnected() && ms.conSel == "" {
		ms.conn.Disconnect(5000)
	}
	if ms.conSel != "" {
		ctx.ReleaseConnection(ms.conSel)
	}
	return nil
}
