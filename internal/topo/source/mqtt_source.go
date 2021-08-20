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

package source

import (
	"crypto/tls"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"path"
	"strconv"
	"strings"
)

type MQTTSource struct {
	srv      string
	format   string
	tpc      string
	clientid string
	pVersion uint
	uName    string
	password string
	certPath string
	pkeyPath string

	model  modelVersion
	schema map[string]interface{}
	conn   MQTT.Client
}

type MQTTConfig struct {
	Format            string   `json:"format"`
	Qos               int      `json:"qos"`
	Servers           []string `json:"servers"`
	Clientid          string   `json:"clientid"`
	PVersion          string   `json:"protocolVersion"`
	Uname             string   `json:"username"`
	Password          string   `json:"password"`
	Certification     string   `json:"certificationPath"`
	PrivateKPath      string   `json:"privateKeyPath"`
	KubeedgeModelFile string   `json:"kubeedgeModelFile"`
	KubeedgeVersion   string   `json:"kubeedgeVersion"`
}

func (ms *MQTTSource) WithSchema(schema string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) Configure(topic string, props map[string]interface{}) error {
	cfg := &MQTTConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	ms.tpc = topic
	if srvs := cfg.Servers; srvs != nil && len(srvs) > 0 {
		ms.srv = srvs[0]
	} else {
		return fmt.Errorf("missing server property")
	}

	ms.format = cfg.Format
	ms.clientid = cfg.Clientid

	ms.pVersion = 3
	if cfg.PVersion == "3.1.1" {
		ms.pVersion = 4
	}

	ms.uName = cfg.Uname
	ms.password = strings.Trim(cfg.Password, " ")
	ms.certPath = cfg.Certification
	ms.pkeyPath = cfg.PrivateKPath

	if 0 != len(cfg.KubeedgeModelFile) {
		p := path.Join("sources", cfg.KubeedgeModelFile)
		ms.model = modelFactory(cfg.KubeedgeVersion)
		err = conf.LoadConfigFromPath(p, ms.model)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MQTTSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	log := ctx.GetLogger()

	opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetProtocolVersion(ms.pVersion)
	if ms.clientid == "" {
		if uuid, err := uuid.NewUUID(); err != nil {
			errCh <- fmt.Errorf("failed to get uuid, the error is %s", err)
		} else {
			ms.clientid = uuid.String()
			opts.SetClientID(uuid.String())
		}
	} else {
		opts.SetClientID(ms.clientid)
	}

	if ms.certPath != "" || ms.pkeyPath != "" {
		log.Infof("Connect MQTT broker with certification and keys.")
		if cp, err := conf.ProcessPath(ms.certPath); err == nil {
			log.Infof("The certification file is %s.", cp)
			if kp, err1 := conf.ProcessPath(ms.pkeyPath); err1 == nil {
				log.Infof("The private key file is %s.", kp)
				if cer, err2 := tls.LoadX509KeyPair(cp, kp); err2 != nil {
					errCh <- err2
				} else {
					opts.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cer}})
				}
			} else {
				errCh <- err1
			}
		} else {
			errCh <- err
		}
	} else {
		log.Infof("Connect MQTT broker with username and password.")
		if ms.uName != "" {
			opts = opts.SetUsername(ms.uName)
		} else {
			log.Infof("The username is empty.")
		}

		if ms.password != "" {
			opts = opts.SetPassword(ms.password)
		} else {
			log.Infof("The password is empty.")
		}
	}
	opts.SetAutoReconnect(true)
	var reconn = false
	opts.SetConnectionLostHandler(func(client MQTT.Client, e error) {
		log.Errorf("The connection %s is disconnected due to error %s, will try to re-connect later.", ms.srv+": "+ms.clientid, e)
		reconn = true
		subscribe(ms.tpc, client, ctx, consumer, ms.model, ms.format)
	})

	opts.SetOnConnectHandler(func(client MQTT.Client) {
		if reconn {
			log.Infof("The connection is %s re-established successfully.", ms.srv+": "+ms.clientid)
			subscribe(ms.tpc, client, ctx, consumer, ms.model, ms.format)
		}
	})

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		errCh <- fmt.Errorf("found error when connecting to %s: %s", ms.srv, token.Error())
	}
	log.Infof("The connection to server %s was established successfully", ms.srv)
	ms.conn = c
	subscribe(ms.tpc, c, ctx, consumer, ms.model, ms.format)
	log.Infof("Successfully subscribe to topic %s", ms.srv+": "+ms.clientid)
}

func subscribe(topic string, client MQTT.Client, ctx api.StreamContext, consumer chan<- api.SourceTuple, model modelVersion, format string) {
	log := ctx.GetLogger()
	h := func(client MQTT.Client, msg MQTT.Message) {
		log.Debugf("instance %d received %s", ctx.GetInstanceId(), msg.Payload())
		result, e := message.Decode(msg.Payload(), format)
		//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
		if e != nil {
			log.Errorf("Invalid data format, cannot decode %s to %s format with error %s", string(msg.Payload()), format, e)
			return
		}

		meta := make(map[string]interface{})
		meta["topic"] = msg.Topic()
		meta["messageid"] = strconv.Itoa(int(msg.MessageID()))

		if nil != model {
			sliErr := model.checkType(result, msg.Topic())
			for _, v := range sliErr {
				log.Errorf(v)
			}
		}

		select {
		case consumer <- api.NewDefaultSourceTuple(result, meta):
			log.Debugf("send data to source node")
		case <-ctx.Done():
			return
		}
	}

	if token := client.Subscribe(topic, 0, h); token.Wait() && token.Error() != nil {
		log.Errorf("Found error: %s", token.Error())
	} else {
		log.Infof("Successfully subscribe to topic %s", topic)
	}
}

func (ms *MQTTSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Mqtt Source instance %d Done", ctx.GetInstanceId())
	if ms.conn != nil && ms.conn.IsConnected() {
		ms.conn.Disconnect(5000)
	}
	return nil
}
