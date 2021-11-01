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
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"path"
	"strconv"
	"strings"
)

type MQTTSource struct {
	srv        string
	qos        int
	format     string
	tpc        string
	clientid   string
	pVersion   uint
	uName      string
	password   string
	certPath   string
	pkeyPath   string
	rootCapath string
	conSel     string
	InSecure   bool

	model  modelVersion
	schema map[string]interface{}
	conn   MQTT.Client
}

type MQTTConfig struct {
	Format             string   `json:"format"`
	Qos                int      `json:"qos"`
	Servers            []string `json:"servers"`
	Clientid           string   `json:"clientid"`
	PVersion           string   `json:"protocolVersion"`
	Uname              string   `json:"username"`
	Password           string   `json:"password"`
	Certification      string   `json:"certificationPath"`
	PrivateKPath       string   `json:"privateKeyPath"`
	RootCaPath         string   `json:"rootCaPath"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify"`
	KubeedgeModelFile  string   `json:"kubeedgeModelFile"`
	KubeedgeVersion    string   `json:"kubeedgeVersion"`
	ConnectionSelector string   `json:"connectionSelector"`
}

func (ms *MQTTSource) WithSchema(_ string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) Configure(topic string, props map[string]interface{}) error {
	cfg := &MQTTConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	ms.tpc = topic
	if cfg.ConnectionSelector == "" {
		if srvs := cfg.Servers; srvs != nil && len(srvs) > 0 {
			ms.srv = srvs[0]
		} else {
			return fmt.Errorf("missing server property")
		}
	}
	ms.conSel = cfg.ConnectionSelector
	ms.format = cfg.Format
	ms.clientid = cfg.Clientid
	ms.qos = cfg.Qos

	ms.pVersion = 3
	if cfg.PVersion == "3.1.1" {
		ms.pVersion = 4
	}

	ms.uName = cfg.Uname
	ms.password = strings.Trim(cfg.Password, " ")
	ms.certPath = cfg.Certification
	ms.pkeyPath = cfg.PrivateKPath
	ms.rootCapath = cfg.RootCaPath

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
	var client MQTT.Client
	log := ctx.GetLogger()

	if ms.conSel != "" {
		con, err := ctx.GetConnection(ms.conSel)
		if err != nil {
			log.Errorf("The mqtt client for connection selector %s get fail with error: %s", ms.conSel, err)
			errCh <- err
			return
		}
		client = con.(MQTT.Client)
		log.Infof("The mqtt client for connection selector %s get successfully", ms.conSel)
	} else {
		opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetProtocolVersion(ms.pVersion)
		if ms.clientid == "" {
			if newUUID, err := uuid.NewUUID(); err != nil {
				errCh <- fmt.Errorf("failed to get uuid, the error is %s", err)
				return
			} else {
				ms.clientid = newUUID.String()
				opts = opts.SetClientID(newUUID.String())
			}
		} else {
			opts = opts.SetClientID(ms.clientid)
		}

		tlsOpts := cert.TlsConfigurationOptions{
			SkipCertVerify: ms.InSecure,
			CertFile:       ms.certPath,
			KeyFile:        ms.pkeyPath,
			CaFile:         ms.rootCapath,
		}
		log.Infof("Connect MQTT broker with TLS configs. %v", tlsOpts)
		tlscfg, err := cert.GenerateTLSForClient(tlsOpts)
		if err != nil {
			errCh <- err
			return
		}

		opts = opts.SetTLSConfig(tlscfg)

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
		opts.SetAutoReconnect(true)
		var reconn = false
		opts.SetConnectionLostHandler(func(client MQTT.Client, e error) {
			log.Errorf("The connection %s is disconnected due to error %s, will try to re-connect later.", ms.srv+": "+ms.clientid, e)
			reconn = true
			subscribe(ms, client, ctx, consumer)
		})

		opts.SetOnConnectHandler(func(client MQTT.Client) {
			if reconn {
				log.Infof("The connection is %s re-established successfully.", ms.srv+": "+ms.clientid)
				subscribe(ms, client, ctx, consumer)
			}
		})

		client = MQTT.NewClient(opts)

		if token := client.Connect(); token.Wait() && token.Error() != nil {
			errCh <- fmt.Errorf("found error when connecting to %s: %s", ms.srv, token.Error())
			return
		}
		log.Infof("The connection to server %s:%s was established successfully", ms.srv, ms.clientid)
	}

	ms.conn = client
	subscribe(ms, client, ctx, consumer)
}

func subscribe(ms *MQTTSource, client MQTT.Client, ctx api.StreamContext, consumer chan<- api.SourceTuple) {
	log := ctx.GetLogger()
	h := func(client MQTT.Client, msg MQTT.Message) {
		log.Debugf("instance %d received %s", ctx.GetInstanceId(), msg.Payload())
		result, e := message.Decode(msg.Payload(), ms.format)
		//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
		if e != nil {
			log.Errorf("Invalid data format, cannot decode %s to %s format with error %s", string(msg.Payload()), ms.format, e)
			return
		}

		meta := make(map[string]interface{})
		meta["topic"] = msg.Topic()
		meta["messageid"] = strconv.Itoa(int(msg.MessageID()))

		if nil != ms.model {
			sliErr := ms.model.checkType(result, msg.Topic())
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

	if token := client.Subscribe(ms.tpc, byte(ms.qos), h); token.Wait() && token.Error() != nil {
		log.Errorf("Found error: %s", token.Error())
	} else {
		log.Infof("Successfully subscribe to topic %s", ms.tpc)
	}
}

func (ms *MQTTSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Mqtt Source instance %d Done", ctx.GetInstanceId())
	if ms.conn != nil && ms.conn.IsConnected() && ms.conSel == "" {
		ms.conn.Disconnect(5000)
	}
	if ms.conSel != "" {
		ctx.ReleaseConnection(ms.conSel)
	}
	return nil
}
