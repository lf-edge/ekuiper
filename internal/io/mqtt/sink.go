// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/io/connection"
	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// AdConf is the advanced configuration for the mqtt sink
type AdConf struct {
	Tpc      string `json:"topic"`
	Qos      byte   `json:"qos"`
	Retained bool   `json:"retained"`
	SelId    string `json:"connectionSelector"`
}

type Sink struct {
	adconf *AdConf
	config map[string]interface{}
	cli    *client.Connection
}

func (ms *Sink) Provision(_ api.StreamContext, ps map[string]any) error {
	_, err := client.ValidateConfig(ps)
	if err != nil {
		return err
	}
	adconf := &AdConf{}
	err = cast.MapToStruct(ps, adconf)
	if err != nil {
		return err
	}
	if adconf.Tpc == "" {
		return fmt.Errorf("mqtt sink is missing property topic")
	}
	if err := validateMQTTSinkTopic(adconf.Tpc); err != nil {
		return err
	}
	if adconf.Qos != 0 && adconf.Qos != 1 && adconf.Qos != 2 {
		return fmt.Errorf("invalid qos value %v, the value could be only int 0 or 1 or 2", adconf.Qos)
	}
	ms.config = ps
	ms.adconf = adconf
	return nil
}

func (ms *Sink) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Connecting to mqtt server")
	var cli *client.Connection
	var err error
	if len(ms.adconf.SelId) > 0 {
		conn, err := connection.GetNameConnection(ms.adconf.SelId)
		if err != nil {
			return err
		}
		c, ok := conn.(*client.Connection)
		if !ok {
			return fmt.Errorf("connection %s should be mqtt connection", ms.adconf.SelId)
		}
		cli = c
	} else {
		cli, err = client.CreateAnonymousConnection(ctx, ms.config)
	}
	ms.cli = cli
	return err
}

func validateMQTTSinkTopic(topic string) error {
	if strings.Contains(topic, "#") || strings.Contains(topic, "+") {
		return fmt.Errorf("mqtt sink topic shouldn't contain # or +")
	}
	return nil
}

func (ms *Sink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	tpc := ms.adconf.Tpc
	// If tpc supports dynamic props(template), planner will guarantee the result has the parsed dynamic props
	if dp, ok := item.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(tpc)
		if transformed {
			tpc = temp
		}
	}
	ctx.GetLogger().Debugf("publishing to topic %s", tpc)
	return ms.cli.Publish(tpc, ms.adconf.Qos, ms.adconf.Retained, item.Raw())
}

func (ms *Sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Info("Closing mqtt sink connector")
	if ms.cli != nil {
		if len(ms.adconf.SelId) < 1 {
			ms.cli.Close()
		} else {
			ms.cli.DetachPub(nil)
		}
	}
	return nil
}

func GetSink() api.Sink {
	return &Sink{}
}

var _ api.BytesCollector = &Sink{}
