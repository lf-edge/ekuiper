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
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

// AdConf is the advanced configuration for the mqtt sink
type AdConf struct {
	Tpc      string `json:"topic"`
	Qos      byte   `json:"qos"`
	Retained bool   `json:"retained"`

	ResendTopic string `json:"resendDestination"`
}

type Sink struct {
	adconf *AdConf
	config map[string]interface{}
	cli    *Connection
}

func (ms *Sink) Provision(_ api.StreamContext, ps map[string]any) error {
	_, err := validateConfig(ps)
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
	if adconf.ResendTopic == "" {
		adconf.ResendTopic = adconf.Tpc
	}
	ms.adconf = adconf
	return nil
}

func (ms *Sink) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Connecting to mqtt server")
	cli, err := GetConnection(ctx, ms.config)
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
	token := ms.cli.Publish(tpc, ms.adconf.Qos, ms.adconf.Retained, item.Raw())
	err := handleToken(token)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("found error when publishing to topic %s: %s", ms.adconf.Tpc, err))
	}
	return nil
}

func (ms *Sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Info("Closing mqtt sink connector")
	if ms.cli != nil {
		DetachConnection(ms.cli.GetClientId(), "")
		ms.cli = nil
	}
	return nil
}

func GetSink() api.Sink {
	return &Sink{}
}

var _ api.BytesCollector = &Sink{}
