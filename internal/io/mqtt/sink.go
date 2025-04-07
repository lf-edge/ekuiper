// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

// AdConf is the advanced configuration for the mqtt sink
type AdConf struct {
	Tpc      string            `json:"topic"`
	Qos      byte              `json:"qos"`
	Retained bool              `json:"retained"`
	SelId    string            `json:"connectionSelector"`
	Props    map[string]string `json:"properties"`
	PVersion string            `json:"protocolVersion"`
}

type Sink struct {
	id     string
	cw     *connection.ConnWrapper
	adconf *AdConf
	config map[string]interface{}
	cli    *Connection
}

func (ms *Sink) Provision(ctx api.StreamContext, ps map[string]any) error {
	err := ValidateConfig(ps)
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
	if adconf.PVersion != "5" && adconf.Props != nil {
		ctx.GetLogger().Warnf("Only mqtt v5 supports properties, ignore the properties setting")
	}
	return nil
}

func (ms *Sink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to mqtt server")
	var err error
	ms.id = fmt.Sprintf("%s-%s-%s-mqtt-sink", ctx.GetRuleId(), ctx.GetOpId(), ms.adconf.Tpc)
	ms.cw, err = connection.FetchConnection(ctx, ms.id, "mqtt", ms.config, sch)
	if err != nil {
		return err
	}
	conn, err := ms.cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("mqtt client not ready: %v", err)
	}
	c, ok := conn.(*Connection)
	if !ok {
		return fmt.Errorf("connection %s should be mqtt connection", ms.adconf.SelId)
	}
	ms.cli = c
	conf.Log.Info("mqtt sink client ready")
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
	props := ms.adconf.Props
	// If tpc supports dynamic props(template), planner will guarantee the result has the parsed dynamic props
	if dp, ok := item.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(tpc)
		if transformed {
			tpc = temp
		}
		newProps := make(map[string]string, len(props))
		for k, v := range props {
			nv, ok := dp.DynamicProps(v)
			if ok {
				newProps[k] = nv
			} else {
				newProps[k] = v
			}
		}
		props = newProps
	}
	traced, _, span := tracenode.TraceInput(ctx, item, fmt.Sprintf("%s_emit", ctx.GetOpId()))
	if traced {
		defer span.End()
		traceID := span.SpanContext().TraceID()
		spanID := span.SpanContext().SpanID()
		if props == nil {
			props = make(map[string]string)
		}
		props["traceparent"] = tracenode.BuildTraceParentId(traceID, spanID)
	}
	ctx.GetLogger().Debugf("publishing to topic %s", tpc)
	return ms.cli.Publish(ctx, tpc, ms.adconf.Qos, ms.adconf.Retained, item.Raw(), props)
}

func (ms *Sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing mqtt sink connector, id:%v", ms.id)
	if ms.cw != nil {
		return connection.DetachConnection(ctx, ms.cw.ID)
	}
	return nil
}

func (ms *Sink) Ping(ctx api.StreamContext, props map[string]any) error {
	cli := &Connection{}
	err := cli.Provision(ctx, "test", props)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

func GetSink() api.Sink {
	return &Sink{}
}

var (
	_ api.BytesCollector = &Sink{}
	_ util.PingableConn  = &Sink{}
)
