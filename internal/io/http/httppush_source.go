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

package http

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type HttpPushSource struct {
	topic    string
	sourceID string
	ch       <-chan any
	conf     *PushConf
	props    map[string]any
}

type PushConf struct {
	Method       string `json:"method"`
	BufferLength int    `json:"bufferLength"`
	DataSource   string `json:"datasource"`
}

func (h *HttpPushSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &PushConf{
		Method:       http.MethodPost,
		BufferLength: 1024,
	}
	err := cast.MapToStruct(configs, cfg)
	if err != nil {
		return err
	}
	if cfg.Method != http.MethodPost && cfg.Method != http.MethodPut {
		return fmt.Errorf("method %s is not supported, must be POST or PUT", cfg.Method)
	}
	if !strings.HasPrefix(cfg.DataSource, "/") {
		return fmt.Errorf("property `endpoint` must start with /")
	}

	h.conf = cfg
	h.props = configs
	conf.Log.Debugf("Initialized with configurations %#v.", cfg)
	return nil
}

func (h *HttpPushSource) Close(ctx api.StreamContext) error {
	pubsub.CloseSourceConsumerChannel(h.topic, h.sourceID)
	// TODO if supports to be resource, this should change to the unique conn id
	return connection.DetachConnection(ctx, h.conf.DataSource)
}

func (h *HttpPushSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	cw, err := connection.FetchConnection(ctx, h.conf.DataSource, "httppush", h.props, sch)
	if err != nil {
		return err
	}
	c, err := cw.Wait(ctx)
	if c == nil {
		return fmt.Errorf("http push endpoint not ready: %v", err)
	}
	hc, ok := c.(*httpserver.HttpPushConnection)
	if !ok {
		return fmt.Errorf("connection isn't httppushConnection")
	}
	h.sourceID = fmt.Sprintf("%s_%s_%v", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
	h.topic = hc.GetTopic()
	sch(api.ConnectionConnected, "")
	return nil
}

func (h *HttpPushSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	ch := pubsub.CreateSub(h.topic, nil, h.sourceID, 1024)
	h.ch = ch
	go func(ctx api.StreamContext) {
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-h.ch:
				data := v.([]byte)
				e := infra.SafeRun(func() error {
					ingest(ctx, data, nil, timex.GetNow())
					return nil
				})
				if e != nil {
					ingestError(ctx, e)
				}
			}
		}
	}(ctx)
	return nil
}

var _ api.BytesSource = &HttpPushSource{}
