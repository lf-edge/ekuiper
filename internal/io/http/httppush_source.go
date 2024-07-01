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
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type HttpPushSource struct {
	conf *PushConf
	ch   <-chan any
}

type PushConf struct {
	Method       string `json:"method"`
	ContentType  string `json:"contentType"`
	BufferLength int    `json:"bufferLength"`
	DataSource   string `json:"datasource"`
}

func (h *HttpPushSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &PushConf{
		Method:       http.MethodPost,
		ContentType:  "application/json",
		BufferLength: 1024,
	}
	err := cast.MapToStruct(configs, cfg)
	if err != nil {
		return err
	}
	if cfg.Method != http.MethodPost && cfg.Method != http.MethodPut {
		return fmt.Errorf("method %s is not supported, must be POST or PUT", cfg.Method)
	}
	if cfg.ContentType != "application/json" {
		return fmt.Errorf("property `contentType` must be application/json")
	}
	if !strings.HasPrefix(cfg.DataSource, "/") {
		return fmt.Errorf("property `endpoint` must start with /")
	}

	h.conf = cfg
	conf.Log.Debugf("Initialized with configurations %#v.", cfg)
	return nil
}

func (h *HttpPushSource) Close(ctx api.StreamContext) error {
	httpserver.UnregisterEndpoint(h.conf.DataSource)
	return nil
}

func (h *HttpPushSource) Connect(ctx api.StreamContext) error {
	t, err := httpserver.RegisterEndpoint(h.conf.DataSource, h.conf.Method)
	if err != nil {
		return err
	}
	h.ch = pubsub.CreateSub(t, nil, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), h.conf.BufferLength)
	return nil
}

func (h *HttpPushSource) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	go func(ctx api.StreamContext) {
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-h.ch:
				e := infra.SafeRun(func() error {
					ingest(ctx, v, nil, timex.GetNow())
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

var _ api.TupleSource = &HttpPushSource{}
