// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type PushConf struct {
	Method       string `json:"method"`
	ContentType  string `json:"contentType"`
	BufferLength int    `json:"bufferLength"`
	Endpoint     string `json:"endpoint"`
}

type PushSource struct {
	conf *PushConf
}

func (hps *PushSource) Configure(endpoint string, props map[string]interface{}) error {
	cfg := &PushConf{
		Method:       http.MethodPost,
		ContentType:  "application/json",
		BufferLength: 1024,
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return err
	}
	if cfg.Method != http.MethodPost && cfg.Method != http.MethodPut {
		return fmt.Errorf("method %s is not supported, must be POST or PUT", cfg.Method)
	}
	if cfg.ContentType != "application/json" {
		return fmt.Errorf("property `contentType` must be application/json")
	}
	if !strings.HasPrefix(endpoint, "/") {
		return fmt.Errorf("property `endpoint` must start with /")
	}

	cfg.Endpoint = endpoint
	hps.conf = cfg
	conf.Log.Debugf("Initialized with configurations %#v.", cfg)
	return nil
}

func (hps *PushSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	t, done, err := httpserver.RegisterEndpoint(hps.conf.Endpoint, hps.conf.Method, hps.conf.ContentType)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	defer httpserver.UnregisterEndpoint(hps.conf.Endpoint)
	ch := pubsub.CreateSub(t, nil, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()), hps.conf.BufferLength)
	defer pubsub.CloseSourceConsumerChannel(t, fmt.Sprintf("%s_%s_%d", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId()))
	for {
		select {
		case <-done: // http data server error
			infra.DrainError(ctx, fmt.Errorf("http data server shutdown"), errCh)
			return
		case v, opened := <-ch:
			if !opened {
				return
			}
			consumer <- v
		case <-ctx.Done():
			return
		}
	}
}

func (hps *PushSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP push source")
	return nil
}
