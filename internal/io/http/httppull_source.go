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

package http

import (
	"encoding/json"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type HttpPullSource struct {
	*ClientConf
	lastMD5 string
}

func (hps *HttpPullSource) Pull(ctx api.StreamContext, trigger time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	results, err := hps.doPull(ctx)
	if err != nil {
		ingestError(ctx, err)
		return
	}
	ingest(ctx, results, nil, trigger)
}

func (hps *HttpPullSource) Close(ctx api.StreamContext) error {
	return nil
}

func (hps *HttpPullSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnected, "")
	return nil
}

type pullSourceConfig struct {
	Path string `json:"datasource"`
}

func (hps *HttpPullSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	pc := &pullSourceConfig{}
	if err := cast.MapToStruct(configs, pc); err != nil {
		return err
	}
	if hps.ClientConf == nil {
		hps.ClientConf = &ClientConf{}
	}
	return hps.InitConf(pc.Path, configs)
}

func (hps *HttpPullSource) doPull(ctx api.StreamContext) ([]map[string]any, error) {
	result, latestMD5, err := doPull(ctx, hps.ClientConf, hps.lastMD5)
	if err != nil {
		return nil, err
	}
	hps.lastMD5 = latestMD5
	return result, nil
}

func doPull(ctx api.StreamContext, c *ClientConf, lastMD5 string) ([]map[string]any, string, error) {
	headers, err := c.parseHeaders(ctx, c.tokens)
	if err != nil {
		return nil, "", err
	}
	body := writeTokenIntoBody(c.tokens, []byte(c.config.Body))
	resp, err := httpx.Send(ctx.GetLogger(), c.client, c.config.BodyType, c.config.Method, c.config.Url, headers, body)
	if err != nil {
		return nil, "", err
	}
	results, newMD5, err := c.parseResponse(ctx, resp, lastMD5, true, false)
	if err != nil {
		return nil, "", err
	}
	return results, newMD5, nil
}

func writeTokenIntoBody(tokens map[string]interface{}, body []byte) []byte {
	m := make(map[string]interface{})
	err := json.Unmarshal(body, &m)
	if err != nil {
		return body
	}
	for k, v := range tokens {
		m[k] = v
	}
	newbody, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return newbody
}

func GetSource() api.Source {
	return &HttpPullSource{}
}

var _ api.PullTupleSource = &HttpPullSource{}
