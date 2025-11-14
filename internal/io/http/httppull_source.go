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

package http

import (
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type HttpPullSource struct {
	*ClientConf
	lastMD5 string
	psc     *pullSourceConfig
}

func (hps *HttpPullSource) GetOffset() (any, error) {
	return hps.psc.States, nil
}

func (hps *HttpPullSource) Rewind(offset any) error {
	m, ok := offset.(map[string]interface{})
	if ok {
		for k, v := range m {
			hps.psc.States[k] = v
		}
	}
	return nil
}

func (hps *HttpPullSource) ResetOffset(input map[string]any) error {
	for k, v := range input {
		hps.psc.States[k] = v
	}
	return nil
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
	err := hps.Conn(ctx)
	if err != nil {
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

type pullSourceConfig struct {
	Path   string         `json:"datasource"`
	States map[string]any `json:"states"`
}

func (hps *HttpPullSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	pc := &pullSourceConfig{States: map[string]any{}}
	if err := cast.MapToStruct(configs, pc); err != nil {
		return err
	}
	if hps.ClientConf == nil {
		hps.ClientConf = &ClientConf{}
	}
	hps.psc = pc
	return hps.InitConf(ctx, pc.Path, configs)
}

func (hps *HttpPullSource) doPull(ctx api.StreamContext) ([]map[string]any, error) {
	result, latestMD5, err := hps.doPullInternal(ctx, hps.ClientConf, hps.lastMD5)
	if err != nil {
		return nil, err
	}
	hps.lastMD5 = latestMD5
	return result, nil
}

func (hps *HttpPullSource) doPullInternal(ctx api.StreamContext, c *ClientConf, lastMD5 string) ([]map[string]any, string, error) {
	// if auth is set, the auth is handled by the client connect
	headers := c.config.Headers
	if c.accessConf != nil {
		headers = c.parsedHeaders
	}
	newBody := c.config.Body
	if c.accessConf != nil {
		newBody = c.parsedBody
	}
	var err error
	newUrl := c.config.Url
	if len(hps.psc.States) > 0 {
		newUrl, err = ctx.ParseTemplate(c.config.Url, hps.psc.States)
		if err != nil {
			return nil, "", err
		}
	}
	resp, err := hps.Send(ctx, c.config.BodyType, c.config.Method, newUrl, headers, nil, "", []byte(newBody))
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	results, newMD5, err := c.parseResponse(ctx, resp, lastMD5, true, false)
	if err != nil {
		return nil, "", err
	}
	hps.updateState(results)
	return results, newMD5, nil
}

func (hps *HttpPullSource) updateState(results []map[string]interface{}) {
	for _, r := range results {
		for k, v := range r {
			_, ok := hps.psc.States[k]
			if ok {
				hps.psc.States[k] = v
			}
		}
	}
}

func GetSource() api.Source {
	return &HttpPullSource{}
}

var _ api.PullTupleSource = &HttpPullSource{}
