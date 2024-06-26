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
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type HttpPullSource struct {
	*ClientConf
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

func (hps *HttpPullSource) Connect(ctx api.StreamContext) error {
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
	return hps.InitConf(pc.Path, configs)
}

func (hps *HttpPullSource) doPull(ctx api.StreamContext) ([]map[string]any, error) {
	// Parse body which may contain dynamic time range and tokens, so merge them
	var tempProps map[string]any
	if hps.tokens != nil {
		tempProps = hps.tokens
	} else {
		tempProps = make(map[string]any)
	}
	url, err := ctx.ParseTemplate(hps.config.Url, tempProps)
	if err != nil {
		return nil, err
	}

	// check oAuth token expiration
	if hps.accessConf != nil && hps.accessConf.ExpireInSecond > 0 &&
		int(time.Now().Sub(hps.tokenLastUpdateAt).Abs().Seconds()) >= hps.accessConf.ExpireInSecond {
		ctx.GetLogger().Debugf("Refreshing token for HTTP pull")
		if err := hps.refresh(ctx); err != nil {
			ctx.GetLogger().Warnf("Refresh HTTP pull token error: %v", err)
		}
	}
	headers, err := hps.parseHeaders(ctx, tempProps)
	if err != nil {
		return nil, err
	}
	body, err := ctx.ParseTemplate(hps.config.Body, tempProps)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Debugf("httppull source sending request url: %s, headers: %v, body %s", url, headers, hps.config.Body)
	resp, err := httpx.Send(ctx.GetLogger(), hps.client, hps.config.BodyType, hps.config.Method, url, headers, true, []byte(body))
	if err != nil {
		return nil, err
	}
	results, _, err := hps.parseResponse(ctx, resp, true, false)
	if err != nil {
		return nil, err
	}
	return results, nil
}
