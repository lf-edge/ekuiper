// Copyright 2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func GetLookUpSource() *lookupSource {
	return &lookupSource{}
}

type lookupSource struct {
	*ClientConf
}

func (l *lookupSource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("lookup source is opened")
	return nil
}

func (l *lookupSource) Configure(datasource string, props map[string]interface{}) error {
	conf.Log.Infof("Initialized Httppull lookup table with configurations %#v.", props)
	if l.ClientConf == nil {
		l.ClientConf = &ClientConf{}
	}
	return l.InitConf(datasource, props)
}

func (l *lookupSource) Lookup(ctx api.StreamContext, _ []string, keys []string, values []interface{}) ([]api.SourceTuple, error) {
	resps, err := l.pull(ctx)
	if err != nil {
		return nil, err
	}
	matched := l.lookupJoin(resps, keys, values)
	var results []api.SourceTuple
	meta := make(map[string]interface{})
	for _, resp := range matched {
		results = append(results, api.NewDefaultSourceTupleWithTime(resp, meta, conf.GetNow()))
	}
	return results, nil
}

func (l *lookupSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP pull lookup table")
	return nil
}

func (l *lookupSource) lookupJoin(dataMap []map[string]interface{}, keys []string, values []interface{}) []map[string]interface{} {
	var resps []map[string]interface{}
	for _, resp := range dataMap {
		match := true
		for i, k := range keys {
			if val, ok := resp[k]; !ok || val != values[i] {
				match = false
				break
			}
		}
		if match {
			resps = append(resps, resp)
		}
	}
	return resps
}

func (l *lookupSource) pull(ctx api.StreamContext) ([]map[string]interface{}, error) {
	// check oAuth token expiration
	if l.accessConf != nil && l.accessConf.ExpireInSecond > 0 &&
		// S1012 static check fix: We should use time.Since to replace time.Now().Sub()
		int(time.Since(l.tokenLastUpdateAt).Abs().Seconds()) >= l.accessConf.ExpireInSecond {
		ctx.GetLogger().Debugf("Refreshing token for HTTP pull")
		if err := l.refresh(ctx); err != nil {
			ctx.GetLogger().Warnf("Refresh HTTP pull token error: %v", err)
		}
	}
	headers, err := l.parseHeaders(ctx, l.tokens)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Debugf("httppull source sending request url: %s, headers: %v, body %s", l.config.Url, headers, l.config.Body)

	body := l.config.Body
	resp, err := httpx.Send(ctx.GetLogger(), l.client, l.config.Url, l.config.Method,
		httpx.WithHeadersMap(headers),
		httpx.WithBody(body, l.config.BodyType, true, l.compressor, l.config.Compression),
	)
	if err != nil {
		ctx.GetLogger().Warnf("Found error %s when trying to reach %v ", err, l)
		return nil, err
	}
	ctx.GetLogger().Debugf("httppull source got response %v", resp)
	results, _, e := l.parseResponse(ctx, resp, true, nil, false)
	if e != nil {
		return nil, err
	}
	return results, nil
}
