// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type HttpLookupSource struct {
	*ClientConf
}

func (hls *HttpLookupSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	pc := &pullSourceConfig{}
	if err := cast.MapToStruct(configs, pc); err != nil {
		return err
	}
	if hls.ClientConf == nil {
		hls.ClientConf = &ClientConf{}
	}
	return hls.InitConf(ctx, pc.Path, configs)
}

func (hls *HttpLookupSource) Close(ctx api.StreamContext) error {
	return nil
}

func (hls *HttpLookupSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnected, "")
	return nil
}

func (hls *HttpLookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error) {
	resps, _, err := doPull(ctx, hls.ClientConf, "")
	if err != nil {
		return nil, err
	}
	resps = pruneData(resps, fields)
	return lookupJoin(resps, keys, values), nil
}

func pruneData(data []map[string]any, fields []string) []map[string]any {
	for index, row := range data {
		for key := range row {
			if !findColumn(key, fields) {
				delete(row, key)
			}
		}
		data[index] = row
	}
	return data
}

func findColumn(column string, fields []string) bool {
	for _, field := range fields {
		if field == column {
			return true
		}
	}
	return false
}

func lookupJoin(dataMap []map[string]interface{}, keys []string, values []interface{}) []map[string]any {
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

func GetLookUpSource() api.Source {
	return &HttpLookupSource{}
}

var _ api.LookupSource = &HttpLookupSource{}

func doPull(ctx api.StreamContext, c *ClientConf, lastMD5 string) ([]map[string]any, string, error) {
	headers := c.config.Headers
	if c.accessConf != nil {
		headers = c.parsedHeaders
	}
	newBody := c.config.Body
	if c.accessConf != nil {
		newBody = c.parsedBody
	}
	resp, err := c.Send(ctx, c.config.BodyType, c.config.Method, c.config.Url, headers, nil, "", []byte(newBody))
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	results, newMD5, err := c.parseResponse(ctx, resp, lastMD5, true, false)
	if err != nil {
		return nil, "", err
	}
	return results, newMD5, nil
}
