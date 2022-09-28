// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package node

import (
	nodeConf "github.com/lf-edge/ekuiper/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"reflect"
	"testing"
)

func TestGetConf_Apply(t *testing.T) {
	result := map[string]interface{}{
		"url":                "http://localhost",
		"method":             "post",
		"interval":           10000,
		"timeout":            5000,
		"incremental":        false,
		"body":               "{}",
		"bodyType":           "json",
		"key":                "",
		"format":             "json",
		"insecureSkipVerify": true,
		"headers": map[string]interface{}{
			"Accept": "application/json",
		},
	}
	n := NewSourceNode("test", ast.TypeStream, nil, &ast.Options{
		DATASOURCE: "/feed",
		TYPE:       "httppull",
	}, false)
	conf := nodeConf.GetSourceConf(n.sourceType, n.options)
	if !reflect.DeepEqual(result, conf) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", result, conf)
	}
}

func TestGetConfAndConvert_Apply(t *testing.T) {
	result := map[string]interface{}{
		"url":                "http://localhost:9090/",
		"method":             "post",
		"interval":           10000,
		"timeout":            5000,
		"incremental":        true,
		"body":               "{}",
		"bodyType":           "json",
		"key":                "",
		"format":             "json",
		"insecureSkipVerify": true,
		"headers": map[string]interface{}{
			"Accept": "application/json",
		},
	}
	n := NewSourceNode("test", ast.TypeStream, nil, &ast.Options{
		DATASOURCE: "/feed",
		TYPE:       "httppull",
		CONF_KEY:   "application_conf",
	}, false)
	conf := nodeConf.GetSourceConf(n.sourceType, n.options)
	if !reflect.DeepEqual(result, conf) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", result, conf)
		return
	}

	r := &httpPullSourceConfig{
		Url:                "http://localhost:9090/",
		Method:             "post",
		Interval:           10000,
		Timeout:            5000,
		Incremental:        true,
		Body:               "{}",
		BodyType:           "json",
		InsecureSkipVerify: true,
		Headers: map[string]interface{}{
			"Accept": "application/json",
		},
	}

	cfg := &httpPullSourceConfig{}
	err := cast.MapToStruct(conf, cfg)
	if err != nil {
		t.Errorf("map to sturct error %s", err)
		return
	}

	if !reflect.DeepEqual(r, cfg) {
		t.Errorf("result mismatch:\n\nexp=%v\n\ngot=%v\n\n", r, cfg)
		return
	}
}

type httpPullSourceConfig struct {
	Url                string                 `json:"url"`
	Method             string                 `json:"method"`
	Interval           int                    `json:"interval"`
	Timeout            int                    `json:"timeout"`
	Incremental        bool                   `json:"incremental"`
	Body               string                 `json:"body"`
	BodyType           string                 `json:"bodyType"`
	InsecureSkipVerify bool                   `json:"insecureSkipVerify"`
	Headers            map[string]interface{} `json:"headers"`
}
