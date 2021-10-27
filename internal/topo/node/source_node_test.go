// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"reflect"
	"testing"
)

func TestGetConf_Apply(t *testing.T) {
	result := map[string]interface{}{
		"interval": 1000,
		"ashost":   "192.168.1.100",
		"sysnr":    "02",
		"client":   "900",
		"user":     "SPERF",
		"passwd":   "PASSPASS",
		"format":   "json",
		"params": map[string]interface{}{
			"QUERY_TABLE": "VBAP",
			"ROWCOUNT":    10,
			"FIELDS": []interface{}{
				map[string]interface{}{"FIELDNAME": "MANDT"},
				map[string]interface{}{"FIELDNAME": "VBELN"},
				map[string]interface{}{"FIELDNAME": "POSNR"},
			},
		},
	}
	n := NewSourceNode("test", ast.TypeStream, nil, &ast.Options{
		DATASOURCE: "RFC_READ_TABLE",
		TYPE:       "test",
	}, false)
	contextLogger := conf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	conf := getSourceConf(ctx, n.sourceType, n.options)
	if !reflect.DeepEqual(result, conf) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", result, conf)
	}
}

func TestGetConfAndConvert_Apply(t *testing.T) {
	result := map[string]interface{}{
		"interval": 100,
		"seed":     1,
		"format":   "json",
		"pattern": map[string]interface{}{
			"count": 50,
		},
		"deduplicate": 50,
	}
	n := NewSourceNode("test", ast.TypeStream, nil, &ast.Options{
		DATASOURCE: "test",
		TYPE:       "random",
		CONF_KEY:   "dedup",
	}, false)
	contextLogger := conf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	conf := getSourceConf(ctx, n.sourceType, n.options)
	if !reflect.DeepEqual(result, conf) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", result, conf)
		return
	}

	r := &randomSourceConfig{
		Interval: 100,
		Seed:     1,
		Pattern: map[string]interface{}{
			"count": 50,
		},
		Deduplicate: 50,
	}

	cfg := &randomSourceConfig{}
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

type randomSourceConfig struct {
	Interval    int                    `json:"interval"`
	Seed        int                    `json:"seed"`
	Pattern     map[string]interface{} `json:"pattern"`
	Deduplicate int                    `json:"deduplicate"`
}
