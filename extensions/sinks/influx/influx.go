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

// +build plugins

package main

import (
	_ "github.com/influxdata/influxdb1-client/v2"
	client "github.com/influxdata/influxdb1-client/v2"
	api "github.com/lf-edge/ekuiper/pkg/api"
	"strings"
	"time"
)

type influxSink struct {
	addr         string
	username     string
	password     string
	measurement  string
	databasename string
	tagkey       string
	tagvalue     string
	fields       string
	cli          client.Client
	fieldmap     map[string]interface{}
}

func (m *influxSink) Configure(props map[string]interface{}) error {
	if i, ok := props["addr"]; ok {
		if i, ok := i.(string); ok {
			m.addr = i
		}
	}
	if i, ok := props["username"]; ok {
		if i, ok := i.(string); ok {
			m.username = i
		}
	}
	if i, ok := props["password"]; ok {
		if i, ok := i.(string); ok {
			m.password = i
		}
	}
	if i, ok := props["measurement"]; ok {
		if i, ok := i.(string); ok {
			m.measurement = i
		}
	}
	if i, ok := props["databasename"]; ok {
		if i, ok := i.(string); ok {
			m.databasename = i
		}
	}
	if i, ok := props["tagkey"]; ok {
		if i, ok := i.(string); ok {
			m.tagkey = i
		}
	}
	if i, ok := props["tagvalue"]; ok {
		if i, ok := i.(string); ok {
			m.tagvalue = i
		}
	}
	if i, ok := props["fields"]; ok {
		if i, ok := i.(string); ok {
			m.fields = i
		}
	}
	return nil
}

func (m *influxSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening influx sink")
	m.cli, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:     m.addr,
		Username: m.username,
		Password: m.password,
	})
	if err != nil {
		logger.Debug(err)
		return err
	}
	return nil
}

func (m *influxSink) Collect(ctx api.StreamContext, data interface{}) error {
	logger := ctx.GetLogger()
	var output map[string]interface{}
	switch v := data.(type) {
	case map[string]interface{}:
		output = v
	case []map[string]interface{}:
		if len(v) > 0 {
			output = v[0]
		} else {
			ctx.GetLogger().Warnf("Get empty data %v, just return", data)
			return nil
		}
	}

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.databasename,
		Precision: "ns",
	})
	if err != nil {
		logger.Debug(err)
		return err
	}
	tags := map[string]string{m.tagkey: m.tagvalue}
	fields := strings.Split(m.fields, ",")
	m.fieldmap = make(map[string]interface{}, 100)
	for _, field := range fields {
		if output[field] != nil {
			m.fieldmap[field] = output[field]
		}
	}

	pt, err := client.NewPoint(m.measurement, tags, m.fieldmap, time.Now())
	if err != nil {
		logger.Debug(err)
		return err
	}
	bp.AddPoint(pt)
	err = m.cli.Write(bp)
	if err != nil {
		logger.Debug(err)
		return err
	}
	logger.Debug("insert success")

	return nil
}

func (m *influxSink) Close(ctx api.StreamContext) error {
	m.cli.Close()
	return nil
}

func Influx() api.Sink {
	return &influxSink{}
}
