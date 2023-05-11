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

package main

import (
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/influxdata/influxdb-client-go/v2"
	client "github.com/influxdata/influxdb-client-go/v2"

	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type influxSink2 struct {
	addr         string
	token        string
	measurement  string
	org          string
	bucket       string
	tagKey       string
	tagValue     string
	fields       []string
	cli          client.Client
	fieldMap     map[string]interface{}
	hasTransform bool
}

func (m *influxSink2) Configure(props map[string]interface{}) error {
	if i, ok := props["addr"]; ok {
		if i, ok := i.(string); ok {
			m.addr = i
		}
	}
	if i, ok := props["measurement"]; ok {
		if i, ok := i.(string); ok {
			m.measurement = i
		}
	}
	if i, ok := props["tagKey"]; ok {
		if i, ok := i.(string); ok {
			m.tagKey = i
		}
	}
	if i, ok := props["tagValue"]; ok {
		if i, ok := i.(string); ok {
			m.tagValue = i
		}
	}
	if i, ok := props["fields"]; ok {
		if i, ok := i.([]interface{}); ok {
			for _, v := range i {
				if v, ok := v.(string); ok {
					m.fields = append(m.fields, v)
				}
			}
		}
	}
	if i, ok := props["dataTemplate"]; ok {
		if i, ok := i.(string); ok && i != "" {
			m.hasTransform = true
		}
	}

	if i, ok := props["token"]; ok {
		if i, ok := i.(string); ok {
			m.token = i
		}
	}
	if i, ok := props["org"]; ok {
		if i, ok := i.(string); ok {
			m.org = i
		}
	}
	if i, ok := props["bucket"]; ok {
		if i, ok := i.(string); ok {
			m.bucket = i
		}
	}

	return nil
}

func (m *influxSink2) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening influx2 sink")
	options := client.DefaultOptions().SetBatchSize(100)
	m.cli = client.NewClientWithOptions(m.addr, m.token, options)
	return nil
}

func (m *influxSink2) Collect(ctx api.StreamContext, data interface{}) error {
	logger := ctx.GetLogger()
	if m.hasTransform {
		jsonBytes, _, err := ctx.TransformOutput(data, true)
		if err != nil {
			return err
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(jsonBytes, &m)
		if err != nil {
			return fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
		}
		data = m
	} else if len(m.fields) > 0 {
		d, err := transform.SelectMap(data, m.fields)
		if err != nil {
			return fmt.Errorf("fail to select fields %v for data %v", m.fields, data)
		}
		data = d
	}
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

	writeAPI := m.cli.WriteAPIBlocking(m.org, m.bucket)

	tags := map[string]string{m.tagKey: m.tagValue}
	m.fieldMap = output

	pt := client.NewPoint(m.measurement, tags, m.fieldMap, time.Now())

	err := writeAPI.WritePoint(ctx, pt)
	if err != nil {
		logger.Debug(err)
		return err
	}
	logger.Debug("insert data into influxdb2 success")

	return nil
}

func (m *influxSink2) Close(ctx api.StreamContext) error {
	m.cli.Close()
	return nil
}

func Influx2() api.Sink {
	return &influxSink2{}
}
