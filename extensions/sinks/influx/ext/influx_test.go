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

package influx

import (
	"testing"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]interface{}
		expected c
		error    string
	}{
		{ // 0
			name: "test1",
			conf: map[string]interface{}{
				"addr":        "http://192.168.0.3:8086",
				"username":    "name",
				"password":    "pass",
				"measurement": "test",
				"database":    "db1",
				"tags": map[string]interface{}{
					"tag": "value",
				},
				"fields":      []interface{}{"temperature"},
				"tsFieldName": "ts",
			},
			expected: c{
				Addr:         "http://192.168.0.3:8086",
				Username:     "name",
				Password:     "******",
				Database:     "db1",
				PrecisionStr: "ms",
				Measurement:  "test",
				Tags: map[string]string{
					"tag": "value",
				},
				Fields:      []string{"temperature"},
				TsFieldName: "ts",
				BatchSize:   1,
			},
		},
		{
			name: "unmarshall error",
			conf: map[string]interface{}{
				"database": 12,
			},
			error: "error configuring influx2 sink: 1 error(s) decoding:\n\n* 'database' expected type 'string', got unconvertible type 'int', value: '12'",
		},
		{
			name:  "addr missing error",
			conf:  map[string]interface{}{},
			error: "addr is required",
		},
		{
			name: "database missing error",
			conf: map[string]interface{}{
				"addr": "http://192.168.0.3:8086",
			},
			error: "database is required",
		},
		{
			name: "precision invalid error",
			conf: map[string]interface{}{
				"addr":      "http://192.168.0.3:8086",
				"username":  "user1",
				"password":  "pass1",
				"database":  "bucket_one",
				"precision": "abc",
			},
			error: "precision abc is not supported",
		},
		{
			name: "measurement missing error",
			conf: map[string]interface{}{
				"addr":      "http://192.168.0.3:8086",
				"username":  "user1",
				"password":  "pass1",
				"database":  "bucket_one",
				"precision": "ns",
			},
			error: "measurement is required",
		},
		{
			name: "unmarshall error for tls",
			conf: map[string]interface{}{
				"addr":        "http://192.168.0.3:8086",
				"username":    "user1",
				"password":    "pass1",
				"database":    "bucket_one",
				"precision":   "ns",
				"measurement": "mm",
				"rootCaPath":  12,
			},
			error: "error configuring tls: 1 error(s) decoding:\n\n* 'rootCaPath' expected type 'string', got unconvertible type 'int', value: '12'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink{}
			err := ifsink.Configure(test.conf)
			if test.error == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Equal(t, test.error, err.Error())
				return
			}
			assert.Equal(t, test.expected, ifsink.conf)
		})
	}
}

type rawPoint struct {
	fields map[string]any
	tags   map[string]string
	tt     time.Time
}

func TestCollectPoints(t *testing.T) {
	conf.InitClock()
	mockclock.ResetClock(10)
	tests := []struct {
		name       string
		conf       c
		data       any
		transforms struct {
			dataTemplate string
			dataField    string
			fields       []string
		}
		points []rawPoint
	}{
		{
			name: "normal",
			conf: c{
				Measurement: "test1",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				PrecisionStr: "ms",
				Database:     "db1",
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"temperature": 20,
						"humidity":    50,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					tt: time.UnixMilli(10),
				},
			},
		},
		{
			name: "normal batch",
			conf: c{
				Measurement: "test2",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				PrecisionStr: "s",
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
				},
				{
					"temperature": 30,
					"humidity":    60,
				},
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"temperature": 20,
						"humidity":    50,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					tt: time.UnixMilli(10),
				},
				{
					fields: map[string]any{
						"temperature": 30,
						"humidity":    60,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					tt: time.UnixMilli(10),
				},
			},
		},
		{
			name: "normal batch sendSingle",
			conf: c{
				Measurement: "test3",
				Tags: map[string]string{
					"tag1": "{{.humidity}}",
					"tag2": "value2",
				},
				SendSingle:   true,
				PrecisionStr: "s",
				TsFieldName:  "ts",
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
					"ts":          100,
				},
				{
					"temperature": 30,
					"humidity":    60,
					"ts":          110,
				},
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"temperature": 20,
						"humidity":    50,
						"ts":          100,
					},
					tags: map[string]string{
						"tag1": "50",
						"tag2": "value2",
					},
					tt: time.Unix(100, 0),
				},
				{
					fields: map[string]any{
						"temperature": 30,
						"humidity":    60,
						"ts":          110,
					},
					tags: map[string]string{
						"tag1": "60",
						"tag2": "value2",
					},
					tt: time.Unix(110, 0),
				},
			},
		},
		{
			name: "batch/sendSingle with dataTemplate",
			conf: c{
				Measurement: "test4",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				SendSingle:   true,
				PrecisionStr: "us",
				TsFieldName:  "ts",
			},
			transforms: struct {
				dataTemplate string
				dataField    string
				fields       []string
			}{
				dataTemplate: `{"t":{{.temperature}}}`,
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
					"ts":          100,
				},
				{
					"temperature": 30,
					"humidity":    60,
					"ts":          110,
				},
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"t": 20.0,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					tt: time.UnixMicro(100),
				},
				{
					fields: map[string]any{
						"t": 30.0,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					tt: time.UnixMicro(110),
				},
			},
		},
		{
			name: "single with fields",
			conf: c{
				Measurement: "test5",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "{{.humidity}}",
				},
				PrecisionStr: "ns",
				TsFieldName:  "ts",
			},
			transforms: struct {
				dataTemplate string
				dataField    string
				fields       []string
			}{
				fields: []string{"humidity"},
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
				"ts":          100,
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"humidity": 50,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "50",
					},
					tt: time.Unix(0, 100),
				},
			},
		},
		{
			name: "single with dataTemplate and dataField",
			conf: c{
				Measurement: "test5",
				Tags: map[string]string{
					"tag1": "{{.t}}",
					"tag2": "{{.h}}",
				},
			},
			transforms: struct {
				dataTemplate string
				dataField    string
				fields       []string
			}{
				dataTemplate: `{"payload":{"t":{{.temperature}},"h":{{.humidity}}}}`,
				dataField:    "payload",
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"t": 20.0,
						"h": 50.0,
					},
					tags: map[string]string{
						"tag1": "20",
						"tag2": "50",
					},
					tt: time.UnixMilli(10),
				},
			},
		},
		{
			name: "batch with fields",
			conf: c{
				Measurement: "test6",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "{{.humidity}}",
				},
			},
			transforms: struct {
				dataTemplate string
				dataField    string
				fields       []string
			}{
				fields: []string{"humidity"},
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
				},
				{
					"temperature": 30,
					"humidity":    60,
				},
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"humidity": 50,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "50",
					},
					tt: time.UnixMilli(10),
				},
				{
					fields: map[string]any{
						"humidity": 60,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "60",
					},
					tt: time.UnixMilli(10),
				},
			},
		},
		{
			name: "batch with dataTemplate of single output",
			conf: c{
				Measurement: "test6",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "{{.humidity}}",
				},
			},
			transforms: struct {
				dataTemplate string
				dataField    string
				fields       []string
			}{
				dataTemplate: `{{json (index . 0)}}`,
				fields:       []string{"temperature"},
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
				},
				{
					"temperature": 30,
					"humidity":    60,
				},
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"temperature": 20.0,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "50",
					},
					tt: time.UnixMilli(10),
				},
			},
		},
		{
			name: "batch with dataTemplate of batch output",
			conf: c{
				Measurement: "test6",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
			},
			transforms: struct {
				dataTemplate string
				dataField    string
				fields       []string
			}{
				dataTemplate: `{{json .}}`,
				fields:       []string{"temperature"},
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
				},
				{
					"temperature": 30,
					"humidity":    60,
				},
			},
			points: []rawPoint{
				{
					fields: map[string]any{
						"temperature": 20.0,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					tt: time.UnixMilli(10),
				},
				{
					fields: map[string]any{
						"temperature": 30.0,
					},
					tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					tt: time.UnixMilli(10),
				},
			},
		},
	}

	transform.RegisterAdditionalFuncs()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink{
				conf:    test.conf,
				tagEval: make(map[string]string),
			}
			if test.transforms.dataTemplate != "" {
				ifsink.hasTransform = true
			}
			if test.transforms.dataField != "" {
				ifsink.conf.DataField = test.transforms.dataField
			}
			if test.transforms.fields != nil {
				ifsink.conf.Fields = test.transforms.fields
			}
			contextLogger := conf.Log.WithField("rule", test.name)
			ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
			tf, _ := transform.GenTransform(test.transforms.dataTemplate, "json", "", "", test.transforms.dataField, nil)
			vCtx := context.WithValue(ctx, context.TransKey, tf)
			err := ifsink.parseTemplates(vCtx)
			assert.NoError(t, err)
			err = ifsink.transformPoints(vCtx, test.data)
			assert.NoError(t, err)
			result, err := client.NewBatchPoints(client.BatchPointsConfig{
				Database:  test.conf.Database,
				Precision: test.conf.PrecisionStr,
			})
			assert.NoError(t, err)
			for _, p := range test.points {
				pt, err := client.NewPoint(test.conf.Measurement, p.tags, p.fields, p.tt)
				assert.NoError(t, err)
				result.AddPoint(pt)
			}
			assert.Equal(t, result, ifsink.bp)
		})
	}
}
