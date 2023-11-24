// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package influx2

import (
	"testing"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
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
				"token":       "Token_test",
				"measurement": "test",
				"org":         "admin",
				"bucket":      "bucket_one",
				"tags": map[string]interface{}{
					"tag": "value",
				},
				"fields": []interface{}{"temperature"},
			},
			expected: c{
				Addr:            "http://192.168.0.3:8086",
				Token:           "Token_test",
				Org:             "admin",
				Bucket:          "bucket_one",
				PrecisionStr:    "ms",
				Precision:       time.Millisecond,
				UseLineProtocol: false,
				Measurement:     "test",
				Tags: map[string]string{
					"tag": "value",
				},
				Fields: []string{"temperature"},
			},
		},
		{
			name: "unmarshall error",
			conf: map[string]interface{}{
				"org": 12,
			},
			error: "error configuring influx2 sink: 1 error(s) decoding:\n\n* 'org' expected type 'string', got unconvertible type 'int', value: '12'",
		},
		{
			name:  "addr missing error",
			conf:  map[string]interface{}{},
			error: "addr is required",
		},
		{
			name: "org missing error",
			conf: map[string]interface{}{
				"addr": "http://192.168.0.3:8086",
			},
			error: "org is required",
		},
		{
			name: "bucket missing error",
			conf: map[string]interface{}{
				"addr": "http://192.168.0.3:8086",
				"org":  "abc",
			},
			error: "bucket is required",
		},
		{
			name: "precision invalid error",
			conf: map[string]interface{}{
				"addr":      "http://192.168.0.3:8086",
				"org":       "abc",
				"bucket":    "bucket_one",
				"precision": "abc",
			},
			error: "precision abc is not supported",
		},
		{
			name: "measurement missing error",
			conf: map[string]interface{}{
				"addr":      "http://192.168.0.3:8086",
				"org":       "abc",
				"bucket":    "bucket_one",
				"precision": "ns",
			},
			error: "measurement is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{}
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
		result []*write.Point
	}{
		{
			name: "normal",
			conf: c{
				Measurement: "test1",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			result: []*write.Point{
				client.NewPoint("test1", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 20,
					"humidity":    50,
				}, time.UnixMilli(10)),
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
			result: []*write.Point{
				client.NewPoint("test2", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 20,
					"humidity":    50,
				}, time.UnixMilli(10)),
				client.NewPoint("test2", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 30,
					"humidity":    60,
				}, time.UnixMilli(10)),
			},
		},
		{
			name: "normal batch sendSingle",
			conf: c{
				Measurement: "test3",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				SendSingle: true,
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
			result: []*write.Point{
				client.NewPoint("test3", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 20,
					"humidity":    50,
				}, time.UnixMilli(10)),
				client.NewPoint("test3", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 30,
					"humidity":    60,
				}, time.UnixMilli(10)),
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
				SendSingle: true,
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
				},
				{
					"temperature": 30,
					"humidity":    60,
				},
			},
			result: []*write.Point{
				client.NewPoint("test4", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"t": 20.0,
				}, time.UnixMilli(10)),
				client.NewPoint("test4", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"t": 30.0,
				}, time.UnixMilli(10)),
			},
		},
		{
			name: "single with fields",
			conf: c{
				Measurement: "test5",
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
				fields: []string{"humidity"},
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			result: []*write.Point{
				client.NewPoint("test5", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"humidity": 50,
				}, time.UnixMilli(10)),
			},
		},
		{
			name: "single with dataTemplate and dataField",
			conf: c{
				Measurement: "test5",
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
				dataTemplate: `{"payload":{"t":{{.temperature}},"h":{{.humidity}}}}`,
				dataField:    "payload",
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			result: []*write.Point{
				client.NewPoint("test5", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"t": 20.0,
					"h": 50.0,
				}, time.UnixMilli(10)),
			},
		},
		{
			name: "batch with fields",
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
			result: []*write.Point{
				client.NewPoint("test6", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"humidity": 50,
				}, time.UnixMilli(10)),
				client.NewPoint("test6", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"humidity": 60,
				}, time.UnixMilli(10)),
			},
		},
		{
			name: "batch with dataTemplate of single output",
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
			result: []*write.Point{
				client.NewPoint("test6", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 20.0,
				}, time.UnixMilli(10)),
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
			result: []*write.Point{
				client.NewPoint("test6", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 20.0,
				}, time.UnixMilli(10)),
				client.NewPoint("test6", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 30.0,
				}, time.UnixMilli(10)),
			},
		},
	}

	transform.RegisterAdditionalFuncs()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
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
			tf, _ := transform.GenTransform(test.transforms.dataTemplate, "json", "", "", test.transforms.dataField, test.transforms.fields)
			vCtx := context.WithValue(ctx, context.TransKey, tf)
			points, err := ifsink.transformPoints(vCtx, test.data)
			assert.NoError(t, err)
			assert.Equal(t, test.result, points)
		})
	}
}

func TestCollectPointsError(t *testing.T) {
	tests := []struct {
		name       string
		conf       c
		data       any
		transforms struct {
			dataTemplate string
			dataField    string
			fields       []string
		}
		err string
	}{
		{
			name: "unsupported data",
			conf: c{
				Measurement: "test1",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
			},
			data: []byte{1, 2, 3},
			err:  "influx2 sink needs map or []map, but receive unsupported data [1 2 3]",
		},
		{
			name: "transform error",
			conf: c{
				Measurement: "test4",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				SendSingle: true,
			},
			transforms: struct {
				dataTemplate string
				dataField    string
				fields       []string
			}{
				dataTemplate: `{"t":{{.temperatureHigh}}}`,
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
			err: "fail to decode data {\"t\":<no value>} after applying dataTemplate for error invalid character '<' looking for beginning of value",
		},
		{
			name: "unmarshall after transform error",
			conf: c{
				Measurement: "test5",
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
				dataTemplate: `"t":{{.temperature}}}`,
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			err: "fail to decode data \"t\":<no value>} after applying dataTemplate for error invalid character ':' after top-level value",
		},
		{
			name: "batch with transform unmarshall error",
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
				dataTemplate: `abc{{json (index . 0)}}`,
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
			err: "fail to TransItem data [map[humidity:50 temperature:20] map[humidity:60 temperature:30]] for error fail to decode data abc{\"humidity\":50,\"temperature\":20} for error invalid character 'a' looking for beginning of value",
		},
	}

	transform.RegisterAdditionalFuncs()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
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
			tf, _ := transform.GenTransform(test.transforms.dataTemplate, "json", "", "", test.transforms.dataField, test.transforms.fields)
			vCtx := context.WithValue(ctx, context.TransKey, tf)
			_, err := ifsink.transformPoints(vCtx, test.data)
			assert.Error(t, err)
			assert.Equal(t, test.err, err.Error())
		})
	}
}

// Do not test for multiple tags and data to avoid order problem
func TestCollectLines(t *testing.T) {
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
		result []string
	}{
		{
			name: "normal",
			conf: c{
				Measurement: "test1",
				Tags: map[string]string{
					"tag1": "value1",
				},
				UseLineProtocol: true,
			},
			data: map[string]any{
				"name": "home",
			},
			result: []string{"test1,tag1=value1 name=\"home\" 10"},
		},
		{
			name: "normal batch",
			conf: c{
				Measurement: "test2",
				Tags: map[string]string{
					"tag2": "value2",
				},
			},
			data: []map[string]any{
				{
					"temperature": 20,
				},
				{
					"humidity": 60,
				},
			},
			result: []string{"test2,tag2=value2 temperature=20 10", "test2,tag2=value2 humidity=60 10"},
		},
		{
			name: "normal batch sendSingle",
			conf: c{
				Measurement: "test3",
				Tags: map[string]string{
					"tag1": "value1",
				},
				SendSingle: true,
			},
			data: []map[string]any{
				{
					"humidity": 50,
				},
				{
					"temperature": 30,
				},
			},
			result: []string{"test3,tag1=value1 humidity=50 10", "test3,tag1=value1 temperature=30 10"},
		},
		{
			name: "batch/sendSingle with dataTemplate",
			conf: c{
				Measurement: "test4",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				SendSingle: true,
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
				},
				{
					"temperature": 30,
					"humidity":    60,
				},
			},
			result: []string{"{\"t\":20}", "{\"t\":30}"}, // no validation now
		},
		{
			name: "single with fields",
			conf: c{
				Measurement: "test5",
				Tags: map[string]string{
					"tag2": "value2",
				},
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
			},
			result: []string{"test5,tag2=value2 humidity=50 10"},
		},
		{
			name: "batch with fields",
			conf: c{
				Measurement: "test6",
				Tags: map[string]string{
					"tag1": "value1",
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
			result: []string{"test6,tag1=value1 humidity=50 10", "test6,tag1=value1 humidity=60 10"},
		},
	}

	transform.RegisterAdditionalFuncs()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
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
			tf, _ := transform.GenTransform(test.transforms.dataTemplate, "json", "", "", test.transforms.dataField, test.transforms.fields)
			vCtx := context.WithValue(ctx, context.TransKey, tf)
			lines, err := ifsink.transformLines(vCtx, test.data)
			assert.NoError(t, err)
			assert.Equal(t, test.result, lines)
		})
	}
}

func TestCollectLinesError(t *testing.T) {
	tests := []struct {
		name       string
		conf       c
		data       any
		transforms struct {
			dataTemplate string
			dataField    string
			fields       []string
		}
		err string
	}{
		{
			name: "unsupported data",
			conf: c{
				Measurement: "test1",
				Tags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
			},
			data: []byte{1, 2, 3},
			err:  "influx2 sink needs map or []map, but receive unsupported data [1 2 3]",
		},
	}

	transform.RegisterAdditionalFuncs()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
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
			tf, _ := transform.GenTransform(test.transforms.dataTemplate, "json", "", "", test.transforms.dataField, test.transforms.fields)
			vCtx := context.WithValue(ctx, context.TransKey, tf)
			_, err := ifsink.transformLines(vCtx, test.data)
			assert.Error(t, err)
			assert.Equal(t, test.err, err.Error())
		})
	}
}
