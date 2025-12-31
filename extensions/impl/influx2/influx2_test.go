// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"reflect"
	"testing"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/tspoint"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
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
				"fields":      []interface{}{"temperature"},
				"tsFieldName": "ts",
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
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag": "value",
					},
					TsFieldName:  "ts",
					PrecisionStr: "ms",
					Fields:       []string{"temperature"},
				},
				BatchSize: 1,
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
			name: "no error",
			conf: map[string]interface{}{
				"addr":            "http://192.168.0.3:8086",
				"org":             "abc",
				"bucket":          "bucket_one",
				"precision":       "ns",
				"useLineProtocol": true,
			},
			expected: c{
				Addr:            "http://192.168.0.3:8086",
				Org:             "abc",
				Bucket:          "bucket_one",
				PrecisionStr:    "ns",
				Precision:       time.Nanosecond,
				UseLineProtocol: true,
				WriteOptions: tspoint.WriteOptions{
					PrecisionStr: "ns",
				},
				BatchSize: 1,
			},
		},
		{
			name: "measurement missing error",
			conf: map[string]interface{}{
				"addr":            "http://192.168.0.3:8086",
				"org":             "abc",
				"bucket":          "bucket_one",
				"precision":       "ns",
				"useLineProtocol": false,
			},
			error: "measurement is required",
		},
		{
			name: "unmarshall error for tls",
			conf: map[string]interface{}{
				"addr":        "http://192.168.0.3:8086",
				"org":         "abc",
				"bucket":      "bucket_one",
				"precision":   "ns",
				"measurement": "mm",
				"rootCaPath":  12,
			},
			error: "error configuring tls: 1 error(s) decoding:\n\n* 'rootCaPath' expected type 'string', got unconvertible type 'int', value: '12'",
		},
	}
	ctx := mockContext.NewMockContext("testconfig", "op")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{}
			err := ifsink.Provision(ctx, test.conf)
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
	timex.Set(10)
	tests := []struct {
		name   string
		conf   c
		data   any
		result []*write.Point
	}{
		{
			name: "normal",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
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
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					PrecisionStr: "s",
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
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "{{.humidity}}",
						"tag2": "value2",
					},
					PrecisionStr: "s",
					TsFieldName:  "ts",
				},
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
			result: []*write.Point{
				client.NewPoint("test3", map[string]string{
					"tag1": "50",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 20,
					"humidity":    50,
					"ts":          100,
				}, time.Unix(100, 0)),
				client.NewPoint("test3", map[string]string{
					"tag1": "60",
					"tag2": "value2",
				}, map[string]any{
					"temperature": 30,
					"humidity":    60,
					"ts":          110,
				}, time.Unix(110, 0)),
			},
		},
		{
			name: "batch/sendSingle with dataTemplate",
			conf: c{
				Measurement: "test4",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					PrecisionStr: "us",
					TsFieldName:  "ts",
				},
			},
			data: []map[string]any{
				{
					"t":  20,
					"ts": 100,
				},
				{
					"t":  30,
					"ts": 110,
				},
			},
			result: []*write.Point{
				client.NewPoint("test4", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"t":  20,
					"ts": 100,
				}, time.UnixMicro(100)),
				client.NewPoint("test4", map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				}, map[string]any{
					"t":  30,
					"ts": 110,
				}, time.UnixMicro(110)),
			},
		},
		{
			name: "single with fields",
			conf: c{
				Measurement: "test5",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "{{.humidity}}",
					},
					PrecisionStr: "ns",
					TsFieldName:  "ts",
				},
			},
			data: map[string]any{
				"humidity": 50,
				"ts":       100,
			},
			result: []*write.Point{
				client.NewPoint("test5", map[string]string{
					"tag1": "value1",
					"tag2": "50",
				}, map[string]any{
					"humidity": 50,
					"ts":       100,
				}, time.Unix(0, 100)),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
			}
			ctx := mockContext.NewMockContext(test.name, "op")
			points, err := ifsink.transformPoints(ctx, test.data)
			assert.NoError(t, err)
			assert.Equal(t, test.result, points)
		})
	}
}

func TestCollectPointsError(t *testing.T) {
	tests := []struct {
		name string
		conf c
		data any
		err  string
	}{
		{
			name: "unsupported data",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
				},
			},
			data: []byte{1, 2, 3},
			err:  "sink needs map or []map, but receive unsupported data [1 2 3]",
		},
		{
			name: "single without ts field",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					TsFieldName: "ts",
				},
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			err: "time field ts not found",
		},
		{
			name: "normal batch with incorrect ts field",
			conf: c{
				Measurement: "test2",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					PrecisionStr: "s",
					TsFieldName:  "ts",
				},
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
					"ts":          "add",
				},
				{
					"temperature": 30,
					"humidity":    60,
					"ts":          "ddd",
				},
			},
			err: "time field ts can not convert to timestamp(int64) : add",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
			}
			ctx := mockContext.NewMockContext(test.name, "op")
			_, err := ifsink.transformPoints(ctx, test.data)
			assert.Error(t, err)
			assert.Equal(t, test.err, err.Error())
		})
	}
}

// Do not test for multiple tags and data to avoid order problem
func TestCollectLines(t *testing.T) {
	timex.Set(10)
	tests := []struct {
		name    string
		conf    c
		data    any
		result  []string
		result2 []string
	}{
		{
			name: "normal",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
					},
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
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag2": "value2",
					},
					PrecisionStr: "ns",
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
			result: []string{"test2,tag2=value2 temperature=20 10000000", "test2,tag2=value2 humidity=60 10000000"},
		},
		{
			name: "normal batch sendSingle",
			conf: c{
				Measurement: "test3",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
					},
					PrecisionStr: "us",
				},
			},
			data: []map[string]any{
				{
					"humidity": 50,
				},
				{
					"temperature": 30,
				},
			},
			result: []string{"test3,tag1=value1 humidity=50 10000", "test3,tag1=value1 temperature=30 10000"},
		},
		{
			name: "single with fields",
			conf: c{
				Measurement: "test5",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag2": "{{.humidity}}",
					},
					PrecisionStr: "s",
					TsFieldName:  "ts",
				},
			},
			data: map[string]any{
				"humidity": 50,
				"ts":       100,
			},
			result:  []string{"test5,tag2=50 humidity=50,ts=100 100"},
			result2: []string{"test5,tag2=50 ts=100,humidity=50 100"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
			}
			ctx := mockContext.NewMockContext(test.name, "op")
			lines, err := ifsink.transformLines(ctx, test.data)
			assert.NoError(t, err)
			if test.result2 == nil {
				assert.Equal(t, test.result, lines)
			} else {
				assert.True(t, reflect.DeepEqual(test.result, lines) || reflect.DeepEqual(test.result2, lines))
			}
		})
	}
}

func TestCollectLinesError(t *testing.T) {
	tests := []struct {
		name string
		conf c
		data any
		err  string
	}{
		{
			name: "unsupported data",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
				},
			},
			data: []byte{1, 2, 3},
			err:  "sink needs map or []map, but receive unsupported data [1 2 3]",
		},
		{
			name: "single wrong ts format",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
					},
					TsFieldName: "name",
				},
				UseLineProtocol: true,
			},
			data: map[string]any{
				"name": "home",
			},
			err: "time field name can not convert to timestamp(int64) : home",
		},
		{
			name: "batch wront ts field",
			conf: c{
				Measurement: "test2",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag2": "value2",
					},
					PrecisionStr: "ns",
					TsFieldName:  "ts",
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
			err: "time field ts not found",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink2{
				conf: test.conf,
			}
			ctx := mockContext.NewMockContext(test.name, "op")
			_, err := ifsink.transformLines(ctx, test.data)
			assert.Error(t, err)
			assert.Equal(t, test.err, err.Error())
		})
	}
}
