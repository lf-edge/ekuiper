// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package edgex

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	v4 "github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

var (
	contextLogger = conf.Log.WithField("rule", "testEdgexSink")
	ctx           = context.WithValue(context.Background(), context.LoggerKey, contextLogger)
)

func compareEvent(expected, actual *dtos.Event) bool {
	if (expected.Id == actual.Id || (expected.Id == "" && actual.Id != "")) && expected.ProfileName == actual.ProfileName && expected.DeviceName == actual.DeviceName && (expected.Origin == actual.Origin || (expected.Origin == 0 && actual.Origin > 0)) && reflect.DeepEqual(expected.Tags, actual.Tags) && expected.SourceName == actual.SourceName && len(expected.Readings) == len(actual.Readings) {
		for _, r := range expected.Readings {
			compared := false
			for _, a := range actual.Readings {
				if r.ResourceName == a.ResourceName {
					compared = true
					if !compareReading(r, a) {
						return false
					}
				}
			}
			if !compared {
				return false
			}
		}
		return true
	}
	return false
}

func compareReading(expected, actual dtos.BaseReading) bool {
	if (expected.Id == actual.Id || (expected.Id == "" && actual.Id != "")) && expected.ProfileName == actual.ProfileName && expected.DeviceName == actual.DeviceName && (expected.Origin == actual.Origin || (expected.Origin == 0 && actual.Origin > 0)) && expected.ResourceName == actual.ResourceName && expected.Value == actual.Value && expected.ValueType == actual.ValueType {
		if expected.ValueType == v4.ValueTypeObject {
			if !reflect.DeepEqual(expected.ObjectValue, actual.ObjectValue) {
				return false
			}
		}
		return true
	}
	return false
}

func TestConfigure(t *testing.T) {
	tests := []struct {
		conf     map[string]interface{}
		expected *SinkConf
		error    string
	}{
		{ // 0
			conf: map[string]interface{}{
				"metadata": "meta",
			},
			expected: &SinkConf{
				MessageType: MessageTypeEvent,
				ContentType: "application/json",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				Metadata:    "meta",
			},
		},
		{ // 1
			conf: map[string]interface{}{
				"type":        "redis",
				"protocol":    "redis",
				"host":        "edgex-redis",
				"port":        6379,
				"topic":       "ekuiperResult",
				"deviceName":  "ekuiper",
				"profileName": "ekuiper",
				"sourceName":  "ekuiper",
				"contentType": "application/json",
			},
			expected: &SinkConf{
				MessageType: MessageTypeEvent,
				ContentType: "application/json",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiper",
				SourceName:  "ekuiper",
				Topic:       "ekuiperResult",
			},
		},
		{ // 2
			conf: map[string]interface{}{
				"protocol":    "tcp",
				"host":        "127.0.0.1",
				"port":        1883,
				"topic":       "result",
				"type":        "mqtt",
				"metadata":    "edgex_meta",
				"contentType": "application/json",
				"optional": map[string]interface{}{
					"ClientId": "edgex_message_bus_001",
				},
			},
			expected: &SinkConf{
				MessageType: MessageTypeEvent,
				ContentType: "application/json",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "",
				Metadata:    "edgex_meta",
				Topic:       "result",
			},
		},
		{ // 3
			conf: map[string]interface{}{
				"type":        "redis",
				"protocol":    "redis",
				"host":        "edgex-redis",
				"port":        6379,
				"topicPrefix": "edgex/events/device",
				"messageType": "request",
				"contentType": "application/json",
			},
			expected: &SinkConf{
				MessageType: MessageTypeRequest,
				ContentType: "application/json",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "",
				TopicPrefix: "edgex/events/device",
			},
		},
		{ // 4
			conf: map[string]interface{}{
				"type":        "redis",
				"protocol":    "redis",
				"host":        "edgex-redis",
				"port":        6379,
				"topicPrefix": "edgex/events/device",
				"messageType": "requests",
				"contentType": "application/json",
			},
			error: "specified wrong messageType value requests",
		},
		{ // 5
			conf: map[string]interface{}{
				"protocol":    "redis",
				"host":        "edgex-redis",
				"port":        6379,
				"topicPrefix": "edgex/events/device",
				"topic":       "requests",
				"contentType": "application/json",
			},
			error: "not allow to specify both topic and topicPrefix, please set one only",
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			ems := EdgexMsgBusSink{}
			err := ems.Provision(ctx, test.conf)
			if !reflect.DeepEqual(test.error, testx.Errstring(err)) {
				t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.error, err)
			} else if test.error == "" {
				assert.Equal(t, test.expected, ems.c)
				assert.Equal(t, map[string]any{
					"contentType": "application/json",
				}, ems.sendParams)
			}
		})
	}
}

func TestProduceEvents(t1 *testing.T) {
	tests := []struct {
		input    string
		conf     map[string]interface{}
		expected *dtos.Event
		error    string
	}{
		{ // 0
			input: `[
						{"meta":{
							"correlationid":"","deviceName":"demo","id":"","origin":3,
							"humidity":{"deviceName":"test device name1","id":"12","origin":14,"valueType":"Int64"},
							"temperature":{"deviceName":"test device name2","id":"22","origin":24}
							}
						},
						{"humidity":100},
						{"temperature":50}
					]`,
			conf: map[string]interface{}{
				"metadata": "meta",
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "demo",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Origin:      3,
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "humidity",
						DeviceName:    "test device name1",
						ProfileName:   "ekuiperProfile",
						Id:            "12",
						Origin:        14,
						ValueType:     v4.ValueTypeInt64,
						SimpleReading: dtos.SimpleReading{Value: "100"},
					},
					{
						ResourceName:  "temperature",
						DeviceName:    "test device name2",
						ProfileName:   "ekuiperProfile",
						Id:            "22",
						Origin:        24,
						ValueType:     v4.ValueTypeFloat64,
						SimpleReading: dtos.SimpleReading{Value: "5e+01"},
					},
				},
			},
			error: "",
		},

		{ // 1
			input: `[
						{"meta":{
							"correlationid":"","profileName":"demoProfile","deviceName":"demo","sourceName":"demoSource","id":"abc","origin":3,"tags":{"auth":"admin"},
							"humidity":{"deviceName":"test device name1","id":"12","origin":14},
							"temperature":{"deviceName":"test device name2","id":"22","origin":24}
							}
						},
						{"h1":100},
						{"h2":null}
					]`,
			conf: map[string]interface{}{
				"metadata": "meta",
			},
			expected: &dtos.Event{
				Id:          "abc",
				DeviceName:  "demo",
				ProfileName: "demoProfile",
				SourceName:  "demoSource",
				Origin:      3,
				Tags:        map[string]interface{}{"auth": "admin"},
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "h1",
						SimpleReading: dtos.SimpleReading{Value: "1e+02"},
						DeviceName:    "demo",
						ProfileName:   "demoProfile",
						ValueType:     v4.ValueTypeFloat64,
					},
				},
			},
			error: "",
		},

		{ // 2
			input: `[
						{"meta": 50,"h1":100}
					]`,
			conf: map[string]interface{}{
				"sourceName": "demo",
			},
			expected: &dtos.Event{
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "demo",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "meta",
						SimpleReading: dtos.SimpleReading{Value: "5e+01"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat64,
					},
					{
						ResourceName:  "h1",
						SimpleReading: dtos.SimpleReading{Value: "1e+02"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat64,
					},
				},
			},
			error: "",
		},

		{ // 3
			input: `[
						{"meta1": "newmeta"},
						{"h1":true},
						{"sa":["1","2","3","4"]},
						{"fa":[1.1,2.2,3.3,4.4]}
					]`,
			expected: &dtos.Event{
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "meta1",
						SimpleReading: dtos.SimpleReading{Value: "newmeta"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeString,
					},
					{
						ResourceName:  "h1",
						SimpleReading: dtos.SimpleReading{Value: "true"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeBool,
					},
					{
						ResourceName:  "sa",
						SimpleReading: dtos.SimpleReading{Value: "[1, 2, 3, 4]"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeStringArray,
					},
					{
						ResourceName:  "fa",
						SimpleReading: dtos.SimpleReading{Value: "[1.1e+00, 2.2e+00, 3.3e+00, 4.4e+00]"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat64Array,
					},
				},
			},
			error: "",
		},

		{ // 4
			input: `[]`,
			conf: map[string]interface{}{
				"deviceName":  "kuiper",
				"profileName": "kp",
				"topic":       "demo",
			},
			expected: &dtos.Event{
				ProfileName: "kp",
				DeviceName:  "kuiper",
				SourceName:  "ruleTest",
				Origin:      0,
				Readings:    nil,
			},
			error: "",
		},
		{ // 5
			input: `[{"sa":["1","2",3,"4"]}]`, // invalid array, return nil
			expected: &dtos.Event{
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Origin:      0,
				Readings:    nil,
			},
		},
		{ // 6
			input: `[
						{"meta1": "newmeta"},
						{"sa":"SGVsbG8gV29ybGQ="},
						{"meta":{
							"correlationid":"","profileName":"demoProfile","deviceName":"demo","id":"abc","origin":3,"tags":{"auth":"admin"},
							"sa":{"deviceName":"test device name1","id":"12","origin":14, "valueType":"Binary","mediaType":"application/css"}
						}}
					]`,
			conf: map[string]interface{}{
				"metadata":    "meta",
				"profileName": "myprofile",
				"sourceName":  "ds",
			},
			expected: &dtos.Event{
				DeviceName:  "demo",
				ProfileName: "demoProfile",
				SourceName:  "ds",
				Origin:      3,
				Tags:        map[string]interface{}{"auth": "admin"},
				Readings: []dtos.BaseReading{
					{
						DeviceName:    "demo",
						ProfileName:   "demoProfile",
						ResourceName:  "meta1",
						SimpleReading: dtos.SimpleReading{Value: "newmeta"},
						ValueType:     v4.ValueTypeString,
					},
					{
						ResourceName:  "sa",
						BinaryReading: dtos.BinaryReading{BinaryValue: []byte("Hello World"), MediaType: "application/css"},
						ProfileName:   "demoProfile",
						DeviceName:    "test device name1",
						Id:            "12",
						Origin:        14,
						ValueType:     v4.ValueTypeBinary,
					},
				},
			},
			error: "",
		},
		{ // 7
			input: `[
						{"meta":{
							"correlationid":"","deviceName":"demo","id":"","origin":3,
							"obj":{"deviceName":"test device name1","id":"12","origin":14,"valueType":"Object"}
							}
						},
						{"obj":{"a":1,"b":"sttt"}}
					]`,
			conf: map[string]interface{}{
				"metadata": "meta",
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "demo",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Origin:      3,
				Readings: []dtos.BaseReading{
					{
						ResourceName: "obj",
						DeviceName:   "test device name1",
						ProfileName:  "ekuiperProfile",
						Id:           "12",
						Origin:       14,
						ValueType:    v4.ValueTypeObject,
						ObjectReading: dtos.ObjectReading{ObjectValue: map[string]interface{}{
							"a": float64(1),
							"b": "sttt",
						}},
					},
				},
			},
			error: "",
		},
		{ // 8
			input: `[
						{"obj":{"a":1,"b":"sttt"}}
					]`,
			conf: map[string]interface{}{},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Origin:      0,
				Readings: []dtos.BaseReading{
					{
						ResourceName: "obj",
						DeviceName:   "ekuiper",
						ProfileName:  "ekuiperProfile",
						Id:           "",
						Origin:       0,
						ValueType:    v4.ValueTypeObject,
						ObjectReading: dtos.ObjectReading{ObjectValue: map[string]interface{}{
							"a": float64(1),
							"b": "sttt",
						}},
					},
				},
			},
			error: "",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, t := range tests {
		ems := EdgexMsgBusSink{}
		err := ems.Provision(ctx, t.conf)
		if err != nil {
			t1.Errorf("%d: configure error %v", i, err)
			continue
		}
		if ems.c.SourceName == "" {
			ems.c.SourceName = "ruleTest"
		}
		var payload []map[string]interface{}
		json.Unmarshal([]byte(t.input), &payload)
		result, err := ems.produceEvents(ctx, payload)
		if !reflect.DeepEqual(t.error, testx.Errstring(err)) {
			t1.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, t.input, t.error, err)
		} else if t.error == "" && !compareEvent(t.expected, result) {
			t1.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, t.input, t.expected, result)
		}
	}
}

func TestReadingTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected *dtos.Event
	}{
		{ // 0
			name: "primitive",
			input: map[string]any{
				"auint8":   uint8(1),
				"auint16":  uint16(1),
				"auint32":  uint32(1),
				"auint64":  uint64(1),
				"auint":    uint(1),
				"aint8":    int8(1),
				"aint16":   int16(1),
				"aint32":   int32(1),
				"afloat32": float32(1.0),
				"bin":      []byte("byte"),
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "auint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint8,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "auint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint16,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "auint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint32,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "auint64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint64,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "auint",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint64,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "aint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt8,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "aint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt16,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "aint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt32,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "afloat32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat32,
						SimpleReading: dtos.SimpleReading{Value: "1e+00"},
					},
					{
						ResourceName:  "bin",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeBinary,
						BinaryReading: dtos.BinaryReading{BinaryValue: []byte("byte"), MediaType: "application/text"},
					},
				},
			},
		},
		{ // 0
			name: "slice",
			input: map[string]any{
				"abool":    []any{true},
				"auint8":   []any{uint8(1)},
				"auint16":  []any{uint16(1)},
				"auint32":  []any{uint32(1)},
				"auint64":  []any{uint64(1)},
				"auint":    []any{uint(1)},
				"aint8":    []any{int8(1)},
				"aint16":   []any{int16(1)},
				"aint32":   []any{int32(1)},
				"aint64":   []any{int64(1)},
				"aint":     []any{1},
				"afloat32": []any{float32(1.0)},
				"astring":  []any{"test"},
				"invalid":  []any{true, "test", true},
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "abool",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeBoolArray,
						SimpleReading: dtos.SimpleReading{Value: "[true]"},
					},
					{
						ResourceName:  "auint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint8Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "auint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint16Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "auint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint32Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "auint64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint64Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "auint",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint64Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt8Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt16Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt32Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt64Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt64Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "afloat32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat32Array,
						SimpleReading: dtos.SimpleReading{Value: "[1e+00]"},
					},
					{
						ResourceName:  "astring",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeStringArray,
						SimpleReading: dtos.SimpleReading{Value: "[test]"},
					},
				},
			},
		},
		{
			name: "primitive with meta",
			input: map[string]any{
				"meta": map[string]any{
					"abool":    map[string]any{"valueType": v4.ValueTypeBool},
					"astr":     map[string]any{"valueType": v4.ValueTypeString},
					"auint8":   map[string]any{"valueType": v4.ValueTypeUint8},
					"auint16":  map[string]any{"valueType": v4.ValueTypeUint16},
					"auint32":  map[string]any{"valueType": v4.ValueTypeUint32},
					"auint64":  map[string]any{"valueType": v4.ValueTypeUint64},
					"aint8":    map[string]any{"valueType": v4.ValueTypeInt8},
					"aint16":   map[string]any{"valueType": v4.ValueTypeInt16},
					"aint32":   map[string]any{"valueType": v4.ValueTypeInt32},
					"aint64":   map[string]any{"valueType": v4.ValueTypeInt64},
					"afloat32": map[string]any{"valueType": v4.ValueTypeFloat32},
					"afloat64": map[string]any{"valueType": v4.ValueTypeFloat64},
					"bin":      map[string]any{"valueType": v4.ValueTypeBinary},
				},
				"abool":    false,
				"astr":     "hello",
				"auint8":   1,
				"auint16":  1,
				"auint32":  1,
				"auint64":  1,
				"aint8":    1,
				"aint16":   1,
				"aint32":   1,
				"aint64":   1,
				"afloat32": 1,
				"afloat64": 1,
				"bin":      "byte",
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "abool",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeBool,
						SimpleReading: dtos.SimpleReading{Value: "false"},
					},
					{
						ResourceName:  "astr",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeString,
						SimpleReading: dtos.SimpleReading{Value: "hello"},
					},
					{
						ResourceName:  "auint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint8,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "auint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint16,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "auint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint32,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "auint64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint64,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "aint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt8,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "aint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt16,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "aint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt32,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "aint64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt64,
						SimpleReading: dtos.SimpleReading{Value: "1"},
					},
					{
						ResourceName:  "afloat32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat32,
						SimpleReading: dtos.SimpleReading{Value: "1e+00"},
					},
					{
						ResourceName:  "afloat64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat64,
						SimpleReading: dtos.SimpleReading{Value: "1e+00"},
					},
					{
						ResourceName:  "bin",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeBinary,
						BinaryReading: dtos.BinaryReading{BinaryValue: []byte("byte"), MediaType: "application/text"},
					},
				},
			},
		},
		{
			name: "slice with meta",
			input: map[string]any{
				"meta": map[string]any{
					"abool":    map[string]any{"valueType": v4.ValueTypeBoolArray},
					"astring":  map[string]any{"valueType": v4.ValueTypeStringArray},
					"auint8":   map[string]any{"valueType": v4.ValueTypeUint8Array},
					"auint16":  map[string]any{"valueType": v4.ValueTypeUint16Array},
					"auint32":  map[string]any{"valueType": v4.ValueTypeUint32Array},
					"auint64":  map[string]any{"valueType": v4.ValueTypeUint64Array},
					"aint8":    map[string]any{"valueType": v4.ValueTypeInt8Array},
					"aint16":   map[string]any{"valueType": v4.ValueTypeInt16Array},
					"aint32":   map[string]any{"valueType": v4.ValueTypeInt32Array},
					"aint64":   map[string]any{"valueType": v4.ValueTypeInt64Array},
					"afloat32": map[string]any{"valueType": v4.ValueTypeFloat32Array},
					"afloat64": map[string]any{"valueType": v4.ValueTypeFloat64Array},
				},
				"abool":    []any{true},
				"auint8":   []any{1},
				"auint16":  []any{1},
				"auint32":  []any{1},
				"auint64":  []any{1},
				"aint8":    []any{1},
				"aint16":   []any{1},
				"aint32":   []any{1},
				"aint64":   []any{1},
				"afloat32": []any{1},
				"afloat64": []any{1},
				"astring":  []any{"test"},
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "abool",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeBoolArray,
						SimpleReading: dtos.SimpleReading{Value: "[true]"},
					},
					{
						ResourceName:  "auint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint8Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "auint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint16Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "auint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint32Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "auint64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeUint64Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint8",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt8Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint16",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt16Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt32Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "aint64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeInt64Array,
						SimpleReading: dtos.SimpleReading{Value: "[1]"},
					},
					{
						ResourceName:  "afloat32",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat32Array,
						SimpleReading: dtos.SimpleReading{Value: "[1e+00]"},
					},
					{
						ResourceName:  "afloat64",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeFloat64Array,
						SimpleReading: dtos.SimpleReading{Value: "[1e+00]"},
					},
					{
						ResourceName:  "astring",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v4.ValueTypeStringArray,
						SimpleReading: dtos.SimpleReading{Value: "[test]"},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ems := EdgexMsgBusSink{}
			err := ems.Provision(ctx, map[string]any{"metadata": "meta"})
			require.NoError(t, err)
			if ems.c.SourceName == "" {
				ems.c.SourceName = "ruleTest"
			}
			result, err := ems.produceEvents(ctx, test.input)
			require.NoError(t, err)
			assert.True(t, compareEvent(test.expected, result))
		})
	}
}
