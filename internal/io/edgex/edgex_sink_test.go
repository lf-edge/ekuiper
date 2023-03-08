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

//go:build edgex
// +build edgex

package edgex

import (
	"encoding/json"
	"fmt"
	v2 "github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"reflect"
	"testing"
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
		if expected.ValueType == v2.ValueTypeObject {
			if !reflect.DeepEqual(expected.ObjectValue, actual.ObjectValue) {
				return false
			}
		}
		return true
	}
	return false
}

func TestConfigure(t *testing.T) {
	var tests = []struct {
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
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, test := range tests {
		ems := EdgexMsgBusSink{}
		err := ems.Configure(test.conf)
		if !reflect.DeepEqual(test.error, testx.Errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.error, err)
		} else if test.error == "" && !reflect.DeepEqual(test.expected, ems.c) {
			t.Errorf("%d\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, test.expected, ems.c)
		}
	}
}

func TestProduceEvents(t1 *testing.T) {
	var tests = []struct {
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
						ValueType:     v2.ValueTypeInt64,
						SimpleReading: dtos.SimpleReading{Value: "100"},
					},
					{
						ResourceName:  "temperature",
						DeviceName:    "test device name2",
						ProfileName:   "ekuiperProfile",
						Id:            "22",
						Origin:        24,
						ValueType:     v2.ValueTypeFloat64,
						SimpleReading: dtos.SimpleReading{Value: "5.000000e+01"},
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
						SimpleReading: dtos.SimpleReading{Value: "1.000000e+02"},
						DeviceName:    "demo",
						ProfileName:   "demoProfile",
						ValueType:     v2.ValueTypeFloat64,
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
						SimpleReading: dtos.SimpleReading{Value: "5.000000e+01"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v2.ValueTypeFloat64,
					},
					{
						ResourceName:  "h1",
						SimpleReading: dtos.SimpleReading{Value: "1.000000e+02"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v2.ValueTypeFloat64,
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
						ValueType:     v2.ValueTypeString,
					},
					{
						ResourceName:  "h1",
						SimpleReading: dtos.SimpleReading{Value: "true"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v2.ValueTypeBool,
					},
					{
						ResourceName:  "sa",
						SimpleReading: dtos.SimpleReading{Value: "[1, 2, 3, 4]"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v2.ValueTypeStringArray,
					},
					{
						ResourceName:  "fa",
						SimpleReading: dtos.SimpleReading{Value: "[1.100000e+00, 2.200000e+00, 3.300000e+00, 4.400000e+00]"},
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						ValueType:     v2.ValueTypeFloat64Array,
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
			input: `[{"sa":["1","2",3,"4"]}]`, //invalid array, return nil
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
						ValueType:     v2.ValueTypeString,
					},
					{
						ResourceName:  "sa",
						BinaryReading: dtos.BinaryReading{BinaryValue: []byte("Hello World"), MediaType: "application/css"},
						ProfileName:   "demoProfile",
						DeviceName:    "test device name1",
						Id:            "12",
						Origin:        14,
						ValueType:     v2.ValueTypeBinary,
					},
				},
			},
			error: "",
		}, { // 7
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
						ValueType:    v2.ValueTypeObject,
						ObjectReading: dtos.ObjectReading{ObjectValue: map[string]interface{}{
							"a": float64(1),
							"b": "sttt",
						}},
					},
				},
			},
			error: "",
		}, { // 8
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
						ValueType:    v2.ValueTypeObject,
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
		err := ems.Configure(t.conf)
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

func TestEdgeXTemplate_Apply(t1 *testing.T) {
	var tests = []struct {
		input    string
		conf     map[string]interface{}
		expected *dtos.Event
		error    string
	}{
		{ // 0
			input: `[{"meta":{
							"correlationid":"","deviceName":"demo","id":"","origin":3,
							"humidity":{"deviceName":"test device name1","id":"12","origin":14,"valueType":"Int64"},
							"temperature":{"deviceName":"test device name2","id":"22","origin":24}
							},
						"humidity":100,
						"temperature":50}
					]`,
			conf: map[string]interface{}{
				"metadata": "meta",

				"dataTemplate": `{"wrapper":"w1","ab":"{{.humidity}}"}`,
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Origin:      0,
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "wrapper",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						Id:            "",
						Origin:        0,
						ValueType:     v2.ValueTypeString,
						SimpleReading: dtos.SimpleReading{Value: "w1"},
					},
					{
						ResourceName:  "ab",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						Id:            "",
						Origin:        0,
						ValueType:     v2.ValueTypeString,
						SimpleReading: dtos.SimpleReading{Value: "100"},
					},
				},
			},
			error: "",
		}, {
			input: `[{"json":"{\"a\":24,\"b\":\"c\"}"}]`,
			conf: map[string]interface{}{
				"dataTemplate": `{{.json}}`,
			},
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "ekuiper",
				ProfileName: "ekuiperProfile",
				SourceName:  "ruleTest",
				Origin:      0,
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "a",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						Id:            "",
						Origin:        0,
						ValueType:     v2.ValueTypeFloat64,
						SimpleReading: dtos.SimpleReading{Value: "2.400000e+01"},
					},
					{
						ResourceName:  "b",
						DeviceName:    "ekuiper",
						ProfileName:   "ekuiperProfile",
						Id:            "",
						Origin:        0,
						ValueType:     v2.ValueTypeString,
						SimpleReading: dtos.SimpleReading{Value: "c"},
					},
				},
			},
			error: "",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, t := range tests {
		ems := EdgexMsgBusSink{}
		err := ems.Configure(t.conf)
		if err != nil {
			t1.Errorf("%d: configure error %v", i, err)
			continue
		}
		if ems.c.SourceName == "" {
			ems.c.SourceName = "ruleTest"
		}
		var payload []map[string]interface{}
		json.Unmarshal([]byte(t.input), &payload)
		dt := t.conf["dataTemplate"]
		tf, _ := transform.GenTransform(cast.ToStringAlways(dt), "json", "", "")
		vCtx := context.WithValue(ctx, context.TransKey, tf)
		result, err := ems.produceEvents(vCtx, payload[0])
		if !reflect.DeepEqual(t.error, testx.Errstring(err)) {
			t1.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, t.input, t.error, err)
		} else if t.error == "" && !compareEvent(t.expected, result) {
			t1.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, t.input, t.expected, result)
		}
	}
}
