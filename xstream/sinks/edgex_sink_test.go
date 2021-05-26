// +build edgex

package sinks

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/v2"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/v2/dtos"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/contexts"
	"reflect"
	"testing"
)

var (
	contextLogger = common.Log.WithField("rule", "testEdgexSink")
	ctx           = contexts.WithValue(contexts.Background(), contexts.LoggerKey, contextLogger)
)

func compareEvent(expected, actual *dtos.Event) bool {
	if (expected.Id == actual.Id || (expected.Id == "" && actual.Id != "")) && expected.ProfileName == actual.ProfileName && expected.DeviceName == actual.DeviceName && (expected.Origin == actual.Origin || (expected.Origin == 0 && actual.Origin > 0)) && reflect.DeepEqual(expected.Tags, actual.Tags) && expected.SourceName == actual.SourceName && len(expected.Readings) == len(actual.Readings) {
		for i, r := range expected.Readings {
			if !compareReading(r, actual.Readings[i]) {
				break
			}
		}
		return true
	}
	return false
}

func compareReading(expected, actual dtos.BaseReading) bool {
	if (expected.Id == actual.Id || (expected.Id == "" && actual.Id != "")) && expected.ProfileName == actual.ProfileName && expected.DeviceName == actual.DeviceName && (expected.Origin == actual.Origin || (expected.Origin == 0 && actual.Origin > 0)) && expected.ResourceName == actual.ResourceName && expected.Value == actual.Value && expected.ValueType == actual.ValueType {
		return true
	}
	return false
}

func TestProduceEvents(t1 *testing.T) {
	var tests = []struct {
		input       string
		deviceName  string
		profileName string
		topic       string
		expected    *dtos.Event
		error       string
	}{
		{
			input: `[
						{"meta":{
							"correlationid":"","deviceName":"demo","id":"","origin":3,
							"humidity":{"deviceName":"test device name1","id":"12","origin":14,"valueType":"int64"},
							"temperature":{"deviceName":"test device name2","id":"22","origin":24}
							}
						},
						{"humidity":100},
						{"temperature":50}
					]`,
			expected: &dtos.Event{
				Id:          "",
				DeviceName:  "demo",
				ProfileName: "kuiperProfile",
				Origin:      3,
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "humidity",
						DeviceName:    "test device name1",
						ProfileName:   "kuiperProfile",
						Id:            "12",
						Origin:        14,
						ValueType:     v2.ValueTypeInt64,
						SimpleReading: dtos.SimpleReading{Value: "100"},
					},
					{
						ResourceName:  "temperature",
						DeviceName:    "test device name2",
						ProfileName:   "kuiperProfile",
						Id:            "22",
						Origin:        24,
						ValueType:     v2.ValueTypeFloat64,
						SimpleReading: dtos.SimpleReading{Value: "5.000000e+01"},
					},
				},
			},
			error: "",
		},

		{
			input: `[
						{"meta":{
							"correlationid":"","profileName":"demoProfile","deviceName":"demo","sourceName":"demoSource","id":"abc","origin":3,"tags":{"auth":"admin"},
							"humidity":{"deviceName":"test device name1","id":"12","origin":14},
							"temperature":{"deviceName":"test device name2","id":"22","origin":24}
							}
						},
						{"h1":100}
					]`,
			expected: &dtos.Event{
				Id:          "abc",
				DeviceName:  "demo",
				ProfileName: "demoProfile",
				SourceName:  "demoSource",
				Origin:      3,
				Tags:        map[string]string{"auth": "admin"},
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

		{
			input: `[
						{"meta": 50},
						{"h1":100}
					]`,
			expected: &dtos.Event{
				ProfileName: "kuiperProfile",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "h1",
						SimpleReading: dtos.SimpleReading{Value: "1.000000e+02"},
						ProfileName:   "kuiperProfile",
						ValueType:     v2.ValueTypeFloat64,
					},
				},
			},
			error: "",
		},

		{
			input: `[
						{"meta1": "newmeta"},
						{"h1":true},
						{"sa":["1","2","3","4"]},
						{"fa":[1.1,2.2,3.3,4.4]}
					]`,
			expected: &dtos.Event{
				ProfileName: "kuiperProfile",
				Readings: []dtos.BaseReading{
					{
						ResourceName:  "meta1",
						SimpleReading: dtos.SimpleReading{Value: "newmeta"},
						ProfileName:   "kuiperProfile",
						ValueType:     v2.ValueTypeString,
					},
					{
						ResourceName:  "h1",
						SimpleReading: dtos.SimpleReading{Value: "true"},
						ProfileName:   "kuiperProfile",
						ValueType:     v2.ValueTypeBool,
					},
					{
						ResourceName:  "sa",
						SimpleReading: dtos.SimpleReading{Value: "[\"1\",\"2\",\"3\",\"4\"]"},
						ProfileName:   "kuiperProfile",
						ValueType:     v2.ValueTypeStringArray,
					},
					{
						ResourceName:  "fa",
						SimpleReading: dtos.SimpleReading{Value: "[1.100000e+00, 2.200000e+00, 3.300000e+00, 4.400000e+00]"},
						ProfileName:   "kuiperProfile",
						ValueType:     v2.ValueTypeFloat64Array,
					},
				},
			},
			error: "",
		},

		{
			input:       `[]`,
			deviceName:  "kuiper",
			profileName: "kp",
			topic:       "demo",
			expected: &dtos.Event{
				ProfileName: "kp",
				DeviceName:  "kuiper",
				SourceName:  "demo",
				Origin:      0,
				Readings:    nil,
			},
			error: "",
		},
		{
			input: `[{"sa":["1","2",3,"4"]}]`, //invalid array, return nil
			expected: &dtos.Event{
				ProfileName: "kuiperProfile",
				Origin:      0,
				Readings:    nil,
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, t := range tests {
		ems := EdgexMsgBusSink{deviceName: t.deviceName, profileName: t.profileName, topic: t.topic, metadata: "meta"}
		result, err := ems.produceEvents(ctx, []byte(t.input))

		if !reflect.DeepEqual(t.error, common.Errstring(err)) {
			t1.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, t.input, t.error, err)
		} else if t.error == "" && !compareEvent(t.expected, result) {
			t1.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, t.input, t.expected, result)
		}
	}
}
