// +build edgex

package sinks

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/models"
	"reflect"
	"testing"
)

func TestProduceEvents(t1 *testing.T) {
	var tests = []struct {
		input      string
		deviceName string
		expected   *models.Event
		error      string
	}{
		{
			input: `[
						{"meta":{
							"correlationid":"","created":1,"device":"demo","id":"","modified":2,"origin":3,"pushed":0,
							"humidity":{"created":11,"device":"test device name1","id":"12","modified":13,"origin":14,"pushed":15},
							"temperature":{"created":21,"device":"test device name2","id":"22","modified":23,"origin":24,"pushed":25}
							}
						},
						{"humidity":100},
						{"temperature":50}
					]`,
			expected: &models.Event{
				ID:       "",
				Pushed:   0,
				Device:   "demo",
				Created:  1,
				Modified: 2,
				Origin:   3,
				Readings: []models.Reading{
					{
						Name:     "humidity",
						Value:    "100",
						Created:  11,
						Device:   "test device name1",
						Id:       "12",
						Modified: 13,
						Origin:   14,
						Pushed:   15,
					},
					{
						Name:     "temperature",
						Value:    "50",
						Created:  21,
						Device:   "test device name2",
						Id:       "22",
						Modified: 23,
						Origin:   24,
						Pushed:   25,
					},
				},
			},
			error: "",
		},

		{
			input: `[
						{"meta":{
							"correlationid":"","created":1,"device":"demo","id":"","modified":2,"origin":3,"pushed":0,
							"humidity":{"created":11,"device":"test device name1","id":"12","modified":13,"origin":14,"pushed":15},
							"temperature":{"created":21,"device":"test device name2","id":"22","modified":23,"origin":24,"pushed":25}
							}
						},
						{"h1":100}
					]`,
			expected: &models.Event{
				ID:       "",
				Pushed:   0,
				Device:   "demo",
				Created:  1,
				Modified: 2,
				Origin:   3,
				Readings: []models.Reading{
					{
						Name:     "h1",
						Value:    "100",
						Created:  0,
						Device:   "",
						Id:       "",
						Modified: 0,
						Origin:   0,
						Pushed:   0,
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
			expected: &models.Event{
				ID:       "",
				Pushed:   0,
				Device:   "",
				Created:  0,
				Modified: 0,
				Origin:   0,
				Readings: []models.Reading{
					{
						Name:     "h1",
						Value:    "100",
						Created:  0,
						Device:   "",
						Id:       "",
						Modified: 0,
						Origin:   0,
						Pushed:   0,
					},
				},
			},
			error: "",
		},

		{
			input: `[
						{"meta1": 50},
						{"h1":100}
					]`,
			expected: &models.Event{
				ID:       "",
				Pushed:   0,
				Device:   "",
				Created:  0,
				Modified: 0,
				Origin:   0,
				Readings: []models.Reading{
					{
						Name:     "meta1",
						Value:    "50",
						Created:  0,
						Device:   "",
						Id:       "",
						Modified: 0,
						Origin:   0,
						Pushed:   0,
					},
					{
						Name:     "h1",
						Value:    "100",
						Created:  0,
						Device:   "",
						Id:       "",
						Modified: 0,
						Origin:   0,
						Pushed:   0,
					},
				},
			},
			error: "",
		},

		{
			input:      `[]`,
			deviceName: "kuiper",
			expected: &models.Event{
				ID:       "",
				Pushed:   0,
				Device:   "kuiper",
				Created:  0,
				Modified: 0,
				Origin:   0,
				Readings: nil,
			},
			error: "",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, t := range tests {
		ems := EdgexMsgBusSink{deviceName: t.deviceName, metadata: "meta"}
		result, err := ems.produceEvents([]byte(t.input))

		if !reflect.DeepEqual(t.error, errstring(err)) {
			t1.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, t.input, t.error, err)
		} else if t.error == "" && !reflect.DeepEqual(t.expected, result) {
			t1.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, t.input, t.expected, result)
		}
	}
}

// errstring returns the string representation of an error.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
