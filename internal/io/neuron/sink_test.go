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

package neuron

import (
	"reflect"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/internal/io/mock"
)

func sinkTest(t *testing.T) {
	server, ch := mockNeuron(false, true, DefaultNeuronUrl)
	defer server.Close()

	s := GetSink()
	s.Configure(map[string]interface{}{
		"nodeName":  "test1",
		"groupName": "grp",
		"tags":      []string{"temperature", "status"},
		"raw":       false,
	})

	data := []interface{}{
		map[string]interface{}{
			"temperature": 22,
			"humidity":    50,
			"status":      "green",
		},
		map[string]interface{}{
			"temperature": 25,
			"humidity":    82,
			"status":      "wet",
		},
		map[string]interface{}{
			"temperature": 33,
			"humidity":    60,
			"status":      "hot",
		},
	}
	err := mock.RunSinkCollect(s, data)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	exp := []string{
		`{"group_name":"grp","node_name":"test1","tag_name":"temperature","value":22}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"status","value":"green"}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"temperature","value":25}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"status","value":"wet"}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"temperature","value":33}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"status","value":"hot"}`,
	}
	var actual []string
	ticker := time.After(10 * time.Second)
	for i := 0; i < len(exp); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch:
			actual = append(actual, string(d))
		}
	}

	if !reflect.DeepEqual(actual, exp) {
		t.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", exp, actual)
	}
	time.Sleep(100 * time.Millisecond)
}

func sinkConnExpTest(t *testing.T) {
	s := GetSink()
	s.Configure(map[string]interface{}{
		"nodeName":  "test1",
		"groupName": "grp",
		"tags":      []string{"temperature", "status"},
		"raw":       false,
	})
	data := []interface{}{
		map[string]interface{}{
			"temperature": 22,
			"humidity":    50,
			"status":      "green",
		},
		map[string]interface{}{
			"temperature": 25,
			"humidity":    82,
			"status":      "wet",
		},
		map[string]interface{}{
			"temperature": 33,
			"humidity":    60,
			"status":      "hot",
		},
	}
	expErrStr := "io error: Error publish the tag payload temperature: io error: neuron connection is not established"
	err := mock.RunSinkCollect(s, data)
	if err == nil {
		t.Errorf("should have error")
		return
	} else if err.Error() != expErrStr {
		t.Errorf("error mismatch:\n\nexp=%s\n\ngot=%s\n\n", expErrStr, err.Error())
	}
}
