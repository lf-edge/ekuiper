// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Test scenario of multiple neuron instances
func TestMultiNeuron(t *testing.T) {
	// start and test 2 sources
	url1 := "tcp://127.0.0.1:33331"
	url2 := "tcp://127.0.0.1:33332"
	exp1 := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "yellow"}, "errors": map[string]interface{}{"tag_name3": 122.0}}, map[string]interface{}{"topic": "$$neuron_tcp://127.0.0.1:33331"}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "green", "tag_name3": 60.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_tcp://127.0.0.1:33331"}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 15.4, "tag_name2": "green", "tag_name3": 70.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_tcp://127.0.0.1:33331"}, time.Now()),
	}
	exp2 := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "yellow"}, "errors": map[string]interface{}{"tag_name3": 122.0}}, map[string]interface{}{"topic": "$$neuron_tcp://127.0.0.1:33332"}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "green", "tag_name3": 60.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_tcp://127.0.0.1:33332"}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 15.4, "tag_name2": "green", "tag_name3": 70.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_tcp://127.0.0.1:33332"}, time.Now()),
	}
	s1 := GetSource()
	err := s1.Configure("new", map[string]interface{}{"url": url1})
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	s2 := GetSource()
	err = s2.Configure("new2", map[string]interface{}{"url": url2})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	sin1 := GetSink()
	sin1.Configure(map[string]interface{}{
		"nodeName":  "testM",
		"raw":       false,
		"groupName": "grp",
		"url":       url1,
	})

	sin2 := GetSink()
	sin2.Configure(map[string]interface{}{
		"nodeName":  "testM",
		"raw":       false,
		"groupName": "grp",
		"url":       url2,
	})
	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		mock.TestSourceOpen(s1, exp1, t)
		wg.Done()
	}()
	go func() {
		mock.TestSourceOpen(s2, exp2, t)
		wg.Done()
	}()

	server1, ch1 := mockNeuron(true, true, url1)
	defer server1.Close()

	server2, ch2 := mockNeuron(true, true, url2)
	defer server2.Close()

	data1 := []interface{}{
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

	data2 := []interface{}{
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

	go func() {
		time.Sleep(100 * time.Millisecond)
		err = mock.RunSinkCollect(sin1, data1)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		wg.Done()
	}()
	go func() {
		time.Sleep(100 * time.Millisecond)
		err = mock.RunSinkCollect(sin2, data2)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		wg.Done()
	}()

	sexp1 := []string{
		`{"group_name":"grp","node_name":"testM","tag_name":"humidity","value":50}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"status","value":"green"}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"temperature","value":22}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"humidity","value":82}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"status","value":"wet"}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"temperature","value":25}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"humidity","value":60}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"status","value":"hot"}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"temperature","value":33}`,
	}
	sexp2 := []string{
		`{"group_name":"grp","node_name":"testM","tag_name":"humidity","value":50}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"status","value":"green"}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"temperature","value":22}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"humidity","value":82}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"status","value":"wet"}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"temperature","value":25}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"humidity","value":60}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"status","value":"hot"}`,
		`{"group_name":"grp","node_name":"testM","tag_name":"temperature","value":33}`,
	}
	var actual1, actual2 []string
	ticker := time.After(10 * time.Second)
	for i := 0; i < len(sexp1)+len(sexp2); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch1:
			actual1 = append(actual1, string(d))
		case d2 := <-ch2:
			actual2 = append(actual2, string(d2))
		}
	}
	if !reflect.DeepEqual(actual1, sexp1) {
		t.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", sexp1, actual1)
	}
	if !reflect.DeepEqual(actual2, sexp2) {
		t.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", sexp2, actual2)
	}
	wg.Wait()
}
