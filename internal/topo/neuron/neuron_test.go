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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/topo/mock"
	"github.com/lf-edge/ekuiper/pkg/api"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"
)

var data = [][]byte{
	[]byte("{\"timestamp\": 1646125996000, \"node_name\": \"node1\", \"group_name\": \"group1\", \"values\": {\"tag_name1\": 11.22, \"tag_name2\": \"yellow\"}, \"errors\": {\"tag_name3\": 122}}"),
	[]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 11.22, "tag_name2": "green","tag_name3":60}, "errors": {}}`),
	[]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 15.4, "tag_name2": "green","tag_name3":70}, "errors": {}}`),
}

// mockNeuron start the nng pair server
func mockNeuron(send bool, recv bool) (mangos.Socket, chan []byte) {
	var (
		sock mangos.Socket
		err  error
		ch   chan []byte
	)
	if sock, err = pair.NewSocket(); err != nil {
		log.Fatalf("can't get new pair socket: %s", err)
	}
	if err = sock.Listen("ipc:///tmp/neuron-ekuiper.ipc"); err != nil {
		log.Fatalf("can't listen on pair socket: %s", err.Error())
	} else {
		log.Printf("listen on pair socket")
	}
	if recv {
		ch = make(chan []byte)
		go func() {
			for {
				var msg []byte
				var err error
				if msg, err = sock.Recv(); err == nil {
					fmt.Printf("Neuron RECEIVED: \"%s\"\n", string(msg))
					ch <- msg
					fmt.Println("Neuron Sent out")
				}
			}
		}()
	}
	if send {
		go func() {
			for _, msg := range data {
				time.Sleep(10 * time.Millisecond)
				fmt.Printf("Neuron SENDING \"%s\"\n", msg)
				if err := sock.Send(msg); err != nil {
					fmt.Printf("failed sending: %s\n", err)
				}
			}
		}()
	}
	return sock, ch
}

// Test scenario of multiple neuron sources and sinks
func TestMultiSourceSink(t *testing.T) {
	// start and test 2 sources
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "yellow"}, "errors": map[string]interface{}{"tag_name3": 122.0}}, map[string]interface{}{"topic": "$$neuron_ipc:///tmp/neuron-ekuiper.ipc"}),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "green", "tag_name3": 60.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_ipc:///tmp/neuron-ekuiper.ipc"}),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 15.4, "tag_name2": "green", "tag_name3": 70.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_ipc:///tmp/neuron-ekuiper.ipc"}),
	}
	s1 := GetSource()
	err := s1.Configure("new", nil)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	s2 := GetSource()
	err = s2.Configure("new2", nil)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	sin := GetSink()
	sin.Configure(map[string]interface{}{
		"nodeName":  "testM",
		"raw":       false,
		"groupName": "grp",
	})

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		mock.TestSourceOpen(s1, exp, t)
		wg.Done()
	}()
	go func() {
		mock.TestSourceOpen(s2, exp, t)
		wg.Done()
	}()
	// let the server start after the rule to test async dial behavior
	server, ch := mockNeuron(true, true)
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
	go func() {
		time.Sleep(100 * time.Millisecond)
		err = mock.RunSinkCollect(sin, data)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		wg.Done()
	}()
	sexp := []string{
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
	var actual []string
	ticker := time.After(10 * time.Second)
	for i := 0; i < len(sexp); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch:
			actual = append(actual, string(d))
		}
	}

	if !reflect.DeepEqual(actual, sexp) {
		t.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", sexp, actual)
	}
	wg.Wait()
	server.Close()
	time.Sleep(100 * time.Millisecond)
	sinkTest(t)
	sinkConnExpTest(t)
	connectFailTest(t)
}
