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
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	"reflect"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "yellow"}, "errors": map[string]interface{}{"tag_name3": 122.0}}, map[string]interface{}{"topic": "$$neuron_ipc:///tmp/neuron-ekuiper.ipc"}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "green", "tag_name3": 60.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_ipc:///tmp/neuron-ekuiper.ipc"}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 15.4, "tag_name2": "green", "tag_name3": 70.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron_ipc:///tmp/neuron-ekuiper.ipc"}, time.Now()),
	}
	s := GetSource()
	err := s.Configure("new", nil)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	server, _ := mockNeuron(true, false, DefaultNeuronUrl)
	defer server.Close()
	mock.TestSourceOpen(s, exp, t)
}

func connectFailTest(t *testing.T) {
	s := GetSource()
	err := s.Configure("new", nil)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	ctx, cancel := mock.NewMockContext("ruleTestReconnect", "op1").WithCancel()
	consumer := make(chan api.SourceTuple)
	errCh := make(chan error)
	server, _ := mockNeuron(false, false, DefaultNeuronUrl)
	go s.Open(ctx, consumer, errCh)
	go func() {
		select {
		case err := <-errCh:
			t.Errorf("received error: %v", err)
		case tuple := <-consumer:
			if !reflect.DeepEqual(tuple, &xsql.ErrorSourceTuple{Error: fmt.Errorf("neuron connection detached")}) {
				t.Errorf("received unexpected tuple: %v", tuple)
			}
		}
		cancel()
	}()
	time.Sleep(1 * time.Second)
	server.Close()
	time.Sleep(1 * time.Second)
}
