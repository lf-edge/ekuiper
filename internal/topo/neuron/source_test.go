// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/topo/mock"
	"github.com/lf-edge/ekuiper/pkg/api"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	"testing"
)

func TestRun(t *testing.T) {
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "yellow"}, "errors": map[string]interface{}{"tag_name3": 122.0}}, map[string]interface{}{"topic": "$$neuron"}),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 11.22, "tag_name2": "green", "tag_name3": 60.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron"}),
		api.NewDefaultSourceTuple(map[string]interface{}{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]interface{}{"tag_name1": 15.4, "tag_name2": "green", "tag_name3": 70.0}, "errors": map[string]interface{}{}}, map[string]interface{}{"topic": "$$neuron"}),
	}
	s := GetSource()
	err := s.Configure("new", nil)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	server, _ := mockNeuron(true, false)
	defer server.Close()
	mock.TestSourceOpen(s, exp, t)
}
