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

package neuron

import (
	"testing"

	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestRun(t *testing.T) {
	exp := []api.MessageTuple{
		model.NewDefaultSourceTuple(map[string]any{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]any{"tag_name1": 11.22, "tag_name2": "yellow"}, "errors": map[string]any{"tag_name3": 122.0}}, nil, timex.GetNow()),
		model.NewDefaultSourceTuple(map[string]any{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]any{"tag_name1": 11.22, "tag_name2": "green", "tag_name3": 60.0}, "errors": map[string]any{}}, nil, timex.GetNow()),
		model.NewDefaultSourceTuple(map[string]any{"group_name": "group1", "timestamp": 1646125996000.0, "node_name": "node1", "values": map[string]any{"tag_name1": 15.4, "tag_name2": "green", "tag_name3": 70.0}, "errors": map[string]any{}}, nil, timex.GetNow()),
	}
	s := GetSource()
	server, _ := mockNeuron(true, false, DefaultNeuronUrl)
	defer server.Close()
	mock.TestSourceConnector(t, s, map[string]any{
		"datasource": "new",
	}, exp, func() {
		// do nothing
	})
}
