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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestRun(t *testing.T) {
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("{\"timestamp\": 1646125996000, \"node_name\": \"node1\", \"group_name\": \"group1\", \"values\": {\"tag_name1\": 11.22, \"tag_name2\": \"yellow\"}, \"errors\": {\"tag_name3\": 122}}"), nil, timex.GetNow()),
		model.NewDefaultRawTuple([]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 11.22, "tag_name2": "green","tag_name3":60}, "errors": {}}`), nil, timex.GetNow()),
		model.NewDefaultRawTuple([]byte(`{"timestamp": 1646125996000, "node_name": "node1", "group_name": "group1", "values": {"tag_name1": 15.4, "tag_name2": "green","tag_name3":70}, "errors": {}}`), nil, timex.GetNow()),
	}
	s := GetSource()
	server, _ := mockNeuron(true, false, DefaultNeuronUrl)
	defer server.Close()
	mock.TestSourceConnector(t, s, map[string]any{
		"datasource": "new",
		"url":        DefaultNeuronUrl,
	}, exp, func() {
		// do nothing
	})

	ctx := mockContext.NewMockContext("t", "tt")
	err := s.(*source).Ping(ctx, map[string]any{"url": DefaultNeuronUrl})
	assert.NoError(t, err)
}

func TestProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("t", "tt")
	s := GetSource()
	err := s.Provision(ctx, map[string]any{
		"url": "3434",
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "only tcp and ipc scheme are supported")

	err = s.Provision(ctx, map[string]any{
		"url": "tcp://127.0.0.1:8000",
	})
	assert.NoError(t, err)

	su, ok := s.(model.UniqueSub)
	assert.True(t, ok)
	sid := su.SubId(map[string]any{
		"url": "tcp://127.0.0.1:8000",
	})
	assert.Equal(t, "singleton", sid)

	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)

	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)

	err = s.Close(ctx)
	assert.NoError(t, err)
}
