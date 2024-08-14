// Copyright 2024 EMQ Technologies Co., Ltd.
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

package mqtt

import (
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestSourceSink(t *testing.T) {
	url, cancel, err := testx.InitBroker("TestSourceSink")
	require.NoError(t, err)
	defer func() {
		cancel()
	}()
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, connection.InitConnectionManager4Test())
	sc := GetSource().(api.BytesSource)
	sk := GetSink().(api.BytesCollector)
	mc := mockclock.GetMockClock()

	data := [][]byte{
		[]byte("{\"humidity\":50,\"status\":\"green\",\"temperature\":22}"),
		[]byte("{\"humidity\":82,\"status\":\"wet\",\"temperature\":25}"),
		[]byte("{\"humidity\":60,\"status\":\"hot\",\"temperature\":33}"),
	}
	result := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("{\"humidity\":50,\"status\":\"green\",\"temperature\":22}"), map[string]any{
			"topic":     "demo",
			"messageId": uint16(0),
			"qos":       byte(0),
		}, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"humidity\":82,\"status\":\"wet\",\"temperature\":25}"), map[string]any{
			"topic":     "demo",
			"messageId": uint16(0),
			"qos":       byte(0),
		}, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"humidity\":60,\"status\":\"hot\",\"temperature\":33}"), map[string]any{
			"topic":     "demo",
			"messageId": uint16(0),
			"qos":       byte(0),
		}, mc.Now()),
	}

	// Open and subscribe before sending data
	mock.TestSourceConnector(t, sc, map[string]any{
		"server":     url,
		"datasource": "demo",
		"qos":        0,
		"topic":      "demo",
	}, result, func() {
		err := mock.RunBytesSinkCollect(sk, data, map[string]any{
			"server":   url,
			"topic":    "demo",
			"qos":      0,
			"retained": false,
		})
		assert.NoError(t, err)
	})
}
