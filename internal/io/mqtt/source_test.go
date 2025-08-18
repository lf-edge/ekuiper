// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func init() {
	testx.InitEnv("mqtt_source_connector")
	modules.RegisterConnection("mqtt", CreateConnection)
}

func TestProvision(t *testing.T) {
	url, cancel, err := testx.InitBroker("TestProvision")
	require.NoError(t, err)
	defer func() {
		cancel()
	}()
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, connection.InitConnectionManager4Test())
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "Valid configuration",
			props: map[string]any{
				"server":     url,
				"datasource": "demo",
			},
		},
		{
			name: "Invalid configuration",
			props: map[string]any{
				"server":     make(chan any),
				"datasource": "demo",
			},
			err: "1 error(s) decoding:\n\n* 'server' expected type 'string'",
		},
		{
			name: "eof message setting",
			props: map[string]any{
				"server":     url,
				"datasource": "demo",
				"eofMessage": "äöüß",
			},
			err: "illegal base64 data at input byte 0",
		},
	}
	sc := &SourceConnector{}
	ctx := mockContext.NewMockContext("testprov", "source")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sc.Provision(ctx, tt.props)
			if tt.err != "" {
				assert.Error(t, err)
				require.True(t, strings.HasPrefix(err.Error(), tt.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEoF(t *testing.T) {
	url, cancel, err := testx.InitBroker("TestSourceSink")
	require.NoError(t, err)
	defer func() {
		cancel()
	}()
	// Create a batch MQTT stream
	r := &SourceConnector{}
	eofStr := base64.StdEncoding.EncodeToString([]byte{0})
	ctx, cancel := mockContext.NewMockContext("ruleEof", "op1").WithCancel()
	err = r.Provision(ctx, map[string]any{
		"server":     url,
		"datasource": "eofdemo",
		"eofMessage": eofStr,
		"qos":        0,
	})
	assert.NoError(t, err)
	err = r.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	// Create a channel to receive the result
	resultCh := make(chan any, 10)
	// Set eof
	r.SetEofIngest(func(ctx api.StreamContext, msg string) {
		resultCh <- xsql.EOFTuple(msg)
	})
	err = r.Subscribe(ctx, func(ctx api.StreamContext, payload []byte, meta map[string]any, ts time.Time) {
		resultCh <- payload
	}, nil)
	if err != nil {
		return
	}
	// Send the data, add eof message at last
	data := [][]byte{
		[]byte("{\"humidity\":50,\"status\":\"green\",\"temperature\":22}"),
		[]byte("{\"humidity\":82,\"status\":\"wet\",\"temperature\":25}"),
		[]byte("{\"humidity\":60,\"status\":\"hot\",\"temperature\":33}"),
		{0},
		[]byte("won't receive"),
	}
	go func() {
		sk := &Sink{}
		err := mock.RunBytesSinkCollect(sk, data, map[string]any{
			"server":   url,
			"topic":    "eofdemo",
			"qos":      0,
			"retained": false,
		})
		assert.NoError(t, err)
	}()
	// Compare the data
	var result [][]byte
	ticker := timex.GetTicker(10 * time.Second)
	defer ticker.Stop()
loop:
	for {
		select {
		case received := <-resultCh:
			switch rt := received.(type) {
			case xsql.EOFTuple:
				break loop
			case []byte:
				result = append(result, rt)
			}
		case <-ticker.C:
			assert.Fail(t, "time out")
			break loop
		}
	}

	assert.Equal(t, data[:3], result)
}

func TestMultiTopicSubscribe(t *testing.T) {
	//url, cancel, err := testx.InitBroker("TestMultiTopicSubscribe")
	//require.NoError(t, err)
	//defer func() {
	//	cancel()
	//}()
	//// Create a batch MQTT stream
	//r := &SourceConnector{}
	//ctx, cancel := mockContext.NewMockContext("ruleEof", "op1").WithCancel()
	//err = r.Provision(ctx, map[string]any{
	//	"server":     url,
	//	"datasource": "test1,test2",
	//	"qos":        0,
	//})
	//require.NoError(t, err)
	//err = r.Connect(ctx, func(status string, message string) {
	//	// do nothing
	//})
	//require.NoError(t, err)
	//// Create a channel to receive the result
	//resultCh := make(chan any, 10)
	//err = r.Subscribe(ctx, func(ctx api.StreamContext, payload []byte, meta map[string]any, ts time.Time) {
	//	resultCh <- payload
	//}, nil)
	//require.NoError(t, err)
	//// Send the data, add eof message at last
	//data := [][]byte{
	//	[]byte("{\"humidity\":50,\"status\":\"green\",\"temperature\":22}"),
	//	[]byte("{\"humidity\":82,\"status\":\"wet\",\"temperature\":25}"),
	//	[]byte("{\"humidity\":60,\"status\":\"hot\",\"temperature\":33}"),
	//}
}
