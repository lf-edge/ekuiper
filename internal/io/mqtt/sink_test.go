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

package mqtt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSinkConfigure(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, connection.InitConnectionManager4Test())
	tests := []struct {
		name           string
		input          map[string]interface{}
		expectedErr    string
		expectedAdConf *AdConf
	}{
		{
			name: "TLS Error",
			input: map[string]interface{}{
				"topic":         "testTopic3",
				"qos":           0,
				"retained":      false,
				"compression":   "",
				"privateKeyRaw": "MTIz",
				"server":        "123",
			},
			expectedErr: "tls: failed to find any PEM data in certificate input",
		},
		{
			name: "Invalid configuration",
			input: map[string]any{
				"server":     "123",
				"datasource": "demo",
				"qos":        "dd",
			},
			expectedErr: "1 error(s) decoding:\n\n* 'qos' expected type 'uint8', got unconvertible type 'string', value: 'dd'",
		},
		{
			name: "Missing topic",
			input: map[string]interface{}{
				"server":      "123",
				"qos":         1,
				"retained":    false,
				"compression": "zlib",
			},
			expectedErr: "mqtt sink is missing property topic",
		},
		{
			name: "Wrong topic",
			input: map[string]interface{}{
				"server":      "123",
				"topic":       "test/#",
				"qos":         1,
				"retained":    false,
				"compression": "zlib",
			},
			expectedErr: "mqtt sink topic shouldn't contain # or +",
		},
		{
			name: "Invalid QoS",
			input: map[string]interface{}{
				"server":      "123",
				"topic":       "testTopic",
				"qos":         3,
				"retained":    false,
				"compression": "gzip",
			},
			expectedErr: fmt.Sprintf("invalid qos value %v, the value could be only int 0 or 1 or 2", 3),
		},
		{
			name: "Valid configuration with QoS 0 and no compression",
			input: map[string]interface{}{
				"topic":              "testTopic3",
				"qos":                0,
				"retained":           false,
				"compression":        "",
				"server":             "123",
				"connectionSelector": "mqtt.local",
			},
			expectedAdConf: &AdConf{
				Tpc:      "testTopic3",
				Qos:      0,
				Retained: false,
				SelId:    "mqtt.local",
			},
		},
		{
			name: "Valid configuration with QoS 1 and no retained",
			input: map[string]interface{}{
				"topic":       "testTopic4",
				"qos":         1,
				"retained":    false,
				"compression": "zlib",
				"server":      "123",
			},
			expectedAdConf: &AdConf{
				Tpc:      "testTopic4",
				Qos:      1,
				Retained: false,
			},
		},
	}

	ctx := mockContext.NewMockContext("testsinkconfigure", "sink1")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := &Sink{}
			err := ms.Provision(ctx, tt.input)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, ms.adconf, tt.expectedAdConf)
			}
		})
	}
}

func TestValidateMQTTSinkConf(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	require.NoError(t, store.SetupDefault(dataDir))
	require.NoError(t, connection.InitConnectionManager4Test())
	testcases := []struct {
		topic       string
		expectError bool
	}{
		{
			topic:       "/123/+",
			expectError: true,
		},
		{
			topic:       "/123/#",
			expectError: true,
		},
		{
			topic: "/123/",
		},
	}
	for _, tc := range testcases {
		err := validateMQTTSinkTopic(tc.topic)
		if tc.expectError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}
