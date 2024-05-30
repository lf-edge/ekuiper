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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/connection"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// NOTICE!!! Need to run a MQTT broker in localhost:1883 for this test or change the url to your broker
const url = "tcp://127.0.0.1:1883"

func init() {
	testx.InitEnv("mqtt_source_connector")
}

func TestProvision(t *testing.T) {
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
