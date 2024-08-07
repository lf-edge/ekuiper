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

package client

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestValidate(t *testing.T) {
	url, cancel, err := testx.InitBroker("TestValidate")
	require.NoError(t, err)
	defer func() {
		cancel()
	}()

	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "No server",
			props: map[string]any{
				"server": "",
			},
			err: "missing server property",
		},
		{
			name: "invalid protocol",
			props: map[string]any{
				"server":          url,
				"protocolVersion": "5.0",
			},
			err: "unsupported protocol version 5.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateConfig(tt.props)
			assert.Error(t, err)
			assert.Equal(t, tt.err, err.Error())
		})
	}
}

func TestMqttClientPing(t *testing.T) {
	url, cancel, err := testx.InitBroker("TestMqttClientPing")
	require.NoError(t, err)
	ctx := mockContext.NewMockContext("1", "2")
	c, err := CreateClient(ctx, map[string]any{
		"server":     url,
		"datasource": "demo",
	})
	require.NoError(t, err)
	// wait connection done
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, c.Ping(ctx))
	cancel()
	// wait status done
	time.Sleep(100 * time.Millisecond)
	require.Error(t, c.Ping(ctx))
}

func TestCreateClientPanic(t *testing.T) {
	url, cancel, err := testx.InitBroker("TestMqttClientPing")
	require.NoError(t, err)
	defer cancel()
	ctx := mockContext.NewMockContext("1", "2")
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client/panic", "return(true)")
	defer failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client/panic")
	_, err = CreateClient(ctx, map[string]any{
		"server":     url,
		"datasource": "demo",
	})
	require.Error(t, err)
}
