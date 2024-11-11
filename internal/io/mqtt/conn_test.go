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
	"errors"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		err   error
	}{
		{
			name: "valid version",
			props: map[string]any{
				"protocolVersion": 5,
			},
			err: errors.New("1 error(s) decoding:\n\n* 'protocolVersion' expected type 'string', got unconvertible type 'int', value: '5'"),
		},
		{
			name: "invalid conf v4",
			props: map[string]any{
				"protocolVersion": "3.1",
				"server":          1883,
			},
			err: errors.New("1 error(s) decoding:\n\n* 'server' expected type 'string', got unconvertible type 'int', value: '1883'"),
		},
		{
			name: "invalid conf v5",
			props: map[string]any{
				"protocolVersion": "5",
				"server":          1883,
			},
			err: errors.New("1 error(s) decoding:\n\n* 'server' expected type 'string', got unconvertible type 'int', value: '1883'"),
		},
		{
			name: "No server",
			props: map[string]any{
				"server": "",
			},
			err: errors.New("missing server property"),
		},
		{
			name: "No server v5",
			props: map[string]any{
				"server":          "",
				"protocolVersion": "5",
			},
			err: errors.New("missing server property"),
		},
		{
			name: "wrong server",
			props: map[string]any{
				"server":          "http://example.com/%XX",
				"protocolVersion": "5",
			},
			err: errors.New("parse \"http://example.com/%XX\": invalid URL escape \"%XX\""),
		},
		{
			name: "invalid protocol",
			props: map[string]any{
				"server":          "tcp://127.0.0.1:1883",
				"protocolVersion": "5.0",
			},
			err: errors.New("unsupported protocol version 5.0"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.props)
			require.EqualError(t, err, tt.err.Error())
		})
	}
}

func TestMqttClientPing(t *testing.T) {
	url, cancel, err := testx.InitBroker("TestMqttClientPing")
	require.NoError(t, err)
	defer cancel()
	ctx := mockContext.NewMockContext("1", "2")
	c := CreateConnection(ctx)
	err = c.Provision(ctx, "test", map[string]any{
		"server":          url,
		"protocolVersion": 5,
	})
	require.EqualError(t, err, "1 error(s) decoding:\n\n* 'protocolVersion' expected type 'string', got unconvertible type 'int', value: '5'")
	err = c.Provision(ctx, "test", map[string]any{
		"server":          url,
		"protocolVersion": "6",
	})
	require.EqualError(t, err, "unsupported protocol version 6")
	err = c.Provision(ctx, "test", map[string]any{
		"server":          url,
		"datasource":      "demo",
		"protocolVersion": "3.1",
	})
	require.NoError(t, err)
	err = c.Dial(ctx)
	require.NoError(t, err)
	// wait connection done
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, c.Ping(ctx))
}

func TestNoClient(t *testing.T) {
	ctx := mockContext.NewMockContext("1", "2")
	c := CreateConnection(ctx).(*Connection)
	c.status.Store(modules.ConnectionStatus{Status: api.ConnectionConnecting})
	id := c.GetId(ctx)
	assert.Equal(t, "", id)
	status := c.Status(ctx)
	assert.Equal(t, modules.ConnectionStatus{Status: api.ConnectionConnecting}, status)
	c.DetachSub(ctx, map[string]any{"abc": 1})
	c.DetachSub(ctx, map[string]any{"datasource": "test"})
	err := c.Publish(ctx, "abc", 1, false, []byte("hello"), nil)
	assert.True(t, errorx.IsIOError(err))
	err = c.Close(ctx)
	assert.NoError(t, err)
}
