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
	"errors"
	"testing"
	"time"

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
		err   error
	}{
		{
			name: "No server",
			props: map[string]any{
				"server": "",
			},
			err: errors.New("missing server property"),
		},
		{
			name: "invalid protocol",
			props: map[string]any{
				"server":          url,
				"protocolVersion": "5.0",
			},
			err: errors.New("unsupported protocol version 5.0"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateConfig(tt.props)
			require.Equal(t, tt.err, err)
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
		"server":     url,
		"datasource": "demo",
	})
	require.NoError(t, err)
	err = c.Dial(ctx)
	require.NoError(t, err)
	// wait connection done
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, c.Ping(ctx))
}
