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

	"github.com/stretchr/testify/assert"

	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestConnectionLC(t *testing.T) {
	propsShared := map[string]any{
		"connectionSelector": "mqtt.localConnection",
	}
	propsNormal := map[string]any{
		"server": url,
	}
	propsInvalid := map[string]any{
		"server": "abc",
	}
	ctx := mockContext.NewMockContext("test", "op")
	connShared1, err := GetConnection(ctx, propsShared)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(connectionPool))
	_, err = GetConnection(ctx, propsNormal)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(connectionPool))
	connShared2, err := GetConnection(ctx, propsShared)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(connectionPool))
	assert.Equal(t, connShared1, connShared2)
	_, err = GetConnection(ctx, propsInvalid)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "found error when connecting for abc: network Error : dial tcp: address abc: missing port in address")
	assert.Equal(t, 1, len(connectionPool))

	c := connShared1.refCount.Load()
	assert.Equal(t, int32(2), c)

	err = connShared1.Ping()
	assert.NoError(t, err)

	// Test subscribe in the connector test.

	DetachConnection("mqtt.localConnection", "")
	assert.Equal(t, 1, len(connectionPool))
	DetachConnection("mqtt.localConnection", "")
	assert.Equal(t, 0, len(connectionPool))
}

func TestValidate(t *testing.T) {
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
			_, err := validateConfig(tt.props)
			assert.Error(t, err)
			assert.Equal(t, tt.err, err.Error())
		})
	}
}
