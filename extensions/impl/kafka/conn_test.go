// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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

package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestKafkaConnectionProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	conn := CreateConnection(ctx)
	require.IsType(t, &KafkaConnection{}, conn)

	require.Error(t, conn.Provision(ctx, "kafkaConn", map[string]any{}))
	require.Error(t, conn.Provision(ctx, "kafkaConn", map[string]any{
		"brokers":      "127.0.0.1:9092",
		"saslAuthType": "plain",
		"saslUserName": "user",
	}))
	require.NoError(t, conn.Provision(ctx, "kafkaConn", map[string]any{
		"brokers": "127.0.0.1:9092",
	}))
	require.Equal(t, "kafkaConn", conn.GetId(ctx))
	require.NoError(t, conn.Close(ctx))
}

func TestKafkaConnectionRegistered(t *testing.T) {
	provider, ok := modules.GetConnectionProvider("kafka")
	require.True(t, ok)
	require.NotNil(t, provider)
}
