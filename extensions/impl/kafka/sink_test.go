// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestKafkaSink(t *testing.T) {
	ks := &KafkaSink{}
	testcases := []struct {
		configs map[string]any
	}{
		{
			configs: map[string]any{},
		},
		{
			configs: map[string]any{
				"topic": "t",
			},
		},
		{
			configs: map[string]any{
				"topic":            "t",
				"brokers":          "localhost:9092",
				"certificationRaw": "mockErr",
			},
		},
		{
			configs: map[string]any{
				"datasource":   "t",
				"brokers":      "localhost:9092",
				"saslAuthType": "mockErr",
			},
		},
		{
			configs: map[string]any{
				"datasource":   "t",
				"brokers":      "localhost:9092",
				"saslAuthType": "plain",
			},
		},
		{
			configs: map[string]any{
				"topic":   "t",
				"brokers": "localhost:9092",
				"headers": 1,
			},
		},
	}
	ctx := mockContext.NewMockContext("1", "2")
	for index, tc := range testcases {
		require.Error(t, ks.Provision(ctx, tc.configs), index)
	}
	configs := map[string]any{
		"topic":   "t",
		"brokers": "localhost:9092",
	}
	require.NoError(t, ks.Provision(ctx, configs))
	require.NoError(t, ks.Connect(ctx))
	mockT := testx.MockTuple{
		Map: map[string]any{"1": 1},
	}
	msgs, err := ks.collect(ctx, mockT)
	require.Len(t, msgs, 1)
	require.NoError(t, err)
	require.NoError(t, ks.Close(ctx))

	for i := mockErrStart + 1; i < mockErrEnd; i++ {
		failpoint.Enable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr", fmt.Sprintf("return(%v)", i))
		require.Error(t, ks.Provision(ctx, configs), i)
	}
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr")
}

func TestKafkaSinkBuildMsg(t *testing.T) {
	configs := map[string]any{
		"topic":   "t",
		"brokers": "localhost:9092",
		"headers": map[string]any{
			"a": "{{.a}}",
		},
		"key": "{{.a}}",
	}
	ks := &KafkaSink{}
	ctx := mockContext.NewMockContext("1", "2")
	require.NoError(t, ks.Provision(ctx, configs))
	require.NoError(t, ks.Connect(ctx))
	item := map[string]any{
		"a": 1,
	}
	d, _ := json.Marshal(item)
	mockT := testx.MockTuple{
		Map:      item,
		Template: map[string]string{"a": "1", "key": "1"},
	}
	msg, err := ks.buildMsg(ctx, mockT, d)
	require.NoError(t, err)
	require.Equal(t, "a", msg.Headers[0].Key)
	b := make([]uint8, 0, 8)
	b = strconv.AppendInt(b, int64(1), 10)
	require.Equal(t, b, msg.Headers[0].Value)
	require.Equal(t, []byte("1"), msg.Key)
	require.NoError(t, ks.Close(ctx))
}
