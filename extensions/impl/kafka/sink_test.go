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

package kafka

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
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
	require.NoError(t, ks.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	mockT := &testx.MockRawTuple{
		Content: []byte(`{"a":1}`),
	}

	err := ks.collect(ctx, mockT)
	time.Sleep(100 * time.Millisecond)
	require.Len(t, ks.messages, 1)
	require.NoError(t, err)
	require.NoError(t, ks.Close(ctx))

	//for i := mockErrStart + 1; i < mockErrEnd; i++ {
	//	failpoint.Enable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr", fmt.Sprintf("return(%v)", i))
	//	require.Error(t, ks.Provision(ctx, configs), i)
	//}
	//failpoint.Disable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr")
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
	require.NoError(t, ks.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	item := map[string]any{
		"a": 1,
	}
	d, _ := json.Marshal(item)
	mockT := &testx.MockRawTuple{
		Content:  d,
		Template: map[string]string{"{{.a}}": "1"},
	}
	msg, err := ks.buildMsg(ctx, mockT)
	require.NoError(t, err)
	require.Equal(t, "a", msg.Headers[0].Key)
	b := make([]uint8, 0, 8)
	b = strconv.AppendInt(b, int64(1), 10)
	require.Equal(t, b, msg.Headers[0].Value)
	require.Equal(t, []byte("1"), msg.Key)
	require.NoError(t, ks.Close(ctx))

	configs = map[string]any{
		"topic":   "t",
		"brokers": "localhost:9092",
		"headers": "{\"a\":\"{{.a}}\"}",
	}
	ks = &KafkaSink{}
	require.NoError(t, ks.Provision(ctx, configs))
	require.NoError(t, ks.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	mockT = &testx.MockRawTuple{
		Content:  d,
		Template: map[string]string{"{\"a\":\"{{.a}}\"}": "{\"a\":\"1\"}"},
	}
	msg, err = ks.buildMsg(ctx, mockT)
	require.NoError(t, err)
	require.Equal(t, "a", msg.Headers[0].Key)
	require.Equal(t, b, msg.Headers[0].Value)
}

func TestToCompression(t *testing.T) {
	testcase := []struct {
		c      string
		expect kafkago.Compression
	}{
		{
			c:      "gzip",
			expect: kafkago.Gzip,
		},
		{
			c:      "snappy",
			expect: kafkago.Snappy,
		},
		{
			c:      "lz4",
			expect: kafkago.Lz4,
		},
		{
			c:      "zstd",
			expect: kafkago.Zstd,
		},
		{
			c:      "",
			expect: 0,
		},
	}
	for _, tc := range testcase {
		e := toCompression(tc.c)
		require.Equal(t, tc.expect, e)
	}
}
