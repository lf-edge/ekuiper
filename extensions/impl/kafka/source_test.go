// Copyright 2024-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestKafkaSource(t *testing.T) {
	ks := &KafkaSource{}
	testcases := []struct {
		configs map[string]any
	}{
		{
			configs: map[string]any{},
		},
		{
			configs: map[string]any{
				"datasource": "t",
			},
		},
		{
			configs: map[string]any{
				"datasource":       "t",
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
				"datasource":  "t",
				"brokers":     "localhost:9092",
				"offsetReset": "foo",
			},
		},
	}
	ctx := mockContext.NewMockContext("1", "2")
	for _, tc := range testcases {
		require.Error(t, ks.Provision(ctx, tc.configs))
	}
	configs := map[string]any{
		"datasource": "t",
		"brokers":    "localhost:9092",
	}
	require.NoError(t, ks.Provision(ctx, configs))
	for i := mockErrStart + 1; i < mockErrEnd; i++ {
		failpoint.Enable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr", fmt.Sprintf("return(%v)", i))
		require.Error(t, ks.Provision(ctx, configs), i)
	}
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr")
}

func TestKafkaSourceOffsetReset(t *testing.T) {
	ctx := mockContext.NewMockContext("1", "2")
	testcases := []struct {
		name        string
		groupID     string
		offsetReset string
		expected    int64
	}{
		{
			name:     "no groupID and no offsetReset, defaults to latest",
			expected: kafkago.LastOffset,
		},
		{
			name:        "no groupID and explicit earliest",
			offsetReset: OffsetResetEarliest,
			expected:    kafkago.FirstOffset,
		},
		{
			name:     "groupID and no offsetReset, leaves StartOffset unset",
			groupID:  "test-group",
			expected: 0,
		},
		{
			name:        "groupID and explicit latest",
			groupID:     "test-group",
			offsetReset: OffsetResetLatest,
			expected:    kafkago.LastOffset,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			configs := map[string]any{
				"datasource": "t",
				"brokers":    "localhost:9092",
			}
			if tc.groupID != "" {
				configs["groupID"] = tc.groupID
			}
			if tc.offsetReset != "" {
				configs["offsetReset"] = tc.offsetReset
			}
			ks := &KafkaSource{}
			require.NoError(t, ks.Provision(ctx, configs))
			if tc.groupID != "" {
				// StartOffset 0 = unset; kafka-go treats it as FirstOffset for consumer groups.
				require.Equal(t, tc.expected, ks.sc.GetReaderConfig().StartOffset)
			}
			require.NoError(t, ks.Connect(ctx, func(status string, message string) {
				// do nothing
			}))
			if tc.groupID == "" {
				// Without groupID, kafka-go's StartOffset config is unused;
				// the reader actually starts from the offset set in Connect.
				require.Equal(t, tc.expected, ks.reader.Offset())
			}
			require.NoError(t, ks.Close(ctx))
		})
	}
}

func TestKafkaPassword(t *testing.T) {
	testcase := []struct {
		oldPassword    string
		newPassword    string
		expectPassword string
	}{
		{
			oldPassword:    "",
			newPassword:    "",
			expectPassword: "",
		},
		{
			oldPassword:    "123",
			newPassword:    "",
			expectPassword: "123",
		},
		{
			oldPassword:    "",
			newPassword:    "123",
			expectPassword: "123",
		},
		{
			oldPassword:    "123",
			newPassword:    "1234",
			expectPassword: "1234",
		},
	}
	for _, tc := range testcase {
		sconf := &saslConf{
			OldPassword:  tc.oldPassword,
			SaslPassword: tc.newPassword,
		}
		sconf.resolvePassword()
		require.Equal(t, tc.expectPassword, sconf.SaslPassword)
	}
}

func TestMessageMeta(t *testing.T) {
	testcases := []struct {
		name   string
		msg    kafkago.Message
		expect map[string]any
	}{
		{
			name: "full",
			msg: kafkago.Message{
				Topic:     "demo",
				Partition: 2,
				Offset:    10,
				Key:       []byte("device1"),
				Headers: []kafkago.Header{
					{Key: "traceId", Value: []byte("abc")},
					{Key: "type", Value: []byte("temp")},
				},
				Value: []byte(`{"a":1}`),
			},
			expect: map[string]any{
				"topic":     "demo",
				"partition": 2,
				"offset":    int64(10),
				"key":       "device1",
				"headers": map[string]any{
					"traceId": "abc",
					"type":    "temp",
				},
			},
		},
		{
			name: "no key and headers",
			msg: kafkago.Message{
				Topic:  "demo",
				Offset: 0,
				Value:  []byte(`{"a":1}`),
			},
			expect: map[string]any{
				"topic":     "demo",
				"partition": 0,
				"offset":    int64(0),
			},
		},
		{
			name: "empty key and duplicated header",
			msg: kafkago.Message{
				Topic: "demo",
				Key:   []byte{},
				Headers: []kafkago.Header{
					{Key: "h", Value: []byte("1")},
					{Key: "h", Value: []byte("2")},
				},
			},
			expect: map[string]any{
				"topic":     "demo",
				"partition": 0,
				"offset":    int64(0),
				"key":       "",
				"headers": map[string]any{
					"h": "2",
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, messageMeta(tc.msg))
		})
	}
}
