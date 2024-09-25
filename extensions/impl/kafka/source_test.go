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
	require.NoError(t, ks.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	require.NoError(t, ks.Close(ctx))

	for i := mockErrStart + 1; i < mockErrEnd; i++ {
		failpoint.Enable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr", fmt.Sprintf("return(%v)", i))
		require.Error(t, ks.Provision(ctx, configs), i)
	}
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/extensions/impl/kafka/kafkaErr")
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
