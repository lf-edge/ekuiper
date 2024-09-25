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

package simulator

import (
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSourcePull(t *testing.T) {
	data := make([]map[string]any, 0)
	data = append(data, map[string]any{
		"a": 1,
	})
	props1 := map[string]any{
		"data": data,
		"loop": true,
	}
	s1 := &SimulatorSource{}
	ctx := mockContext.NewMockContext("1", "2")
	require.NoError(t, s1.Provision(ctx, props1))
	require.NoError(t, s1.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	recvData := make(chan any, 10)
	s1.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		recvData <- data
	}, func(ctx api.StreamContext, err error) {})
	expData := map[string]any{
		"a": 1,
	}
	require.Equal(t, expData, <-recvData)
	s1.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		recvData <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, expData, <-recvData)
	require.NoError(t, s1.Close(ctx))

	props2 := map[string]any{
		"data": data,
		"loop": false,
	}
	s2 := &SimulatorSource{}
	require.NoError(t, s2.Provision(ctx, props2))
	require.NoError(t, s2.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	s2.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		recvData <- data
	}, func(ctx api.StreamContext, err error) {})
	require.Equal(t, expData, <-recvData)
	s2.Pull(ctx, time.Now(), func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {}, func(ctx api.StreamContext, err error) {
		recvData <- err
	})
	require.NoError(t, s2.Close(ctx))
}
