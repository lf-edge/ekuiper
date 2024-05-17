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

package node

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestNewSinkNode(t *testing.T) {
	ctx := mockContext.NewMockContext("testSink", "sink")
	tests := []struct {
		name           string
		sc             *conf.SinkConf
		isRetry        bool
		resendInterval int
		bufferLength   int
	}{
		{
			name: "normal sink",
			sc: &conf.SinkConf{
				ResendInterval: 100,
			},
			isRetry:        false,
			resendInterval: 0,
			bufferLength:   1024,
		},
		{
			name: "linear cache sink",
			sc: &conf.SinkConf{
				ResendInterval:       100,
				EnableCache:          true,
				MemoryCacheThreshold: 10,
			},
			isRetry:        false,
			resendInterval: 100,
			bufferLength:   10,
		},
		{
			name: "retry cache normal sink",
			sc: &conf.SinkConf{
				ResendInterval:       100,
				EnableCache:          true,
				MemoryCacheThreshold: 10,
				ResendAlterQueue:     true,
			},
			isRetry: false,
			// resend interval is set but no use
			resendInterval: 100,
			bufferLength:   1024,
		},
		{
			name: "retry cache resend sink",
			sc: &conf.SinkConf{
				ResendInterval:       100,
				EnableCache:          true,
				MemoryCacheThreshold: 10,
				ResendAlterQueue:     true,
			},
			isRetry: true,
			// resend interval is set but no use
			resendInterval: 100,
			bufferLength:   10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newSinkNode(ctx, "test", def.RuleOption{
				BufferLength: 1024,
			}, 1, tt.sc, tt.isRetry)
			assert.Equal(t, tt.resendInterval, n.resendInterval, "resend interval")
			assert.Equal(t, tt.bufferLength, cap(n.input))
		})
	}
}
