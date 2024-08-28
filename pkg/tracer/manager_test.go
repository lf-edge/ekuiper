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

package tracer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestLocalSpan(t *testing.T) {
	conf.InitConf()
	s := newLocalSpanMemoryStorage(1)
	span0 := &LocalSpan{
		TraceID: "t0",
		SpanID:  "s0",
	}
	span1 := &LocalSpan{
		TraceID: "t1",
		SpanID:  "s1",
	}
	require.NoError(t, s.saveSpan(span0))
	require.NoError(t, s.saveSpan(span1))
	require.Equal(t, 1, s.queue.Len())
	// span0 should be dropped
	require.Nil(t, s.GetTraceById("t0"))
	// span1 should be root span
	require.Equal(t, span1, s.GetTraceById("t1"))
}
