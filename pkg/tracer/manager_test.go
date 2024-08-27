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
	s := newLocalSpanMemoryStorage(4)
	span0 := &LocalSpan{
		TraceID: "t0",
		SpanID:  "s0",
	}
	require.NoError(t, s.saveSpan(span0))
	//   		s1
	// 		s2   	s3
	// 	s4
	span1 := &LocalSpan{
		TraceID: "t1",
		SpanID:  "s1",
	}
	span2 := &LocalSpan{
		TraceID:      "t1",
		SpanID:       "s2",
		ParentSpanID: "s1",
	}
	span3 := &LocalSpan{
		TraceID:      "t1",
		SpanID:       "s3",
		ParentSpanID: "s1",
	}
	span4 := &LocalSpan{
		TraceID:      "t1",
		SpanID:       "s4",
		ParentSpanID: "s2",
	}
	require.NoError(t, s.saveSpan(span1))
	require.NoError(t, s.saveSpan(span2))
	require.NoError(t, s.saveSpan(span3))
	require.NoError(t, s.saveSpan(span4))
	require.Equal(t, 4, s.queue.Len())
	// span0 should be dropped
	require.Nil(t, s.GetTraceById("t0"))
	// span1 should be root span
	require.Equal(t, span1, s.GetTraceById("t1"))
}
