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

//go:build trace || !core

package tracer

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
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
	root, err := s.GetTraceById("t0")
	require.NoError(t, err)
	require.Nil(t, root)
	// span1 should be root span
	s1, err := s.GetTraceById("t1")
	require.NoError(t, err)
	require.Equal(t, span1, s1)
}

func TestLocalTraceByRule(t *testing.T) {
	conf.InitConf()
	s := newLocalSpanMemoryStorage(2)
	span0 := &LocalSpan{
		TraceID: "t0",
		SpanID:  "s0",
		RuleID:  "r1",
	}
	span1 := &LocalSpan{
		TraceID: "t1",
		SpanID:  "s1",
		RuleID:  "r1",
	}
	require.NoError(t, s.saveSpan(span0))
	require.NoError(t, s.saveSpan(span1))
	ids, err := s.GetTraceByRuleID("r1", 0)
	require.NoError(t, err)
	require.Len(t, ids, 2)
	ids, err = s.GetTraceByRuleID("r1", 0)
	require.NoError(t, err)
	require.Len(t, ids, 2)
}

func TestLocalStorageTraceManager(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	os.Remove(filepath.Join(dataDir, "trace.db"))
	require.NoError(t, store.SetupDefault(dataDir))
	spanStorage := newSqlspanStorage()
	span0 := &LocalSpan{
		TraceID: "t0",
		SpanID:  "s0",
		RuleID:  "r1",
	}
	span1 := &LocalSpan{
		TraceID: "t1",
		SpanID:  "s1",
		RuleID:  "r1",
	}
	require.NoError(t, spanStorage.saveLocalSpan(span0))
	require.NoError(t, spanStorage.saveLocalSpan(span1))
	got, err := spanStorage.loadTraceByRuleID("r1")
	require.NoError(t, err)
	require.Equal(t, []string{"t0", "t1"}, got)
	gotSpan, err := spanStorage.GetTraceById("t1")
	require.NoError(t, err)
	require.Equal(t, gotSpan, span1)
}

func TestLocalStorageTraceManagerErr(t *testing.T) {
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	os.Remove(filepath.Join(dataDir, "trace.db"))
	require.NoError(t, store.SetupDefault(dataDir))
	spanStorage := newSqlspanStorage()
	span0 := &LocalSpan{
		TraceID: "t0",
		SpanID:  "s0",
		RuleID:  "r1",
	}
	span1 := &LocalSpan{
		TraceID: "t1",
		SpanID:  "s1",
		RuleID:  "r1",
	}
	spanStorage.saveLocalSpan(span1)
	for i := 1; i <= 2; i++ {
		failpointPath := fmt.Sprintf("github.com/lf-edge/ekuiper/v2/pkg/tracer/injectTraceErr_%v", i)
		failpoint.Enable(failpointPath, "return(true)")
		err := spanStorage.saveLocalSpan(span0)
		require.Error(t, err, failpointPath)
		failpoint.Disable(failpointPath)
	}
	for i := 3; i <= 5; i++ {
		failpointPath := fmt.Sprintf("github.com/lf-edge/ekuiper/v2/pkg/tracer/injectTraceErr_%v", i)
		failpoint.Enable(failpointPath, "return(true)")
		_, err := spanStorage.loadTraceByRuleID("r1")
		require.Error(t, err)
		failpoint.Disable(failpointPath)
	}

	for i := 6; i <= 8; i++ {
		failpointPath := fmt.Sprintf("github.com/lf-edge/ekuiper/v2/pkg/tracer/injectTraceErr_%v", i)
		failpoint.Enable(failpointPath, "return(true)")
		_, err := spanStorage.loadTraceByTraceID("t1")
		require.Error(t, err, failpointPath)
		failpoint.Disable(failpointPath)
	}
}
