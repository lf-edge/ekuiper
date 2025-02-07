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

package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestMetricsDumpJob(t *testing.T) {
	conf.InitConf()
	m := &MetricsDumpManager{}
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.init(ctx))
	time.Sleep(10 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestExtractFilename(t *testing.T) {
	conf.InitConf()
	filename := `metrics.20250102-13.log`
	m := &MetricsDumpManager{}
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.init(ctx))
	defer cancel()
	filetime, err := m.extractFileTime(filename)
	require.NoError(t, err)
	require.NotNil(t, filetime)
}

func TestNeedGCFile(t *testing.T) {
	conf.InitConf()
	ctx, cancel := context.WithCancel(context.Background())
	m := &MetricsDumpManager{}
	require.NoError(t, m.init(ctx))
	defer cancel()
	needgc, err := m.needGCFile(`metrics.20240102-13.log`, time.Now())
	require.NoError(t, err)
	require.True(t, needgc)
}

func TestDumpMetrics(t *testing.T) {
	conf.InitConf()
	ctx, cancel := context.WithCancel(context.Background())
	m := &MetricsDumpManager{}
	require.NoError(t, m.init(ctx))
	defer cancel()
	require.NoError(t, m.dumpMetrics())
}

func TestIsFileIncludeMetricsTime(t *testing.T) {
	a := time.Now()
	require.True(t, isFileIncludeMetricsTime(a, a.Add(time.Minute)))
}
