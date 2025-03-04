// Copyright 2025 EMQ Technologies Co., Ltd.
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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
)

func TestStartStopMetricsManager(t *testing.T) {
	conf.InitConf()
	ctx := context.Background()
	m := &MetricsDumpManager{
		dryRun: true,
	}
	require.NoError(t, m.init(ctx))
	m.Stop()
	require.False(t, m.enabeld)
	m.Start()
	require.True(t, m.enabeld)
}
