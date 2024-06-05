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

package bump

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestBumpFrom1To2(t *testing.T) {
	conf.IsTesting = true
	prepareData(t)
	err := bumpFrom1TO2()
	require.NoError(t, err)
	assertData(t)
}

func prepareData(t *testing.T) {
	err := conf.WriteCfgIntoKVStorage("sinks", "influx2", "a", map[string]interface{}{
		"token": "123",
	})
	require.NoError(t, err)

	err = conf.WriteCfgIntoKVStorage("sinks", "sql", "a", map[string]interface{}{
		"url": "123",
	})
	require.NoError(t, err)

	err = conf.WriteCfgIntoKVStorage("sources", "sql", "a", map[string]interface{}{
		"url": "123",
	})
	require.NoError(t, err)

	err = conf.WriteCfgIntoKVStorage("sources", "kafka", "a", map[string]interface{}{
		"saslPassword": "123",
	})
	require.NoError(t, err)
}

func assertData(t *testing.T) {
	props, err := conf.GetCfgFromKVStorage("sources", "sql", "a")
	require.NoError(t, err)
	require.NotNil(t, props)
	prop, ok := props["sources.sql.a"]
	require.True(t, ok)

	_, ok = prop["url"]
	require.False(t, ok)
	v, ok := prop["dburl"]
	require.True(t, ok)
	require.Equal(t, "123", v)

	props, err = conf.GetCfgFromKVStorage("sources", "kafka", "a")
	require.NoError(t, err)
	require.NotNil(t, props)
	prop, ok = props["sources.kafka.a"]
	require.True(t, ok)

	_, ok = prop["saslPassword"]
	require.False(t, ok)
	v, ok = prop["password"]
	require.True(t, ok)
	require.Equal(t, "123", v)

	props, err = conf.GetCfgFromKVStorage("sinks", "influx2", "a")
	require.NoError(t, err)
	require.NotNil(t, prop)
	prop, ok = props["sinks.influx2.a"]
	require.True(t, ok)
	_, ok = prop["token"]
	require.False(t, ok)
	v, ok = prop["password"]
	require.True(t, ok)
	require.Equal(t, "123", v)

	props, err = conf.GetCfgFromKVStorage("sinks", "sql", "a")
	require.NoError(t, err)
	require.NotNil(t, prop)
	prop, ok = props["sinks.sql.a"]
	require.True(t, ok)
	_, ok = prop["url"]
	require.False(t, ok)
	v, ok = prop["dburl"]
	require.True(t, ok)
	require.Equal(t, "123", v)
}

func TestBump2Err(t *testing.T) {
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/rewriteErr", "return(1)")
	err := bumpFrom1TO2()
	require.Error(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/rewriteErr", "return(2)")
	err = bumpFrom1TO2()
	require.Error(t, err)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/server/bump/rewriteErr", "return(3)")
	err = bumpFrom1TO2()
	require.Error(t, err)

	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/server/bump/rewriteErr")
}
