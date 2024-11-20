// Copyright 2024 EMQ Technologies Co., Ltd.
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

package bump

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestBumpTo4(t *testing.T) {
	conf.IsTesting = true
	require.NoError(t, conf.WriteCfgIntoKVStorage("sources", "sql", "sql1", map[string]interface{}{
		"url":      "123",
		"cacheTtl": 1000,
	}))
	require.NoError(t, bumpFrom3TO4())
	mm, err := conf.GetCfgFromKVStorage("sources", "sql", "sql1")
	require.NoError(t, err)
	for _, props := range mm {
		require.Equal(t, map[string]interface{}{
			"dburl":    "123",
			"cacheTtl": "1s",
		}, props)
		break
	}
}
