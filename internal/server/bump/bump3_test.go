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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/store"
)

func TestRewriteSinkConf(t *testing.T) {
	conf.IsTesting = true
	require.NoError(t, conf.WriteCfgIntoKVStorage("sinks", "sql", "conf1", map[string]interface{}{
		"url": "123",
	}))
	require.NoError(t, rewriteSQLSinkConfiguration())
	d, err := conf.GetCfgFromKVStorage("sinks", "sql", "conf1")
	require.NoError(t, err)
	v, ok := d["sinks.sql.conf1"]["dburl"]
	require.True(t, ok)
	require.Equal(t, "123", v)
}

func TestRewriteSQLConf(t *testing.T) {
	testcases := []struct {
		oldCfg   *OriginSqlSourceCfg
		expected *TargetSqlSourceCfg
	}{
		{
			oldCfg: &OriginSqlSourceCfg{
				DBUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
			},
			expected: &TargetSqlSourceCfg{
				DbUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
			},
		},
		{
			oldCfg: &OriginSqlSourceCfg{
				DBUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
				InternalSqlQueryCfg: &OriginInternalSqlQueryCfg{
					Table:              "t",
					Limit:              10,
					IndexFieldName:     "a",
					IndexFieldValue:    1,
					IndexFieldDataType: "bigint",
				},
			},
			expected: &TargetSqlSourceCfg{
				DbUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
				InternalSqlQueryCfg: &TargetInternalSqlQueryCfg{
					Table: "t",
					Limit: 10,
					IndexFields: []*store.IndexField{
						{
							IndexFieldName:     "a",
							IndexFieldValue:    float64(1),
							IndexFieldDataType: "bigint",
						},
					},
				},
			},
		},
		{
			oldCfg: &OriginSqlSourceCfg{
				DBUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
				TemplateSqlQueryCfg: &OriginTemplateSqlQueryCfg{
					TemplateSQL:        "select * from t",
					IndexFieldName:     "a",
					IndexFieldValue:    1,
					IndexFieldDataType: "bigint",
				},
			},
			expected: &TargetSqlSourceCfg{
				DbUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
				TemplateSqlQueryCfg: &TargetTemplateSqlQueryCfg{
					TemplateSQL: "select * from t",
					IndexFields: []*store.IndexField{
						{
							IndexFieldName:     "a",
							IndexFieldValue:    float64(1),
							IndexFieldDataType: "bigint",
						},
					},
				},
			},
		},
		{
			oldCfg: &OriginSqlSourceCfg{
				DBUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
				InternalSqlQueryCfg: &OriginInternalSqlQueryCfg{
					Table:              "t",
					Limit:              10,
					IndexFieldName:     "a",
					IndexFieldValue:    1,
					IndexFieldDataType: "bigint",
					IndexFields: []*store.IndexField{
						{
							IndexFieldName:     "b",
							IndexFieldValue:    1,
							IndexFieldDataType: "bigint",
						},
					},
				},
				TemplateSqlQueryCfg: &OriginTemplateSqlQueryCfg{
					TemplateSQL:        "select * from t",
					IndexFieldName:     "a",
					IndexFieldValue:    1,
					IndexFieldDataType: "bigint",
					IndexFields: []*store.IndexField{
						{
							IndexFieldName:     "b",
							IndexFieldValue:    1,
							IndexFieldDataType: "bigint",
						},
					},
				},
			},
			expected: &TargetSqlSourceCfg{
				DbUrl:    "123",
				Interval: cast.DurationConf(10 * time.Second),
				InternalSqlQueryCfg: &TargetInternalSqlQueryCfg{
					Table: "t",
					Limit: 10,
					IndexFields: []*store.IndexField{
						{
							IndexFieldName:     "b",
							IndexFieldValue:    float64(1),
							IndexFieldDataType: "bigint",
						},
						{
							IndexFieldName:     "a",
							IndexFieldValue:    float64(1),
							IndexFieldDataType: "bigint",
						},
					},
				},
				TemplateSqlQueryCfg: &TargetTemplateSqlQueryCfg{
					TemplateSQL: "select * from t",
					IndexFields: []*store.IndexField{
						{
							IndexFieldName:     "b",
							IndexFieldValue:    float64(1),
							IndexFieldDataType: "bigint",
						},
						{
							IndexFieldName:     "a",
							IndexFieldValue:    float64(1),
							IndexFieldDataType: "bigint",
						},
					},
				},
			},
		},
	}

	for index, tc := range testcases {
		require.NoError(t, conf.ClearKVStorage())
		originData, err := json.Marshal(tc.oldCfg)
		require.NoError(t, err)
		m := make(map[string]interface{})
		require.NoError(t, json.Unmarshal(originData, &m))
		cfgKey := fmt.Sprintf("%v", index)
		require.NoError(t, conf.WriteCfgIntoKVStorage("sources", "sql", cfgKey, m))
		require.NoError(t, rewriteSQLSourceConfiguration())
		gotm, err := conf.GetCfgFromKVStorage("sources", "sql", cfgKey)
		require.NoError(t, err)
		tcfg := &TargetSqlSourceCfg{}
		cast.MapToStruct(gotm[fmt.Sprintf("sources.sql.%s", cfgKey)], tcfg)
		require.Equal(t, tc.expected, tcfg)
	}
}

type TargetSqlSourceCfg struct {
	DbUrl               string                     `json:"dburl"`
	Interval            cast.DurationConf          `json:"interval"`
	InternalSqlQueryCfg *TargetInternalSqlQueryCfg `json:"internalSqlQueryCfg,omitempty"`
	TemplateSqlQueryCfg *TargetTemplateSqlQueryCfg `json:"templateSqlQueryCfg,omitempty"`
}

type TargetInternalSqlQueryCfg struct {
	Table       string              `json:"table"`
	Limit       int                 `json:"limit"`
	IndexFields []*store.IndexField `json:"indexFields"`
}

type TargetTemplateSqlQueryCfg struct {
	TemplateSQL string              `json:"templateSql"`
	IndexFields []*store.IndexField `json:"indexFields"`
}
