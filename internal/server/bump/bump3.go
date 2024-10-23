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
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/store"
)

func bumpFrom2TO3() error {
	rewriteSQLSinkConfiguration()
	return rewriteSQLSourceConfiguration()
}

func rewriteSQLSinkConfiguration() error {
	keyProps, err := conf.GetCfgFromKVStorage("sinks", "sql", "")
	if err != nil {
		return err
	}
	for key, props := range keyProps {
		dbURLRaw, ok := props["url"]
		if ok {
			dbURL, ok := dbURLRaw.(string)
			if ok {
				props["dburl"] = dbURL
				_, _, confKey, _ := extractKey(key)
				conf.WriteCfgIntoKVStorage("sinks", "sql", confKey, props)
			}
		}
	}
	return nil
}

func rewriteSQLSourceConfiguration() error {
	keyProps, err := conf.GetCfgFromKVStorage("sources", "sql", "")
	if err != nil {
		return err
	}
	for key, props := range keyProps {
		cfg := &OriginSqlSourceCfg{}
		if err := cast.MapToStruct(props, cfg); err != nil || cfg == nil {
			continue
		}
		data := rewriteCfg(cfg)
		_, _, confKey, _ := extractKey(key)
		conf.WriteCfgIntoKVStorage("sources", "sql", confKey, data)
	}
	return nil
}

func rewriteCfg(cfg *OriginSqlSourceCfg) map[string]interface{} {
	m := make(map[string]interface{})
	m["dburl"] = cfg.DBUrl
	m["interval"] = time.Duration(cfg.Interval).String()
	if cfg.InternalSqlQueryCfg != nil {
		icfg := cfg.InternalSqlQueryCfg
		im := make(map[string]interface{})
		im["table"] = icfg.Table
		im["limit"] = icfg.Limit
		f := extractIndexField(icfg)
		if f != nil {
			icfg.IndexFields = append(icfg.IndexFields, f)
		}
		if len(icfg.IndexFields) > 0 {
			im["indexFields"] = icfg.IndexFields
		}
		m["internalSqlQueryCfg"] = im
	}
	if cfg.TemplateSqlQueryCfg != nil {
		tcfg := cfg.TemplateSqlQueryCfg
		tm := make(map[string]interface{})
		tm["templateSql"] = tcfg.TemplateSQL
		f := extractIndexField2(tcfg)
		if f != nil {
			tcfg.IndexFields = append(tcfg.IndexFields, f)
		}
		if len(tcfg.IndexFields) > 0 {
			tm["indexFields"] = tcfg.IndexFields
		}
		m["templateSqlQueryCfg"] = tm
	}
	bs, _ := json.Marshal(m)
	newm := make(map[string]interface{})
	json.Unmarshal(bs, &newm)
	return newm
}

func extractIndexField(icfg *OriginInternalSqlQueryCfg) *store.IndexField {
	if len(icfg.IndexFieldName) < 1 {
		return nil
	}
	f := &store.IndexField{
		IndexFieldName: icfg.IndexFieldName,
	}
	if icfg.IndexFieldValue != nil {
		f.IndexFieldValue = icfg.IndexFieldValue
	}
	if len(icfg.IndexFieldDataType) > 0 {
		f.IndexFieldDataType = icfg.IndexFieldDataType
	}
	if len(icfg.IndexFieldDateTimeFormat) > 0 {
		f.IndexFieldDateTimeFormat = icfg.IndexFieldDateTimeFormat
	}
	return f
}

func extractIndexField2(icfg *OriginTemplateSqlQueryCfg) *store.IndexField {
	if len(icfg.IndexFieldName) < 1 {
		return nil
	}
	f := &store.IndexField{
		IndexFieldName: icfg.IndexFieldName,
	}
	if icfg.IndexFieldValue != nil {
		f.IndexFieldValue = icfg.IndexFieldValue
	}
	if len(icfg.IndexFieldDataType) > 0 {
		f.IndexFieldDataType = icfg.IndexFieldDataType
	}
	if len(icfg.IndexFieldDateTimeFormat) > 0 {
		f.IndexFieldDateTimeFormat = icfg.IndexFieldDateTimeFormat
	}
	return f
}

// OriginSqlSourceCfg tends to rewrite index Field into index Fields
type OriginSqlSourceCfg struct {
	DBUrl               string                     `json:"dburl"`
	Interval            cast.DurationConf          `json:"interval"`
	InternalSqlQueryCfg *OriginInternalSqlQueryCfg `json:"internalSqlQueryCfg,omitempty"`
	TemplateSqlQueryCfg *OriginTemplateSqlQueryCfg `json:"templateSqlQueryCfg,omitempty"`
}

type OriginInternalSqlQueryCfg struct {
	Table                    string              `json:"table"`
	Limit                    int                 `json:"limit"`
	IndexFieldName           string              `json:"indexField"`
	IndexFieldValue          interface{}         `json:"indexValue"`
	IndexFieldDataType       string              `json:"indexFieldType"`
	IndexFieldDateTimeFormat string              `json:"dateTimeFormat"`
	IndexFields              []*store.IndexField `json:"indexFields"`
}

type OriginTemplateSqlQueryCfg struct {
	TemplateSQL              string              `json:"templateSql"`
	IndexFieldName           string              `json:"indexField"`
	IndexFieldValue          interface{}         `json:"indexValue"`
	IndexFieldDataType       string              `json:"indexFieldType"`
	IndexFieldDateTimeFormat string              `json:"dateTimeFormat"`
	IndexFields              []*store.IndexField `json:"indexFields"`
}
