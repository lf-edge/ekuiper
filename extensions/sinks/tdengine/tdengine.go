// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"reflect"
	"strings"

	_ "github.com/taosdata/driver-go/v2/taosSql"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type (
	taosConfig struct {
		ProvideTs      bool     `json:"provideTs"`
		Port           int      `json:"port"`
		Ip             string   `json:"ip"` // To be deprecated
		Host           string   `json:"host"`
		User           string   `json:"user"`
		Password       string   `json:"password"`
		Database       string   `json:"database"`
		Table          string   `json:"table"`
		TsFieldName    string   `json:"tsFieldName"`
		Fields         []string `json:"fields"`
		STable         string   `json:"sTable"`
		TagFields      []string `json:"tagFields"`
		DataTemplate   string   `json:"dataTemplate"`
		TableDataField string   `json:"tableDataField"`
		DataField      string   `json:"dataField"`
	}
	taosSink struct {
		conf *taosConfig
		url  string
		db   *sql.DB
	}
)

func (t *taosConfig) delTsField() {
	var auxFields []string
	for _, v := range t.Fields {
		if v != t.TsFieldName {
			auxFields = append(auxFields, v)
		}
	}
	t.Fields = auxFields
}

func (t *taosConfig) buildSql(ctx api.StreamContext, mapData map[string]interface{}) (string, error) {
	if 0 == len(mapData) {
		return "", fmt.Errorf("data is empty.")
	}
	logger := ctx.GetLogger()
	var (
		table, sTable    string
		keys, vals, tags []string
		err              error
	)
	table, err = ctx.ParseTemplate(t.Table, mapData)
	if err != nil {
		logger.Errorf("parse template for table %s error: %v", t.Table, err)
		return "", err
	}
	sTable, err = ctx.ParseTemplate(t.STable, mapData)
	if err != nil {
		logger.Errorf("parse template for sTable %s error: %v", t.STable, err)
		return "", err
	}

	if t.ProvideTs {
		if v, ok := mapData[t.TsFieldName]; !ok {
			return "", fmt.Errorf("timestamp field not found : %s", t.TsFieldName)
		} else {
			keys = append(keys, t.TsFieldName)
			timeStamp, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
			if err != nil {
				return "", fmt.Errorf("timestamp field can not convert to int64 : %v", v)
			}
			vals = append(vals, fmt.Sprintf(`%v`, timeStamp))
		}
	} else {
		vals = append(vals, "now")
		keys = append(keys, t.TsFieldName)
	}

	if len(t.Fields) != 0 {
		for _, k := range t.Fields {
			if k == t.TsFieldName {
				continue
			}
			if v, ok := mapData[k]; ok {
				keys = append(keys, k)
				if reflect.String == reflect.TypeOf(v).Kind() {
					vals = append(vals, fmt.Sprintf(`"%v"`, v))
				} else {
					vals = append(vals, fmt.Sprintf(`%v`, v))
				}
			} else {
				logger.Warnln("not found field:", k)
			}
		}
	} else {
		for k, v := range mapData {
			if k == t.TsFieldName {
				continue
			}
			keys = append(keys, k)
			if reflect.String == reflect.TypeOf(v).Kind() {
				vals = append(vals, fmt.Sprintf(`"%v"`, v))
			} else {
				vals = append(vals, fmt.Sprintf(`%v`, v))
			}
		}
	}

	if len(t.TagFields) > 0 {
		for _, v := range t.TagFields {
			switch mapData[v].(type) {
			case string:
				tags = append(tags, fmt.Sprintf(`"%s"`, mapData[v]))
			default:
				tags = append(tags, fmt.Sprintf(`%v`, mapData[v]))
			}
		}
	}

	sqlStr := fmt.Sprintf("%s (%s)", table, strings.Join(keys, ","))
	if sTable != "" {
		sqlStr += " using " + sTable
	}
	if len(tags) != 0 {
		sqlStr += " tags (" + strings.Join(tags, ",") + ")"
	}
	sqlStr += " values (" + strings.Join(vals, ",") + ")"
	return sqlStr, nil
}

func (m *taosSink) Configure(props map[string]interface{}) error {
	cfg := &taosConfig{
		User:     "root",
		Password: "taosdata",
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Ip != "" {
		conf.Log.Warnf("Deprecated: Tdengine sink ip property is deprecated, use host instead.")
		if cfg.Host == "" {
			cfg.Host = cfg.Ip
		}
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.User == "" {
		return fmt.Errorf("propert user is required.")
	}
	if cfg.Password == "" {
		return fmt.Errorf("propert password is required.")
	}
	if cfg.Database == "" {
		return fmt.Errorf("property database is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("property table is required")
	}
	if cfg.TsFieldName == "" {
		return fmt.Errorf("property TsFieldName is required")
	}
	if cfg.STable != "" && len(cfg.TagFields) == 0 {
		return fmt.Errorf("property tagFields is required when sTable is set")
	}
	m.url = fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s`, cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	if cfg.DataField == "" {
		cfg.DataField = cfg.TableDataField
	}
	m.conf = cfg
	return nil
}

func (m *taosSink) Open(ctx api.StreamContext) (err error) {
	ctx.GetLogger().Debug("Opening tdengine sink")
	m.db, err = sql.Open("taosSql", m.url)
	return err
}

func (m *taosSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("tdengine sink receive %s", item)
	if m.conf.DataTemplate != "" {
		jsonBytes, _, err := ctx.TransformOutput(item)
		if err != nil {
			return err
		}
		tm := make(map[string]interface{})
		err = json.Unmarshal(jsonBytes, &tm)
		if err != nil {
			return fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
		}
		item = tm
	} else {
		tm, _, err := transform.TransItem(item, m.conf.DataField, m.conf.Fields)
		if err != nil {
			return fmt.Errorf("fail to transform data %v for error %v", item, err)
		}
		item = tm
	}

	switch v := item.(type) {
	case []map[string]interface{}:
		strSli := make([]string, len(v))
		for _, mapData := range v {
			str, err := m.conf.buildSql(ctx, mapData)
			if err != nil {
				ctx.GetLogger().Errorf("tdengine sink build sql error %v for data", err, mapData)
				return err
			}
			strSli = append(strSli, str)
		}
		if len(strSli) > 0 {
			strBatch := strings.Join(strSli, " ")
			return m.writeToDB(ctx, &strBatch)
		}
		return nil
	case map[string]interface{}:
		strBatch, err := m.conf.buildSql(ctx, v)
		if err != nil {
			ctx.GetLogger().Errorf("tdengine sink build sql error %v for data", err, v)
			return err
		}
		return m.writeToDB(ctx, &strBatch)
	case []interface{}:
		strSli := make([]string, len(v))
		for _, data := range v {
			mapData, ok := data.(map[string]interface{})
			if !ok {
				ctx.GetLogger().Errorf("unsupported type: %T", data)
				return fmt.Errorf("unsupported type: %T", data)
			}

			str, err := m.conf.buildSql(ctx, mapData)
			if err != nil {
				ctx.GetLogger().Errorf("tdengine sink build sql error %v for data", err, mapData)
				return err
			}
			strSli = append(strSli, str)
		}
		if len(strSli) > 0 {
			strBatch := strings.Join(strSli, " ")
			return m.writeToDB(ctx, &strBatch)
		}
		return nil
	default: // never happen
		return fmt.Errorf("unsupported type: %T", item)
	}
}

func (m *taosSink) writeToDB(ctx api.StreamContext, SqlVal *string) error {
	finalSql := "INSERT INTO " + *SqlVal + ";"
	ctx.GetLogger().Debugf(finalSql)
	rows, err := m.db.Query(finalSql)
	if err != nil {
		return fmt.Errorf("%s: %s", errorx.IOErr, err.Error())
	}
	rows.Close()
	return nil
}

func (m *taosSink) Close(ctx api.StreamContext) error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func Tdengine() api.Sink {
	return &taosSink{}
}
