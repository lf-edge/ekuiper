// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/extensions/sqldatabase/driver"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/xo/dburl"
	"reflect"
	"strings"
)

type sqlConfig struct {
	Url            string   `json:"url"`
	Table          string   `json:"table"`
	Fields         []string `json:"fields"`
	DataTemplate   string   `json:"dataTemplate"`
	TableDataField string   `json:"tableDataField"`
}

func (t *sqlConfig) buildSql(ctx api.StreamContext, mapData map[string]interface{}) ([]string, string, error) {
	if 0 == len(mapData) {
		return nil, "", fmt.Errorf("data is empty.")
	}
	logger := ctx.GetLogger()
	var keys, vals []string

	if len(t.Fields) != 0 {
		for _, k := range t.Fields {
			keys = append(keys, k)
			if v, ok := mapData[k]; ok {
				if reflect.String == reflect.TypeOf(v).Kind() {
					vals = append(vals, fmt.Sprintf("'%v'", v))
				} else {
					vals = append(vals, fmt.Sprintf(`%v`, v))
				}
			} else {
				logger.Warnln("not found field:", k)
				vals = append(vals, fmt.Sprintf(`NULL`))
			}
		}
	} else {
		for k, v := range mapData {
			keys = append(keys, k)
			if reflect.String == reflect.TypeOf(v).Kind() {
				vals = append(vals, fmt.Sprintf("'%v'", v))
			} else {
				vals = append(vals, fmt.Sprintf(`%v`, v))
			}
		}
	}

	sqlStr := "(" + strings.Join(vals, ",") + ")"
	return keys, sqlStr, nil
}

type sqlSink struct {
	conf *sqlConfig
	//The db connection instance
	db *sql.DB
}

func (m *sqlSink) Configure(props map[string]interface{}) error {
	cfg := &sqlConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Url == "" {
		return fmt.Errorf("property Url is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("property Table is required")
	}
	m.conf = cfg
	return nil
}

func (m *sqlSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debugf("Opening sql sink")

	db, err := dburl.Open(m.conf.Url)
	if err != nil {
		logger.Errorf("support build tags are %v", driver.KnownBuildTags())
		return err
	}
	m.db = db
	return
}

func (m *sqlSink) writeToDB(ctx api.StreamContext, sqlStr *string) error {
	ctx.GetLogger().Debugf(*sqlStr)
	rows, err := m.db.Query(*sqlStr)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (m *sqlSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("sql sink receive %s", item)
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
	}

	var table string
	var err error
	v, ok := item.(map[string]interface{})
	if ok {
		table, err = ctx.ParseTemplate(m.conf.Table, v)
		if err != nil {
			ctx.GetLogger().Errorf("parse template for table %s error: %v", m.conf.Table, err)
			return err
		}
		if m.conf.TableDataField != "" {
			item = v[m.conf.TableDataField]
		}
	}

	var keys []string = nil
	var values []string = nil
	var vars string

	switch v := item.(type) {
	case []map[string]interface{}:
		for _, mapData := range v {
			keys, vars, err = m.conf.buildSql(ctx, mapData)
			if err != nil {
				return err
			}
			values = append(values, vars)
		}
		if keys != nil {
			sqlStr := fmt.Sprintf("INSERT INTO %s (%s) values ", table, strings.Join(keys, ",")) + strings.Join(values, ",") + ";"
			return m.writeToDB(ctx, &sqlStr)
		}
		return nil
	case map[string]interface{}:
		keys, vars, err = m.conf.buildSql(ctx, v)
		if err != nil {
			return err
		}
		values = append(values, vars)
		if keys != nil {
			sqlStr := fmt.Sprintf("INSERT INTO %s (%s) values ", table, strings.Join(keys, ",")) + strings.Join(values, ",") + ";"
			return m.writeToDB(ctx, &sqlStr)
		}
		return nil
	case []interface{}:
		for _, data := range v {
			mapData, ok := data.(map[string]interface{})
			if !ok {
				ctx.GetLogger().Errorf("unsupported type: %T", data)
				return fmt.Errorf("unsupported type: %T", data)
			}

			keys, vars, err = m.conf.buildSql(ctx, mapData)
			if err != nil {
				ctx.GetLogger().Errorf("sql sink build sql error %v for data", err, mapData)
				return err
			}
			values = append(values, vars)
		}

		if keys != nil {
			sqlStr := fmt.Sprintf("INSERT INTO %s (%s) values ", table, strings.Join(keys, ",")) + strings.Join(values, ",") + ";"
			return m.writeToDB(ctx, &sqlStr)
		}
		return nil
	default: // never happen
		return fmt.Errorf("unsupported type: %T", item)
	}
}

func (m *sqlSink) Close(_ api.StreamContext) error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func Sql() api.Sink {
	return &sqlSink{}
}
