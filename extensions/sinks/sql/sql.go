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
	"fmt"
	"github.com/lf-edge/ekuiper/extensions/sqldatabase/driver"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/xo/dburl"
	"reflect"
	"strings"
)

type sqlConfig struct {
	Url    string   `json:"url"`
	Table  string   `json:"table"`
	Fields []string `json:"fields"`
}

func (t *sqlConfig) buildSql(ctx api.StreamContext, mapData map[string]interface{}) (string, error) {
	if 0 == len(mapData) {
		return "", fmt.Errorf("data is empty.")
	}
	logger := ctx.GetLogger()
	var (
		table      string
		keys, vals []string
		err        error
	)
	table, err = ctx.ParseTemplate(t.Table, mapData)
	if err != nil {
		logger.Errorf("parse template for table %s error: %v", t.Table, err)
		return "", err
	}

	if len(t.Fields) != 0 {
		for _, k := range t.Fields {
			if v, ok := mapData[k]; ok {
				keys = append(keys, k)
				if reflect.String == reflect.TypeOf(v).Kind() {
					vals = append(vals, fmt.Sprintf("'%v'", v))
				} else {
					vals = append(vals, fmt.Sprintf(`%v`, v))
				}
			} else {
				logger.Warnln("not found field:", k)
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

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(keys, ","))
	sqlStr += " values (" + strings.Join(vals, ",") + ");"
	return sqlStr, nil
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

func (m *sqlSink) writeToDB(ctx api.StreamContext, mapData map[string]interface{}) error {
	sqlStr, err := m.conf.buildSql(ctx, mapData)
	if nil != err {
		return err
	}
	ctx.GetLogger().Debugf(sqlStr)
	rows, err := m.db.Query(sqlStr)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (m *sqlSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("sql sink receive %s", item)

	switch v := item.(type) {
	case []map[string]interface{}:
		var err error
		for _, mapData := range v {
			e := m.writeToDB(ctx, mapData)
			if e != nil {
				err = e
			}
		}
		return err
	case map[string]interface{}:
		return m.writeToDB(ctx, v)
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
