// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	_ "github.com/taosdata/driver-go/v2/taosSql"
	"reflect"
	"strings"
)

type (
	taosConfig struct {
		ProvideTs   bool     `json:"provideTs"`
		Port        int      `json:"port"`
		Ip          string   `json:"ip"` // To be deprecated
		Host        string   `json:"host"`
		User        string   `json:"user"`
		Password    string   `json:"password"`
		Database    string   `json:"database"`
		Table       string   `json:"table"`
		TsFieldName string   `json:"tsFieldName"`
		Fields      []string `json:"fields"`
		STable      string   `json:"sTable"`
		TagFields   []string `json:"tagFields"`
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
			return "", fmt.Errorf("Timestamp field not found : %s.", t.TsFieldName)
		} else {
			keys = append(keys, t.TsFieldName)
			vals = append(vals, fmt.Sprintf(`"%v"`, v))
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

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(keys, ","))
	if sTable != "" {
		sqlStr += " using " + sTable
	}
	if len(tags) != 0 {
		sqlStr += " tags (" + strings.Join(tags, ",") + ")"
	}
	sqlStr += " values (" + strings.Join(vals, ",") + ");"
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

func (m *taosSink) writeToDB(ctx api.StreamContext, mapData map[string]interface{}) error {
	sql, err := m.conf.buildSql(ctx, mapData)
	if nil != err {
		return err
	}
	ctx.GetLogger().Debugf(sql)
	rows, err := m.db.Query(sql)
	if err != nil {
		return err
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
