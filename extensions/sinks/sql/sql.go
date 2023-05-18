// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/extensions/sqldatabase/driver"
	"github.com/lf-edge/ekuiper/extensions/util"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type sqlConfig struct {
	Url            string   `json:"url"`
	Table          string   `json:"table"`
	Fields         []string `json:"fields"`
	DataTemplate   string   `json:"dataTemplate"`
	TableDataField string   `json:"tableDataField"`
	DataField      string   `json:"dataField"`
	RowkindField   string   `json:"rowkindField"`
	KeyField       string   `json:"keyField"`
}

func (t *sqlConfig) buildInsertSql(ctx api.StreamContext, mapData map[string]interface{}) ([]string, string, error) {
	keys, vals, err := t.getKeyValues(ctx, mapData)
	if err != nil {
		return keys, "", err
	}
	if strings.HasPrefix(strings.ToLower(t.Url), "dm://") {
		for i, key := range keys {
			keys[i] = fmt.Sprintf(`"%v"`, key)
		}
	}
	sqlStr := "(" + strings.Join(vals, ",") + ")"
	return keys, sqlStr, nil
}

func (t *sqlConfig) getKeyValues(ctx api.StreamContext, mapData map[string]interface{}) ([]string, []string, error) {
	if 0 == len(mapData) {
		return nil, nil, fmt.Errorf("data is empty.")
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
	return keys, vals, nil
}

type sqlSink struct {
	conf *sqlConfig
	// The db connection instance
	db *sql.DB
}

func (m *sqlSink) Configure(props map[string]interface{}) error {
	cfg := &sqlConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.DataField == "" {
		cfg.DataField = cfg.TableDataField
	}
	if cfg.Url == "" {
		return fmt.Errorf("property Url is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("property Table is required")
	}
	if cfg.RowkindField != "" && cfg.KeyField == "" {
		return fmt.Errorf("keyField is required when rowkindField is set")
	}
	m.conf = cfg
	return nil
}

func (m *sqlSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debugf("Opening sql sink")
	db, err := util.FetchDBToOneNode(util.GlobalPool, m.conf.Url)
	if err != nil {
		logger.Errorf("support build tags are %v", driver.KnownBuildTags())
		return err
	}
	m.db = db
	return
}

func (m *sqlSink) writeToDB(ctx api.StreamContext, sqlStr *string) error {
	ctx.GetLogger().Debugf(*sqlStr)
	r, err := m.db.Exec(*sqlStr)
	if err != nil {
		return fmt.Errorf("%s: %s", errorx.IOErr, err.Error())
	}
	d, err := r.RowsAffected()
	if err != nil {
		ctx.GetLogger().Errorf("get rows affected error: %s", err.Error())
	}
	ctx.GetLogger().Debugf("Rows affected: %d", d)
	return nil
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
	} else {
		tm, _, err := transform.TransItem(item, m.conf.DataField, m.conf.Fields)
		if err != nil {
			return fmt.Errorf("fail to 1 transform data %v for error %v!!!!!", item, err)
		}
		item = tm
	}

	var (
		table string
		err   error
	)
	switch v := item.(type) {
	case map[string]interface{}:
		table, err = ctx.ParseTemplate(m.conf.Table, v)
		if err != nil {
			ctx.GetLogger().Errorf("parse template for table %s error: %v", m.conf.Table, err)
			return err
		}
	case []map[string]interface{}:
		if len(v) == 0 {
			ctx.GetLogger().Warnf("empty data array")
			return nil
		}
		table, err = ctx.ParseTemplate(m.conf.Table, v[0])
		if err != nil {
			ctx.GetLogger().Errorf("parse template for table %s error: %v", m.conf.Table, err)
			return err
		}
	}

	var keys []string = nil
	var values []string = nil
	var vars string

	if m.conf.RowkindField == "" {
		switch v := item.(type) {
		case []map[string]interface{}:
			for _, mapData := range v {
				keys, vars, err = m.conf.buildInsertSql(ctx, mapData)
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
			keys, vars, err = m.conf.buildInsertSql(ctx, v)
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

				keys, vars, err = m.conf.buildInsertSql(ctx, mapData)
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
	} else {
		switch d := item.(type) {
		case []map[string]interface{}:
			for _, el := range d {
				err := m.save(ctx, table, el)
				if err != nil {
					ctx.GetLogger().Error(err)
				}
			}
		case map[string]interface{}:
			err := m.save(ctx, table, d)
			if err != nil {
				return err
			}
		case []interface{}:
			for _, vv := range d {
				el, ok := vv.(map[string]interface{})
				if !ok {
					ctx.GetLogger().Errorf("unsupported type: %T", vv)
					return fmt.Errorf("unsupported type: %T", vv)
				}
				err := m.save(ctx, table, el)
				if err != nil {
					ctx.GetLogger().Error(err)
				}
			}
		default:
			return fmt.Errorf("unrecognized format of %s", item)
		}
		return nil
	}
}

func (m *sqlSink) Close(_ api.StreamContext) error {
	if m.db != nil {
		return util.ReturnDBFromOneNode(util.GlobalPool, m.conf.Url)
	}
	return nil
}

// save save updatable data only to db
func (m *sqlSink) save(ctx api.StreamContext, table string, data map[string]interface{}) error {
	rowkind := ast.RowkindInsert
	c, ok := data[m.conf.RowkindField]
	if ok {
		rowkind, ok = c.(string)
		if !ok {
			return fmt.Errorf("rowkind field %s is not a string in data %v", m.conf.RowkindField, data)
		}
		if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete {
			return fmt.Errorf("invalid rowkind %s", rowkind)
		}
	}
	var sqlStr string
	switch rowkind {
	case ast.RowkindInsert:
		keys, vars, err := m.conf.buildInsertSql(ctx, data)
		if err != nil {
			return err
		}
		values := []string{vars}
		if keys != nil {
			sqlStr = fmt.Sprintf("INSERT INTO %s (%s) values ", table, strings.Join(keys, ",")) + strings.Join(values, ",") + ";"
		}
	case ast.RowkindUpdate:
		keyval, ok := data[m.conf.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", m.conf.KeyField, data)
		}
		keys, vals, err := m.conf.getKeyValues(ctx, data)
		if err != nil {
			return err
		}
		sqlStr = fmt.Sprintf("UPDATE %s SET ", table)
		for i, key := range keys {
			if i != 0 {
				sqlStr += ","
			}
			sqlStr += fmt.Sprintf("%s=%s", key, vals[i])
		}
		if _, ok := keyval.(string); ok {
			sqlStr += fmt.Sprintf(" WHERE %s = \"%s\";", m.conf.KeyField, keyval)
		} else {
			sqlStr += fmt.Sprintf(" WHERE %s = %v;", m.conf.KeyField, keyval)
		}
	case ast.RowkindDelete:
		keyval, ok := data[m.conf.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", m.conf.KeyField, data)
		}
		if _, ok := keyval.(string); ok {
			sqlStr = fmt.Sprintf("DELETE FROM %s WHERE %s = \"%s\";", table, m.conf.KeyField, keyval)
		} else {
			sqlStr = fmt.Sprintf("DELETE FROM %s WHERE %s = %v;", table, m.conf.KeyField, keyval)
		}
	default:
		return fmt.Errorf("invalid rowkind %s", rowkind)
	}
	return m.writeToDB(ctx, &sqlStr)
}

func Sql() api.Sink {
	return &sqlSink{}
}
