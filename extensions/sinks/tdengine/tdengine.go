// Copyright 2021 EMQ Technologies Co., Ltd.
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

// +build plugins

package main

import (
	"database/sql"
	"encoding/json"
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
		Ip          string   `json:"ip"`
		User        string   `json:"user"`
		Password    string   `json:"password"`
		Database    string   `json:"database"`
		Table       string   `json:"table"`
		TsFieldName string   `json:"tsFieldName"`
		Fields      []string `json:"fields"`
	}
	taosSink struct {
		conf *taosConfig
		db   *sql.DB
	}
)

func (this *taosConfig) delTsField() {
	var auxFields []string
	for _, v := range this.Fields {
		if v != this.TsFieldName {
			auxFields = append(auxFields, v)
		}
	}
	this.Fields = auxFields
}

func (this *taosConfig) buildSql(ctx api.StreamContext, mapData map[string]interface{}) (string, error) {
	if 0 == len(mapData) {
		return "", fmt.Errorf("data is empty.")
	}
	if 0 == len(this.TsFieldName) {
		return "", fmt.Errorf("tsFieldName is empty.")
	}

	logger := ctx.GetLogger()
	var keys, vals []string

	if this.ProvideTs {
		if v, ok := mapData[this.TsFieldName]; !ok {
			return "", fmt.Errorf("Timestamp field not found : %s.", this.TsFieldName)
		} else {
			keys = append(keys, this.TsFieldName)
			vals = append(vals, fmt.Sprintf(`"%v"`, v))
			delete(mapData, this.TsFieldName)
			this.delTsField()
		}
	} else {
		vals = append(vals, "now")
		keys = append(keys, this.TsFieldName)
	}

	for _, k := range this.Fields {
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

	if 0 != len(this.Fields) {
		if len(this.Fields) < len(mapData) {
			logger.Warnln("some of values will be ignored.")
		}
		return fmt.Sprintf(`INSERT INTO %s (%s)VALUES(%s);`, this.Table, strings.Join(keys, `,`), strings.Join(vals, `,`)), nil
	}

	for k, v := range mapData {
		keys = append(keys, k)
		if reflect.String == reflect.TypeOf(v).Kind() {
			vals = append(vals, fmt.Sprintf(`"%v"`, v))
		} else {
			vals = append(vals, fmt.Sprintf(`%v`, v))
		}
	}
	if 0 != len(keys) {
		return fmt.Sprintf(`INSERT INTO %s (%s)VALUES(%s);`, this.Table, strings.Join(keys, `,`), strings.Join(vals, `,`)), nil
	}
	return "", nil
}

func (m *taosSink) Configure(props map[string]interface{}) error {
	cfg := &taosConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Ip == "" {
		cfg.Ip = "127.0.0.1"
		conf.Log.Infof("Not find IP conf, will use default value '127.0.0.1'.")
	}
	if cfg.User == "" {
		cfg.User = "root"
		conf.Log.Infof("Not find user conf, will use default value 'root'.")
	}
	if cfg.Password == "" {
		cfg.Password = "taosdata"
		conf.Log.Infof("Not find password conf, will use default value 'taosdata'.")
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
	m.conf = cfg
	return nil
}

func (m *taosSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening tdengine sink")
	url := fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s`, m.conf.User, m.conf.Password, m.conf.Ip, m.conf.Port, m.conf.Database)
	m.db, err = sql.Open("taosSql", url)
	return err
}

func (m *taosSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	data, ok := item.([]byte)
	if !ok {
		logger.Debug("tdengine sink receive non string data")
		return nil
	}
	logger.Debugf("tdengine sink receive %s", item)

	var sliData []map[string]interface{}
	err := json.Unmarshal(data, &sliData)
	if nil != err {
		return err
	}
	for _, mapData := range sliData {
		sql, err := m.conf.buildSql(ctx, mapData)
		if nil != err {
			return err
		}
		logger.Debugf(sql)
		rows, err := m.db.Query(sql)
		if err != nil {
			return err
		}
		rows.Close()
	}
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
