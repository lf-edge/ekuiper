// +build plugins
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	//"github.com/emqx/kuiper/common"
	common "github.com/emqx/kuiper/common"
	//"github.com/emqx/kuiper/xstream/api"
	api "github.com/emqx/kuiper/xstream/api"
	//	_ "github.com/taosdata/driver-go/taosSql"
	"reflect"
	"strings"
)

type (
	taosConfig struct {
		Port     int      `json:"port"`
		Ip       string   `json:"ip"`
		User     string   `json:"user"`
		Password string   `json:"password"`
		Database string   `json:"database"`
		Table    string   `json:"table"`
		Fields   []string `json:"fields"`
	}
	taosSink struct {
		conf *taosConfig
		db   *sql.DB
	}
)

func (this *taosConfig) buildSql(ctx api.StreamContext, mapData map[string]interface{}) string {
	if 0 == len(mapData) {
		return ""
	}
	logger := ctx.GetLogger()
	var keys, vals []string
	for _, k := range this.Fields {
		if v, ok := mapData[k]; ok {
			keys = append(keys, k)
			if reflect.String == reflect.TypeOf(v).Kind() {
				vals = append(vals, fmt.Sprintf(`"%v"`, v))
			} else {
				vals = append(vals, fmt.Sprintf(`%v`, v))
			}
		} else {
			logger.Debug("not found field:", k)
		}
	}
	if 0 != len(keys) {
		if len(this.Fields) < len(mapData) {
			logger.Warnln("some of values will be ignored.")
		}
		return fmt.Sprintf(`INSERT INTO %s (%s)VALUES(%s);`, this.Table, strings.Join(keys, `,`), strings.Join(vals, `,`))
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
		return fmt.Sprintf(`INSERT INTO %s (%s)VALUES(%s);`, this.Table, strings.Join(keys, `,`), strings.Join(vals, `,`))
	}
	return ""
}

func (m *taosSink) Configure(props map[string]interface{}) error {
	cfg := &taosConfig{}
	err := common.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Ip == "" {
		return fmt.Errorf("property ip is required")
	}
	if cfg.User == "" {
		return fmt.Errorf("property user is required")
	}
	if cfg.Password == "" {
		return fmt.Errorf("property password is required")
	}
	if cfg.Database == "" {
		return fmt.Errorf("property database is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("property table is required")
	}
	m.conf = cfg
	return nil
}

func (m *taosSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening taos sink")
	url := fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s`, m.conf.User, m.conf.Password, m.conf.Ip, m.conf.Port, m.conf.Database)
	m.db, err = sql.Open("taosSql", url)
	return err
}

func (m *taosSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	data, ok := item.([]byte)
	if !ok {
		logger.Debug("taos sink receive non string data")
		return nil
	}
	logger.Debugf("taos sink receive %s", item)

	var sliData []map[string]interface{}
	err := json.Unmarshal(data, &sliData)
	if nil != err {
		return err
	}
	for _, mapData := range sliData {
		sql := m.conf.buildSql(ctx, mapData)
		if 0 == len(sql) {
			continue
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

func Taos() api.Sink {
	return &taosSink{}
}
