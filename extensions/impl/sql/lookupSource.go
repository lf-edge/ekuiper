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

package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

type SqlLookupSource struct {
	conf          *SQLConf
	conn          *client2.SQLConnection
	props         map[string]any
	driver        string
	table         string
	needReconnect bool
	gen           sqlQueryGen
	conId         string
}

func (s *SqlLookupSource) Ping(ctx api.StreamContext, m map[string]any) error {
	cli := &client2.SQLConnection{}
	err := cli.Provision(ctx, "test", m)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

func (s *SqlLookupSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	cfg := &SQLConf{}
	err := cast.MapToStruct(configs, cfg)
	failpoint.Inject("MapToStructErr", func() {
		err = errors.New("MapToStruct")
	})
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", configs, err)
	}
	props, err := cfg.resolveDBURL(configs)
	if err != nil {
		return err
	}
	s.conf = cfg
	s.driver, err = client2.ParseDriver(s.conf.DBUrl)
	if err != nil {
		return err
	}
	s.table = cfg.Datasource
	s.props = props
	s.gen = s.buildGen()
	return nil
}

func (s *SqlLookupSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sql source connector url:%v", s.conf.DBUrl)
	if s.conn != nil {
		s.conn.DetachSub(ctx, s.props)
	}
	return connection.DetachConnection(ctx, s.conId)
}

func (s *SqlLookupSource) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var cli *client2.SQLConnection
	var err error
	id := s.conf.DBUrl
	cw, err := connection.FetchConnection(ctx, id, "sql", s.props, sc)
	if err != nil {
		return err
	}
	s.conId = cw.ID
	conn, err := cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("sql client not ready: %v", err)
	}
	cli = conn.(*client2.SQLConnection)
	s.conn = cli
	return err
}

func (s *SqlLookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error) {
	if s.needReconnect {
		err := s.conn.Reconnect()
		if err != nil {
			conf.Log.Errorf("reconnect db error %v", err)
			return nil, err
		}
	}
	var query string
	if s.conf.TemplateSqlQueryCfg == nil {
		query = s.gen.buildQuery(fields, keys, values)
	} else {
		mapValue := make(map[string]any)
		for index, key := range keys {
			mapValue[key] = values[index]
		}
		sqlQuery, err := ctx.ParseTemplate(s.conf.TemplateSqlQueryCfg.TemplateSql, mapValue)
		if err != nil {
			return nil, err
		}
		query = sqlQuery
	}
	ctx.GetLogger().Debugf("Query is %s", query)
	rows, err := s.conn.GetDB().Query(query)
	failpoint.Inject("dbErr", func() {
		err = errors.New("dbErr")
	})
	if err != nil {
		s.needReconnect = true
		ctx.GetLogger().Errorf("sql look table failed, err:%v, query: %v", err, query)
		return nil, err
	} else {
		s.needReconnect = false
	}
	cols, _ := rows.Columns()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	dataList := make([]map[string]any, 0)
	for rows.Next() {
		data := make(map[string]any)
		columns := make([]interface{}, len(cols))
		prepareValues(ctx, columns, types, cols)

		err := rows.Scan(columns...)
		if err != nil {
			return nil, err
		}
		scanIntoMap(data, columns, cols, nil)
		dataList = append(dataList, data)
	}
	return dataList, nil
}

type sqlQueryGen interface {
	buildQuery(fields []string, keys []string, values []interface{}) string
}

type defaultSQLGen struct{ table string }

func (g defaultSQLGen) buildQuery(fields []string, keys []string, values []interface{}) string {
	query := "SELECT "
	if len(fields) == 0 {
		query += "*"
	} else {
		for i, f := range fields {
			if i > 0 {
				query += ","
			}
			query += f
		}
	}
	query += fmt.Sprintf(" FROM %s WHERE ", g.table)
	for i, k := range keys {
		if i > 0 {
			query += " AND "
		}
		switch v := values[i].(type) {
		case string:
			query += fmt.Sprintf("`%s` = '%s'", k, strings.ReplaceAll(v, "'", "''"))
		default:
			query += fmt.Sprintf("`%s` = %v", k, v)
		}
	}
	return query
}

type noQuoteSQLGen struct{ table string }

func (g noQuoteSQLGen) buildQuery(fields []string, keys []string, values []interface{}) string {
	query := "SELECT "
	if len(fields) == 0 {
		query += "*"
	} else {
		for i, f := range fields {
			if i > 0 {
				query += ","
			}
			query += f
		}
	}
	query += fmt.Sprintf(" FROM %s WHERE ", g.table)
	for i, k := range keys {
		if i > 0 {
			query += " AND "
		}
		switch v := values[i].(type) {
		case string:
			query += fmt.Sprintf("%s = '%s'", k, strings.ReplaceAll(v, "'", "''"))
		default:
			query += fmt.Sprintf("%s = %v", k, v)
		}
	}
	return query
}

func (s *SqlLookupSource) buildGen() sqlQueryGen {
	switch strings.ToLower(s.driver) {
	case "sqlserver", "mssql", "postgres":
		return noQuoteSQLGen{table: s.table}
	default:
		return defaultSQLGen{table: s.table}
	}
}

func GetLookupSource() api.Source {
	return &SqlLookupSource{}
}

var (
	_ api.LookupSource  = &SqlLookupSource{}
	_ util.PingableConn = &SqlLookupSource{}
)
