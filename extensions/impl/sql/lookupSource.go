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

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
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
}

func (s *SqlLookupSource) Ping(_ string, m map[string]interface{}) error {
	ctx := context.Background()
	if err := s.Provision(ctx, m); err != nil {
		return err
	}
	if err := s.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		s.Close(ctx)
	}()
	return s.conn.Ping(ctx)
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
	if len(cfg.DBUrl) < 1 {
		return fmt.Errorf("dburl should be defined")
	}
	s.conf = cfg
	s.driver, err = client2.ParseDriver(s.conf.DBUrl)
	if err != nil {
		return err
	}
	s.table = cfg.Datasource
	s.props = configs
	s.gen = s.buildGen()
	return nil
}

func (s *SqlLookupSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sql source connector url:%v", s.conf.DBUrl)
	id := s.conf.DBUrl
	connection.DetachConnection(ctx, id, s.props)
	if s.conn != nil {
		s.conn.DetachSub(ctx, s.props)
	}
	return nil
}

func (s *SqlLookupSource) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var cli *client2.SQLConnection
	var err error
	id := s.conf.DBUrl
	cw, err := connection.FetchConnection(ctx, id, "sql", s.props)
	if err != nil {
		return err
	}
	conn, err := cw.Wait()
	if err != nil {
		return err
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
	query := s.gen.buildQuery(fields, keys, values)
	ctx.GetLogger().Debugf("Query is %s", query)
	rows, err := s.conn.GetDB().Query(query)
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
		prepareValues(columns, types, cols)

		err := rows.Scan(columns...)
		if err != nil {
			return nil, err
		}
		scanIntoMap(data, columns, cols)
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
			query += fmt.Sprintf("`%s` = '%s'", k, v)
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
			query += fmt.Sprintf("%s = '%s'", k, v)
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

var _ api.LookupSource = &SqlLookupSource{}
