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

package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/extensions/util"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type sqlLookupConfig struct {
	Url string `json:"url"`
}

type sqlLookupSource struct {
	url    string
	table  string
	db     *sql.DB
	driver string
}

func (s *sqlLookupSource) Ping(_ string, props map[string]interface{}) error {
	if err := s.Configure("", props); err != nil {
		return err
	}
	return s.db.Ping()
}

// Open establish a connection to the database
func (s *sqlLookupSource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening sql lookup source")
	return nil
}

func (s *sqlLookupSource) Configure(datasource string, props map[string]interface{}) error {
	cfg := &sqlLookupConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Url == "" {
		return fmt.Errorf("property Url is required")
	}
	s.url = cfg.Url
	s.table = datasource
	db, err := util.FetchDBToOneNode(util.GlobalPool, s.url)
	if err != nil {
		return fmt.Errorf("connection to %s Open with error %v", s.url, err)
	}
	s.driver, err = util.ParseDriver(s.url)
	if err != nil {
		conf.Log.Warnf("parse url %v driver failed, err:%v", s.url, err)
		s.driver = ""
	}
	s.db = db
	return nil
}

func (s *sqlLookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []interface{}) ([]api.SourceTuple, error) {
	ctx.GetLogger().Debug("Start to lookup tuple")
	rcvTime := conf.GetNow()
	query := s.buildQuery(fields, keys, values)
	ctx.GetLogger().Debugf("Query is %s", query)
	rows, err := s.db.Query(query)
	if err != nil {
		ctx.GetLogger().Errorf("sql look table failed, err:%v, query: %v", err, query)
		return nil, err
	}
	cols, _ := rows.Columns()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var result []api.SourceTuple
	for rows.Next() {
		data := make(map[string]interface{})
		columns := make([]interface{}, len(cols))
		prepareValues(columns, types, cols)

		err := rows.Scan(columns...)
		if err != nil {
			return nil, err
		}
		scanIntoMap(data, columns, cols)
		result = append(result, api.NewDefaultSourceTupleWithTime(data, nil, rcvTime))
	}
	return result, nil
}

func (s *sqlLookupSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Closing sql lookup source")
	defer func() { s.db = nil }()
	if s.db != nil {
		return util.ReturnDBFromOneNode(util.GlobalPool, s.url)
	}
	return nil
}

func GetLookup() api.LookupSource {
	return &sqlLookupSource{}
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

func (s *sqlLookupSource) buildQuery(fields []string, keys []string, values []interface{}) string {
	switch strings.ToLower(s.driver) {
	case "sqlserver", "mssql", "postgres":
		return noQuoteSQLGen{table: s.table}.buildQuery(fields, keys, values)
	default:
		return defaultSQLGen{table: s.table}.buildQuery(fields, keys, values)
	}
}
