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
	"fmt"

	"github.com/lf-edge/ekuiper/extensions/util"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type sqlLookupConfig struct {
	Url string `json:"url"`
}

type sqlLookupSource struct {
	url   string
	table string
	db    *sql.DB
}

// Open establish a connection to the database
func (s *sqlLookupSource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening sql lookup source")
	db, err := util.FetchDBToOneNode(util.GlobalPool, s.url)
	if err != nil {
		return fmt.Errorf("connection to %s Open with error %v", s.url, err)
	}
	s.db = db
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
	return nil
}

func (s *sqlLookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []interface{}) ([]api.SourceTuple, error) {
	ctx.GetLogger().Debug("Start to lookup tuple")
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
	query += fmt.Sprintf(" FROM %s WHERE ", s.table)
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
	ctx.GetLogger().Debugf("Query is %s", query)
	rows, err := s.db.Query(query)
	if err != nil {
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
		result = append(result, api.NewDefaultSourceTuple(data, nil))
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

func SqlLookup() api.LookupSource {
	return &sqlLookupSource{}
}
