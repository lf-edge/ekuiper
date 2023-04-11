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
	"time"

	driver2 "github.com/lf-edge/ekuiper/extensions/sqldatabase/driver"
	"github.com/lf-edge/ekuiper/extensions/sqldatabase/sqlgen"
	"github.com/lf-edge/ekuiper/extensions/util"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/xo/dburl"
)

type sqlConConfig struct {
	Interval int    `json:"interval"`
	Url      string `json:"url"`
}

type sqlsource struct {
	conf  *sqlConConfig
	Query sqlgen.SqlQueryGenerator
	//The db connection instance
	db *sql.DB
}

func (m *sqlsource) Configure(_ string, props map[string]interface{}) error {
	cfg := &sqlConConfig{}

	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Url == "" {
		return fmt.Errorf("property Url is required")
	}
	if cfg.Interval == 0 {
		return fmt.Errorf("property interval is required")
	}

	Db, err := dburl.Parse(cfg.Url)
	if err != nil {
		return fmt.Errorf("dburl.Parse %s fail with error: %v", cfg.Url, err)
	}

	generator, err := sqlgen.GetQueryGenerator(Db, props)
	if err != nil {
		return fmt.Errorf("GetQueryGenerator %s fail with error: %v", cfg.Url, err)
	}

	m.Query = generator
	m.conf = cfg

	db, err := util.Open(m.conf.Url)
	if err != nil {
		return fmt.Errorf("connection to %s Open with error %v, support build tags are %v", m.conf.Url, err, driver2.KnownBuildTags())
	}
	m.db = db

	return nil
}

func (m *sqlsource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	t := time.NewTicker(time.Duration(m.conf.Interval) * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			query, err := m.Query.SqlQueryStatement()
			if err != nil {
				logger.Errorf("Get sql query error %v", err)
			}
			logger.Debugf("Query the database with %s", query)
			rows, err := m.db.Query(query)
			if err != nil {
				logger.Errorf("Run sql query(%s) error %v", query, err)
				errCh <- err
				return
			}

			cols, _ := rows.Columns()

			types, err := rows.ColumnTypes()
			if err != nil {
				logger.Errorf("row ColumnTypes error %v", query, err)
				errCh <- err
				return
			}
			for rows.Next() {
				data := make(map[string]interface{})
				columns := make([]interface{}, len(cols))
				prepareValues(columns, types, cols)

				err := rows.Scan(columns...)
				if err != nil {
					logger.Errorf("Run sql scan(%s) error %v", query, err)
					errCh <- err
					return
				}

				scanIntoMap(data, columns, cols)
				m.Query.UpdateMaxIndexValue(data)
				consumer <- api.NewDefaultSourceTuple(data, nil)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *sqlsource) GetOffset() (interface{}, error) {
	return m.Query.GetIndexValue(), nil
}

func (m *sqlsource) Rewind(offset interface{}) error {
	m.Query.SetIndexValue(offset)
	return nil
}

func (m *sqlsource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Debugf("Closing sql stream to %v", m.conf)
	if m.db != nil {
		_ = m.db.Close()
	}

	return nil
}

func Sql() api.Source {
	return &sqlsource{}
}
