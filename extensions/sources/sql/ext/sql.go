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
	"time"

	driver2 "github.com/lf-edge/ekuiper/v2/extensions/sqldatabase/driver"
	"github.com/lf-edge/ekuiper/v2/extensions/sqldatabase/sqlgen"
	"github.com/lf-edge/ekuiper/v2/extensions/util"
	"github.com/lf-edge/ekuiper/v2/pkg/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/hidden"
)

type sqlConConfig struct {
	Interval int    `json:"interval"`
	Url      string `json:"url"`

	displayURL string
}

type sqlsource struct {
	conf  *sqlConConfig
	Query sqlgen.SqlQueryGenerator
	// The db connection instance
	db *sql.DB
}

func (m *sqlsource) Ping(_ string, props map[string]interface{}) error {
	if err := m.Configure("", props); err != nil {
		return err
	}
	return m.db.Ping()
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

	driver, err := util.ParseDriver(cfg.Url)
	if err != nil {
		return fmt.Errorf("dburl.Parse %s fail with error: %v", cfg.displayURL, err)
	}

	generator, err := sqlgen.GetQueryGenerator(driver, props)
	if err != nil {
		return fmt.Errorf("GetQueryGenerator %s fail with error: %v", cfg.displayURL, err)
	}

	m.Query = generator
	m.conf = cfg
	db, err := util.FetchDBToOneNode(util.GlobalPool, m.conf.Url)
	if err != nil {
		return fmt.Errorf("connection to %s Open with error %v, support build tags are %v", m.conf.displayURL, err, driver2.KnownBuildTags())
	}
	m.db = db

	cfg.displayURL = cfg.Url
	if hiddenURL, hidden := hidden.HiddenURLPasswd(cfg.Url); hidden {
		cfg.displayURL = hiddenURL
	}
	return nil
}

func (m *sqlsource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	t := time.NewTicker(time.Duration(m.conf.Interval) * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case rcvTime := <-t.C:
			query, err := m.Query.SqlQueryStatement()
			if err != nil {
				logger.Errorf("Get sql query error %v", err)
			}
			logger.Debugf("Query the database with %s", query)
			rows, err := m.db.Query(query)
			if err != nil {
				logger.Errorf("sql source meet error, try to reconnection, err:%v, query:%v", err, query)
				if !isConnectionError(err) {
					errCh <- err
					continue
				}
				err2 := m.Reconnect()
				if err2 != nil {
					errCh <- fmt.Errorf("reconnect failed, reconnect err:%v", err2)
				} else {
					logger.Info("sql source reconnect successfully")
				}
				continue
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
				consumer <- api.NewDefaultSourceTupleWithTime(data, nil, rcvTime)
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

func (m *sqlsource) ResetOffset(input map[string]interface{}) error {
	wrap := m.Query.GetIndexValueWrap()
	if wrap != nil {
		wrap.UpdateByInput(input)
	}
	return nil
}

func (m *sqlsource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Debugf("Closing sql stream to %v", m.conf)
	if m.db != nil {
		return util.ReturnDBFromOneNode(util.GlobalPool, m.conf.Url)
	}
	return nil
}

func (m *sqlsource) Reconnect() error {
	// wait half interval to reconnect
	time.Sleep(time.Duration(m.conf.Interval) * time.Millisecond / 2)
	db, err2 := util.ReplaceDbForOneNode(util.GlobalPool, m.conf.Url)
	if err2 != nil {
		return err2
	}
	m.db = db
	return nil
}

func GetSource() api.Source {
	return &sqlsource{}
}

func isConnectionError(err error) bool {
	return strings.Contains(err.Error(), "connection refused")
}
