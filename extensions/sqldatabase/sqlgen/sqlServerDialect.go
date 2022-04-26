// Copyright 2022 EMQ Technologies Co., Ltd.
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

package sqlgen

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type SqlServerQueryGenerator struct {
	*InternalSqlQueryCfg
}

func (q *SqlServerQueryGenerator) quoteIdentifier(identifier string) string {
	return "'" + identifier + "'"
}

func (q *SqlServerQueryGenerator) getSelect() string {
	if q.Limit != 0 {
		return fmt.Sprintf("select top %d * from %s ", q.Limit, q.Table)
	} else {
		return "select * from " + q.Table + " "
	}
}

func (q *SqlServerQueryGenerator) getCondition() (string, error) {
	var val string
	if q.IndexField != "" {
		if q.IndexFieldType == DATETIME_TYPE && q.DateTimeFormat != "" {
			t, err := cast.InterfaceToTime(q.IndexValue, q.DateTimeFormat)
			if err != nil {
				err = fmt.Errorf("SqlQueryStatement InterfaceToTime datetime convert got error %v", err)
				return "", err
			}
			val, err = cast.FormatTime(t, q.DateTimeFormat)
			if err != nil {
				err = fmt.Errorf("SqlQueryStatement FormatTime datetime convert got error %v", err)
				return "", err
			}
		} else {
			val = fmt.Sprintf("%v", q.IndexValue)
		}
		return "where " + q.IndexField + " > " + q.quoteIdentifier(val) + " ", nil
	}

	return "", nil
}

func (q *SqlServerQueryGenerator) getOrderby() string {
	if q.IndexField != "" {
		return "order by " + q.IndexField + " ASC"
	}
	return ""
}

func NewSqlServerQuery(cfg *InternalSqlQueryCfg) SqlQueryGenerator {
	in := &SqlServerQueryGenerator{
		InternalSqlQueryCfg: cfg,
	}
	return in
}

func (q *SqlServerQueryGenerator) SqlQueryStatement() (string, error) {
	con, err := q.getCondition()
	if err != nil {
		return "", err
	}
	return q.getSelect() + con + q.getOrderby(), nil
}

func (q *SqlServerQueryGenerator) UpdateMaxIndexValue(row map[string]interface{}) {
	// since internal sql have asc clause, so the last element is largest
	if q.IndexField != "" {
		v, found := row[q.IndexField]
		if !found {
			return
		}
		q.IndexValue = v
	}
}
