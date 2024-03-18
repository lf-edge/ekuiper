// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	return getCondition(q.InternalSqlQueryCfg, q.quoteIdentifier)
}

func (q *SqlServerQueryGenerator) getOrderby() string {
	return getOrderBy(q.InternalSqlQueryCfg, func(s string) string {
		return s
	})
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
	updateMaxIndexValue(q.InternalSqlQueryCfg, row)
}
