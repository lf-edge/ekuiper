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

type CommonQueryGenerator struct {
	*InternalSqlQueryCfg
}

func (q *CommonQueryGenerator) quoteIdentifier(identifier string) string {
	return "'" + identifier + "'"
}

func (q *CommonQueryGenerator) getSelect() string {
	return "select * from " + q.Table + " "
}

func (q *CommonQueryGenerator) getCondition() (string, error) {
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

func (q *CommonQueryGenerator) getOrderby() string {
	if q.IndexField != "" {
		return "order by " + q.quoteIdentifier(q.IndexField) + " ASC "
	}
	return ""
}

func (q *CommonQueryGenerator) getLimit() string {
	if q.Limit != 0 {
		return fmt.Sprintf("limit %d", q.Limit)
	}
	return ""
}

func NewCommonSqlQuery(cfg *InternalSqlQueryCfg) SqlQueryGenerator {
	in := &CommonQueryGenerator{
		InternalSqlQueryCfg: cfg,
	}
	return in
}

func (q *CommonQueryGenerator) SqlQueryStatement() (string, error) {
	con, err := q.getCondition()
	if err != nil {
		return "", err
	}
	return q.getSelect() + con + q.getOrderby() + q.getLimit(), nil
}

func (q *CommonQueryGenerator) UpdateMaxIndexValue(row map[string]interface{}) {
	if q.IndexField != "" {
		v, found := row[q.IndexField]
		if !found {
			return
		}
		q.IndexValue = v
	}
}
