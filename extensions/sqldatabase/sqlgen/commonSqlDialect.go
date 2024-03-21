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
	"bytes"
	"fmt"

	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/store"
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
	return getCondition(q.InternalSqlQueryCfg, q.quoteIdentifier)
}

func (q *CommonQueryGenerator) getOrderby() string {
	return getOrderBy(q.InternalSqlQueryCfg, q.quoteIdentifier)
}

func (q *CommonQueryGenerator) getLimit() string {
	if q.Limit != 0 {
		return fmt.Sprintf(" limit %d", q.Limit)
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
	updateMaxIndexValue(q.InternalSqlQueryCfg, row)
}

type OracleQueryGenerate struct {
	*CommonQueryGenerator
}

func NewOracleQueryGenerate(cfg *InternalSqlQueryCfg) SqlQueryGenerator {
	return &OracleQueryGenerate{
		CommonQueryGenerator: &CommonQueryGenerator{
			InternalSqlQueryCfg: cfg,
		},
	}
}

func (q *OracleQueryGenerate) SqlQueryStatement() (string, error) {
	con, err := q.getCondition()
	if err != nil {
		return "", err
	}
	query := q.getSelect() + con + q.getOrderby()
	if q.Limit != 0 {
		return fmt.Sprintf("select * from (%s) where rownum <= %v", query, q.Limit), nil
	}
	return query, nil
}

func (q *OracleQueryGenerate) UpdateMaxIndexValue(row map[string]interface{}) {
	q.CommonQueryGenerator.UpdateMaxIndexValue(row)
}

func getCondition(cfg *InternalSqlQueryCfg, quoteIdentifier func(string) string) (string, error) {
	fieldlist := cfg.store.GetFieldList()
	if len(fieldlist) > 0 {
		b := bytes.NewBufferString("where")
		index := 0
		for _, w := range fieldlist {
			condition, err := buildSingleIndexCondition(w, quoteIdentifier)
			if err != nil {
				return "", err
			}
			b.WriteString(" ")
			b.WriteString(condition)
			b.WriteString(" ")
			if index < len(fieldlist)-1 {
				b.WriteString("AND")
			}
			index++
		}
		return b.String(), nil
	}
	return "", nil
}

func buildSingleIndexCondition(w *store.IndexField, quoteIdentifier func(string) string) (string, error) {
	var val string
	if w.IndexFieldDataType == DATETIME_TYPE && w.IndexFieldDateTimeFormat != "" {
		t, err := cast.InterfaceToTime(w.IndexFieldValue, w.IndexFieldDateTimeFormat)
		if err != nil {
			err = fmt.Errorf("SqlQueryStatement InterfaceToTime datetime convert got error %v", err)
			return "", err
		}
		val, err = cast.FormatTime(t, w.IndexFieldDateTimeFormat)
		if err != nil {
			err = fmt.Errorf("SqlQueryStatement FormatTime datetime convert got error %v", err)
			return "", err
		}
	} else {
		val = fmt.Sprintf("%v", w.IndexFieldValue)
	}
	return w.IndexFieldName + " > " + quoteIdentifier(val), nil
}

func getOrderBy(cfg *InternalSqlQueryCfg, quoteIdentifier func(string) string) string {
	fieldList := cfg.store.GetFieldList()
	if len(fieldList) > 0 {
		b := bytes.NewBufferString("order by")
		for i, w := range fieldList {
			b.WriteString(" ")
			orderBy := buildSingleOrderBy(w, quoteIdentifier)
			b.WriteString(orderBy)
			if i < len(fieldList)-1 {
				b.WriteString(",")
			}
		}
		return b.String()
	}
	return ""
}

func buildSingleOrderBy(w *store.IndexField, quoteIdentifier func(string) string) string {
	return quoteIdentifier(w.IndexFieldName) + " ASC"
}

func updateMaxIndexValue(cfg *InternalSqlQueryCfg, row map[string]interface{}) {
	fieldMap := cfg.store.GetFieldMap()
	for _, w := range fieldMap {
		v, found := row[w.IndexFieldName]
		if !found {
			return
		}
		cfg.store.UpdateFieldValue(w.IndexFieldName, v)
	}
}
