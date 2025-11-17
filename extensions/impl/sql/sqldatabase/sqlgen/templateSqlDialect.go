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
	"text/template"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/store"
)

type templateSqlQuery struct {
	tp *template.Template
	*TemplateSqlQueryCfg
}

func NewTemplateSqlQuery(cfg *TemplateSqlQueryCfg) (SqlQueryGenerator, error) {
	t := &templateSqlQuery{
		tp:                  nil,
		TemplateSqlQueryCfg: cfg,
	}

	if err := t.init(); err != nil {
		return nil, err
	} else {
		return t, nil
	}
}

func (t *templateSqlQuery) init() error {
	tp, err := template.New("sql").Parse(t.TemplateSql)
	if err != nil {
		return err
	}
	t.tp = tp
	return nil
}

func (t *templateSqlQuery) SqlQueryStatement() (string, error) {
	var val string
	input := make(map[string]interface{})
	fieldMap := t.store.GetFieldMap()
	for _, w := range fieldMap {
		if w.IndexFieldDataType == DATETIME_TYPE && w.IndexFieldDateTimeFormat != "" {
			time, err := cast.InterfaceToTime(w.IndexFieldValue, w.IndexFieldDateTimeFormat)
			if err != nil {
				err = fmt.Errorf("SqlQueryStatement InterfaceToTime datetime convert got error %v", err)
				return "", err
			}
			val, err = cast.FormatTime(time, w.IndexFieldDateTimeFormat)
			if err != nil {
				err = fmt.Errorf("SqlQueryStatement FormatTime datetime convert got error %v", err)
				return "", err
			}
		} else {
			val = fmt.Sprintf("%v", w.IndexFieldValue)
		}
		input[w.IndexFieldName] = val
	}

	var output bytes.Buffer
	err := t.tp.Execute(&output, input)
	if err != nil {
		return "", err
	}
	return output.String(), nil
}

func (t *templateSqlQuery) UpdateMaxIndexValue(row map[string]interface{}) {
	fieldMap := t.store.GetFieldMap()
	for _, w := range fieldMap {
		v, found := row[w.IndexFieldName]
		if !found {
			return
		}
		t.store.UpdateFieldValue(w.IndexFieldName, v)
	}
}

type TemplateSqlQueryCfg struct {
	TemplateSql string              `json:"templateSql"`
	IndexFields []*store.IndexField `json:"indexFields"`
	store       *store.IndexFieldStoreWrap
}

func (t *TemplateSqlQueryCfg) InitIndexFieldStore() {
	t.store = &store.IndexFieldStoreWrap{}
	t.store.Init(t.IndexFields...)
}

func (t *TemplateSqlQueryCfg) SetIndexValue(v interface{}) {
	switch vv := v.(type) {
	case *store.IndexFieldStore:
		t.store.InitByStore(vv)
		t.store.LoadFromList()
	default:
		t.InitIndexFieldStore()
	}
}

func (t *TemplateSqlQueryCfg) GetIndexValue() interface{} {
	return t.store.GetStore()
}

func (t *TemplateSqlQueryCfg) GetIndexValueWrap() *store.IndexFieldStoreWrap {
	return t.store
}
