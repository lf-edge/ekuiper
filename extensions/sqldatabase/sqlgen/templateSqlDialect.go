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
	"bytes"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/posener/order"
	"text/template"
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
	if t.IndexFieldType == DATETIME_TYPE && t.DateTimeFormat != "" {
		time, err := cast.InterfaceToTime(t.IndexValue, t.DateTimeFormat)
		if err != nil {
			err = fmt.Errorf("SqlQueryStatement InterfaceToTime datetime convert got error %v", err)
			return "", err
		}
		val, err = cast.FormatTime(time, t.DateTimeFormat)
		if err != nil {
			err = fmt.Errorf("SqlQueryStatement FormatTime datetime convert got error %v", err)
			return "", err
		}
	} else {
		val = fmt.Sprintf("%v", t.IndexValue)
	}

	input := map[string]interface{}{
		t.IndexField: val,
	}

	var output bytes.Buffer
	err := t.tp.Execute(&output, input)
	if err != nil {
		return "", err
	}
	return string(output.Bytes()), nil
}

func (t *templateSqlQuery) UpdateMaxIndexValue(row map[string]interface{}) {
	if t.IndexField != "" {
		v, found := row[t.IndexField]
		if !found {
			return
		}
		if val := order.Is(v); val.Greater(t.IndexValue) {
			t.IndexValue = v
		}
	}
}
