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
	"reflect"
	"testing"
	"time"
)

func Test_templateSqlQueryCfg_getSqlQueryStatement(t1 *testing.T) {
	type fields struct {
		TemplateSql string
		FieldName   string
		FieldValue  interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   error
		want1  string
	}{
		{
			name: "select * from table",
			fields: fields{
				TemplateSql: "select * from table",
				FieldName:   "",
				FieldValue:  nil,
			},
			want:  nil,
			want1: "select * from table",
		},

		{
			name: "select * from table where id > {{.id}} ",
			fields: fields{
				TemplateSql: "select * from table where id > {{.id}}",
				FieldName:   "id",
				FieldValue:  100,
			},
			want:  nil,
			want1: "select * from table where id > 100",
		},

		{
			name: "select * from table where responseTime > `{{.responseTime}}` ",
			fields: fields{
				TemplateSql: "select * from table where responseTime > `{{.responseTime}}`",
				FieldName:   "responseTime",
				FieldValue:  "2008-10-29 14:56:59",
			},
			want:  nil,
			want1: "select * from table where responseTime > `2008-10-29 14:56:59`",
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			cfg := &TemplateSqlQueryCfg{
				TemplateSql: tt.fields.TemplateSql,
				IndexField:  tt.fields.FieldName,
				IndexValue:  tt.fields.FieldValue,
			}
			query, _ := NewTemplateSqlQuery(cfg)

			got1, got := query.SqlQueryStatement()
			if !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("SqlQueryStatement() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t1.Errorf("SqlQueryStatement() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func getDatetimeFromstring(dateStr string) time.Time {
	myDate, _ := time.Parse("2006-01-02 15:04:05", dateStr)
	return myDate
}

func TestTemplateQuery(t *testing.T) {
	cfg := &TemplateSqlQueryCfg{
		TemplateSql:    "select * from table where responseTime > `{{.responseTime}}`",
		IndexField:     "responseTime",
		IndexValue:     getDatetimeFromstring("2008-10-25 14:56:59"),
		IndexFieldType: DATETIME_TYPE,
		DateTimeFormat: "YYYY-MM-dd HH:mm:ssSSS",
	}

	s, _ := NewTemplateSqlQuery(cfg)

	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-10-29 14:56:59"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-11 11:12:01"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-09 15:45:21"),
	})
	s.UpdateMaxIndexValue(map[string]interface{}{
		"responseTime": getDatetimeFromstring("2008-11-11 13:23:44"),
	})

	nextSqlStr, _ := s.SqlQueryStatement()

	want := "select * from table where responseTime > `2008-11-11 13:23:44.000`"

	if !reflect.DeepEqual(nextSqlStr, want) {
		t.Errorf("SqlQueryStatement() = %v, want %v", nextSqlStr, want)
	}
}
